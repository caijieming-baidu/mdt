// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <sstream>
#include "sdk/sdk.h"
#include "sdk/db_impl.h"
#include <tera.h>

#include <gflags/gflags.h>

DECLARE_string(log_file);
DECLARE_string(flagfile);

DECLARE_string(tera_root_dir);
DECLARE_string(tera_flag_file_path);
DECLARE_string(database_root_dir);
DECLARE_int64(max_timestamp_table_num);
DECLARE_int64(tera_table_ttl);
DECLARE_bool(multi_table_enable);
DECLARE_int32(multi_table_nr);

namespace mdt {

void SetupLog(const std::string& name) {
    std::string program_name = "mdt";
    if (!name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.size() == 0) {
        if (access("../log", F_OK)) {
            FLAGS_log_dir = "../log";
        } else {
            FLAGS_log_dir = "./log";
        }
    }

    if (access(FLAGS_log_dir.c_str(), F_OK)) {
        mkdir(FLAGS_log_dir.c_str(), 0777);
    }

    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

static pthread_once_t glog_once = PTHREAD_ONCE_INIT;
static void InternalSetupGoogleLog() {
    // init param, setup log
    std::string log_prefix = "mdt";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    tera::Client::SetGlogIsInitialized();
    LOG(INFO) << "start loging...";
}

void SetupGoogleLog() {
    pthread_once(&glog_once, InternalSetupGoogleLog);
}

Status DatabaseImpl::OpenDB(const std::string& db_name, Database** db_ptr) {
    // init log
    SetupGoogleLog();

    Options options;
    options.env_ = Env::Default();
    options.tera_flag_file_path_ = FLAGS_tera_flag_file_path;
    return CreateDB(options, db_name, db_ptr);
}

Status DatabaseImpl::CreateDB(const Options& options,
                              const std::string& db_name,
                              Database** db_ptr) {
    *db_ptr = NULL;
    DatabaseImpl* db_impl = new DatabaseImpl(options, db_name);
    Status s = db_impl->Init();
    if (!s.ok()) {
        delete db_impl;
        return s;
    }
    *db_ptr = db_impl;
    return Status::OK();
}

// Database ops
Options InitDefaultOptions(const Options& options, const std::string& db_name) {
    Options opt = options;
    std::string database_root_dir = FLAGS_database_root_dir + "/" + db_name;
    Status s = opt.env_->CreateDir(database_root_dir);
    if (!s.ok()) {
        LOG(INFO) << "open db, init default, error " << s.ToString();
    }
    return opt;
}

DatabaseImpl::DatabaseImpl(const Options& options, const std::string& db_name)
    : db_name_(db_name),
    options_(InitDefaultOptions(options, db_name)) {

}

Status DatabaseImpl::Init() {
    // create fs's dir
    fs_opt_.env_ = options_.env_;
    fs_opt_.fs_path_ = FLAGS_database_root_dir + "/" + db_name_ + "/Filesystem/";
    fs_opt_.env_->CreateDir(fs_opt_.fs_path_);

    // create tera client
    ::tera::ErrorCode error_code;
    tera_opt_.root_path_ = FLAGS_tera_root_dir;
    tera_opt_.tera_flag_ = options_.tera_flag_file_path_;
    std::string local_flagfile = FLAGS_flagfile;
    tera_opt_.client_ = tera::Client::NewClient(tera_opt_.tera_flag_, "mdt", &error_code);
    if (tera_opt_.client_ == NULL) {
        LOG(INFO) << "open db, new cli error, tera flag " << tera_opt_.tera_flag_;
        FLAGS_flagfile = local_flagfile;
        return Status::IOError("tera client new error");
    }
    // multi client
    if (FLAGS_multi_table_enable) {
        for (int32_t i = 0; i < FLAGS_multi_table_nr; i++) {
            LOG(INFO) << "multi client " << FLAGS_multi_table_nr << ", idx " << i;
            std::ostringstream ss;
            ss << "mdt";
            ss << i;
            std::string client_name = ss.str();
            tera::Client* tera_client = tera::Client::NewClient(tera_opt_.tera_flag_, client_name, &error_code);
            if (tera_client == NULL) {
                LOG(INFO) << "open db, new cli error, tera flag " << tera_opt_.tera_flag_;
                FLAGS_flagfile = local_flagfile;
                return Status::IOError("tera client new error");
            }
            tera_opt_.extra_client_.push_back(tera_client);
        }
    }

    FLAGS_flagfile = local_flagfile;

    // create db schema table (kv mode)
    std::string schema_table_name = db_name_ + "#SchemaTable#";
    tera::TableDescriptor schema_desc(schema_table_name);
    tera::LocalityGroupDescriptor* schema_lg = schema_desc.AddLocalityGroup("lg");
    //schema_lg->SetBlockSize(32);
    schema_lg->SetCompress(tera::kSnappyCompress);
    schema_desc.SetRawKey(tera::kGeneralKv);
    // ignore exist error
    tera_opt_.client_->CreateTable(schema_desc, &error_code);

    tera_opt_.schema_table_ = tera_opt_.client_->OpenTable(schema_table_name, &error_code);
    LOG(INFO) << "open schema table, table name " << schema_table_name <<
        ", addr " << tera_opt_.schema_table_ << ", error code " << tera::strerr(error_code);
    if (tera_opt_.schema_table_ == NULL) {
        delete tera_opt_.client_;
        tera_opt_.client_ = NULL;
        LOG(INFO) << "open db, schema open error, schema table "
            << schema_table_name;
        return Status::IOError("schema table open error");
    }

    tera_adapter_.opt_ = tera_opt_;
    tera_adapter_.table_prefix_ = db_name_;
    return Status::OK();
}

DatabaseImpl::~DatabaseImpl() {
    ReleaseTables();
    if (tera_opt_.schema_table_) {
      delete tera_opt_.schema_table_;
      tera_opt_.schema_table_ = NULL;
    }
    if (tera_opt_.client_) {
      delete tera_opt_.client_;
      tera_opt_.client_ = NULL;
    }
}

Status DatabaseImpl::CreateTable(const TableDescription& table_desc) {
    // insert schema into schema table
    tera::ErrorCode error_code;
    BigQueryTableSchema schema;
    AssembleTableSchema(table_desc, &schema);
    std::string schema_value;
    schema.SerializeToString(&schema_value);
    tera_adapter_.opt_.schema_table_->Put(schema.table_name(), "", "", schema_value, &error_code);
    LOG(INFO) << "Put Schema: table name " << schema.table_name() << ", size " << schema_value.size()
        << ", error code " << tera::strerr(error_code);

    // create primary key table
    std::string primary_table_name = tera_adapter_.table_prefix_ + "#pri#" + schema.table_name();
    tera::TableDescriptor primary_table_desc(primary_table_name);
    LOG(INFO) << "Create primary table name " << primary_table_name;
    primary_table_desc.SetRawKey(tera::kBinary);
    primary_table_desc.SetSplitSize(10240);
    tera::LocalityGroupDescriptor* lg = primary_table_desc.AddLocalityGroup("lg");
    //lg->SetBlockSize(32);
    lg->SetCompress(tera::kSnappyCompress);
    tera::ColumnFamilyDescriptor* cf = primary_table_desc.AddColumnFamily("Location", "lg");
    //cf->SetTimeToLive(FLAGS_tera_table_ttl);
    cf->SetTimeToLive(schema.table_ttl());
    tera_adapter_.opt_.client_->CreateTable(primary_table_desc, &error_code);

    // create index key table
    std::vector<IndexDescription>::const_iterator it;
    for (it = table_desc.index_descriptor_list.begin();
         it != table_desc.index_descriptor_list.end();
         ++it) {
        std::string index_table_name = tera_adapter_.table_prefix_ + "#" + schema.table_name() + "#" + it->index_name;
        LOG(INFO) << "Create index table name " << index_table_name;
        tera::TableDescriptor index_table_desc(index_table_name);
        index_table_desc.SetRawKey(tera::kBinary);
        index_table_desc.SetSplitSize(10240);
        tera::LocalityGroupDescriptor* index_lg = index_table_desc.AddLocalityGroup("lg");
        //index_lg->SetBlockSize(32);
        index_lg->SetCompress(tera::kSnappyCompress);
        tera::ColumnFamilyDescriptor* index_cf = index_table_desc.AddColumnFamily("PrimaryKey", "lg");
        //index_cf->SetTimeToLive(FLAGS_tera_table_ttl);
        index_cf->SetTimeToLive(schema.table_ttl());
        tera_adapter_.opt_.client_->CreateTable(index_table_desc, &error_code);
    }

    // create timestamp table
    int nr_timestamp_table = (int)FLAGS_max_timestamp_table_num;
    for (int i = 0; i < nr_timestamp_table; i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string index_table_name = tera_adapter_.table_prefix_ + "#" + schema.table_name() + "#" + ts_name;
        LOG(INFO) << "Create index table name " << index_table_name;
        tera::TableDescriptor index_table_desc(index_table_name);
        index_table_desc.SetRawKey(tera::kBinary);
        index_table_desc.SetSplitSize(10240);
        tera::LocalityGroupDescriptor* index_lg = index_table_desc.AddLocalityGroup("lg");
        //index_lg->SetBlockSize(32);
        index_lg->SetCompress(tera::kSnappyCompress);
        tera::ColumnFamilyDescriptor* index_cf = index_table_desc.AddColumnFamily("PrimaryKey", "lg");
        //index_cf->SetTimeToLive(FLAGS_tera_table_ttl);
        index_cf->SetTimeToLive(schema.table_ttl());
        tera_adapter_.opt_.client_->CreateTable(index_table_desc, &error_code);
    }

    return Status::OK();
}

Status DatabaseImpl::OpenTable(const std::string& table_name, Table** table_ptr) {
    // read schema from schema table
    *table_ptr = NULL;
    tera::ErrorCode error_code;
    std::string schema_value;
    TableDescription table_desc;
    LOG(INFO) << "table name " << table_name << ", schema_table " << (uint64_t)tera_adapter_.opt_.schema_table_;
    if (!tera_adapter_.opt_.schema_table_->Get(table_name, "", "", &schema_value, &error_code)) {
        LOG(WARNING) << "OpenTable: not such table " << table_name << ", error code "
            << tera::strerr(error_code);
        return Status::NotFound("not such table");
    }
    LOG(INFO) << "OpenTable: get table schema, table name " << table_name
        << ", error code " << tera::strerr(error_code);

    // assemble TableDescription
    BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);
    DisassembleTableSchema(schema, &table_desc);

    if (table_map_.find(table_desc.table_name) != table_map_.end()) {
        *table_ptr = table_map_[table_name];
        return Status::OK();
    }

    if (FLAGS_multi_table_enable) {
        MultiTableImpl::OpenTable(db_name_, tera_opt_, fs_opt_, table_desc, table_ptr);
        if (*table_ptr == NULL) {
            return Status::NotFound("db open error ");
        }
    } else {
        // construct memory structure
        TableImpl::OpenTable(db_name_, tera_opt_, fs_opt_, table_desc, table_ptr);
        if (*table_ptr == NULL) {
            return Status::NotFound("db open error ");
        }
    }

    table_map_[table_name] = *table_ptr;
    return Status::OK();
}

Status DatabaseImpl::ReleaseTables() {
    std::map<std::string, Table*>::iterator it = table_map_.begin();
    for (; it != table_map_.end(); ++it) {
        delete it->second;
    }
    return Status::OK();
}

int DatabaseImpl::AssembleTableSchema(const TableDescription& table_desc,
                                      BigQueryTableSchema* schema) {
    schema->set_table_name(table_desc.table_name);
    schema->set_table_ttl(table_desc.table_ttl);
    schema->set_primary_key_type(table_desc.primary_key_type);
    LOG(INFO) << "Assemble: table name " << schema->table_name();
    std::vector<IndexDescription>::const_iterator it;
    for (it = table_desc.index_descriptor_list.begin();
         it != table_desc.index_descriptor_list.end();
         ++it) {
        IndexSchema* index;
        index = schema->add_index_descriptor_list();
        index->set_index_name(it->index_name);
        index->set_index_key_type(it->index_key_type);
        index->set_shuttle(it->shuttle);
        LOG(INFO) << "Assemble: index table name " << index->index_name();
    }
    return 0;
}

int DatabaseImpl::DisassembleTableSchema(const BigQueryTableSchema& schema,
                                         TableDescription* table_desc) {
    table_desc->table_name = schema.table_name();
    table_desc->table_ttl = schema.table_ttl();
    table_desc->primary_key_type = (TYPE)schema.primary_key_type();
    LOG(INFO) << "Disassemble: table name" << table_desc->table_name;
    for (int32_t i = 0; i < schema.index_descriptor_list_size(); i++) {
        const IndexSchema& index_schema = schema.index_descriptor_list(i);
        IndexDescription index_desc;
        index_desc.index_name = index_schema.index_name();
        index_desc.index_key_type = (TYPE)index_schema.index_key_type();
        index_desc.shuttle = index_schema.shuttle();
        table_desc->index_descriptor_list.push_back(index_desc);
        LOG(INFO) << "Disassemble: index table name " << index_desc.index_name;
    }
    return 0;
}

} // namespace mdt
