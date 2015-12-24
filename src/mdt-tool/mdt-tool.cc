#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <tera.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"

#include "proto/kv.pb.h"

DECLARE_string(flagfile);
DEFINE_string(tera_flagfile, "../conf/tera.flag", "tera flagfile");
DEFINE_int64(max_timestamp_tables, 10, "max number of ts tables");

char* StripWhite(char* line) {
    char *s, *t;
    for (s = line; whitespace(*s); s++);
    if (*s == 0)
        return s;
    t = s + strlen(s) - 1;
    while (t > s && whitespace(*t)) {
        t--;
    }
    *++t = '\0';
    return s;
}

static inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void HelpManue() {
    printf("========= usage ===========\n");
    printf("cmd: quit\n\n");
    printf("cmd: help\n\n");
    printf("cmd: CreateTable <dbname> <tablename> <primary_key_type> "
            "[<index_table> <index_type=kBytes,kUInt64>]\n\n");
    printf("cmd: Put <dbname> <tablename> <value> <primary_key> "
            "[<index_table> <key>]\n\n");
    printf("cmd: Get <dbname> <tablename> <start_ts> <end_ts> <limit> "
            "[<index_name> cmp[==, >=, >, <=, <] <index_key>]\n\n");
    printf("cmd: GetPri <dbname> <tablename> <start_ts> <end_ts> <limit> <primary_key>\n\n");
    printf("cmd: UpdateTable dbname tablename <table_prop_key> <tale_prop_value>\n\n");
    printf("cmd: UpdateLG dbname tablename lgname['lg'] <lg_prop_key> <lg_prop_value>\n\n");
    printf("cmd: UpdateCF dbname tablename lgname['lg'] cfname['Location', 'PrimaryKey'] "
            "<cf_prop_key> <cf_prop_value>\n\n");
    printf("cmd: showschema dbname tablename\n\n");
    printf("cmd: UpdateSingleTable internaltablename <table_prop_key> <tale_prop_value>\n\n");
    printf("cmd: dumpcache dbname tablename\n\n");
    printf("===========================\n");
}

int GetClientAndTableList(const std::string& db_name,
                          const std::string& table_name,
                          tera::Client** client_ptr,
                          std::vector<std::string>& table_list) {
    // new tera client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }
    *client_ptr = client;

    // get schema
    std::string schema_table = db_name + "#SchemaTable#";
    tera::Table* table = client->OpenTable(schema_table, &error);
    if (table == NULL) {
        std::cout << "mdt: schema table " << schema_table << " not exist\n";
        return -1;
    }
    std::string schema_value;
    if (!table->Get(table_name, "", "", &schema_value, &error)) {
        delete table;
        std::cout << " Get index table name from " << schema_table << " fail\n";
        return -1;
    }
    delete table;
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);
    std::cout << "SCHEMA:\n" << schema.DebugString() << "\n";

    // update primary table
    std::string primary_table = db_name + "#pri#" + table_name;
    table_list.push_back(primary_table);

    // update index table
    for (int i = 0; i < (int)(schema.index_descriptor_list_size()); i++) {
        const mdt::IndexSchema& index = schema.index_descriptor_list(i);
        std::string index_name = index.index_name();
        std::string index_table = db_name + "#" + table_name + "#" + index_name;
        table_list.push_back(index_table);
    }

    // update ts table
    for (int i = 0; i < (int)(FLAGS_max_timestamp_tables); i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string timestamp_table = db_name + "#" + table_name + "#" + ts_name;
        table_list.push_back(timestamp_table);
    }
    return 0;
}

// dumpcache with scan op
struct DumpCacheParam {
    tera::Client* client;
    std::string table_name;
};

int DumpCache(tera::Client* client, const std::string& table_name) {
    tera::ErrorCode err;
    tera::ScanDescriptor* scan_desc = new tera::ScanDescriptor("");
    scan_desc->SetEnd("");

    tera::Table* tera_table = client->OpenTable(table_name, &err);
    if (tera_table == NULL) {
        std::cout << "open tera table error\n";
        return -1;
    }
    tera::ResultStream* result = tera_table->Scan(*scan_desc, &err);
    while (!result->Done(&err)) {
        result->Next();
    }
    delete result;
    return 0;
}

void* DumpCacheThread(void* arg) {
    DumpCacheParam* param = (DumpCacheParam*)arg;
    DumpCache(param->client, param->table_name);
    return NULL;
}

int DumpCacheOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];

    tera::Client* client = NULL;
    std::vector<std::string> table_list;
    if (GetClientAndTableList(db_name, table_name, &client, table_list) < 0) {
        std::cout << "dump cache, open client or get table list error\n";
        return -1;
    }
    std::vector<pthread_t> tid_vec;
    std::vector<DumpCacheParam> param_vec;
    for (int32_t i = 0; i < (int32_t)table_list.size(); i++) {
        const std::string& tname = table_list[i];
        pthread_t tid;
        DumpCacheParam param;

        param.client = client;
        param.table_name = tname;
        param_vec.push_back(param);
        pthread_create(&tid, NULL, DumpCacheThread, &param_vec[i]);
        tid_vec.push_back(tid);
    }
    for (int32_t i = 0; i < (int32_t)table_list.size(); i++) {
        pthread_join(tid_vec[i], NULL);
    }
    return 0;
}

// showschema dbname tablename
int GetTeraTableSchema(tera::Client* client, const std::string& table_name) {
    tera::ErrorCode error;
    tera::TableDescriptor* desc = client->GetTableDescriptor(table_name, &error);
    if (desc == NULL) {
        std::cout << "get " << table_name << "'s schema faile\n";
        return -1;
    }
    return 0;
}

int ShowTableSchema(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];

    // open client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }
    // get schema
    std::string schema_table = db_name + "#SchemaTable#";
    tera::Table* table = client->OpenTable(schema_table, &error);
    if (table == NULL) {
        std::cout << "mdt: schema table " << schema_table << " not exist\n";
        return -1;
    }
    std::string schema_value;
    if (!table->Get(table_name, "", "", &schema_value, &error)) {
        delete table;
        std::cout << " Get index table name from " << schema_table << " fail\n";
        return -1;
    }
    delete table;
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);
    std::cout << "SCHEMA:\n" << schema.DebugString() << "\n";

    // update primary table
    std::string primary_table = db_name + "#pri#" + table_name;
    GetTeraTableSchema(client, primary_table);

    // update index table
    for (int i = 0; i < (int)(schema.index_descriptor_list_size()); i++) {
        const mdt::IndexSchema& index = schema.index_descriptor_list(i);
        std::string index_name = index.index_name();
        std::string index_table = db_name + "#" + table_name + "#" + index_name;
        GetTeraTableSchema(client, index_table);
    }

    // update ts table
    for (int i = 0; i < (int)(FLAGS_max_timestamp_tables); i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string timestamp_table = db_name + "#" + table_name + "#" + ts_name;
        GetTeraTableSchema(client, timestamp_table);
    }
    return 0;
}

// GetPri dbname tablename start_ts end_ts limit primary_key
int SearchPrimaryKey(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];
    uint64_t start_timestamp = boost::lexical_cast<uint64_t>(cmd_vec[3]);
    uint64_t end_timestamp = boost::lexical_cast<uint64_t>(cmd_vec[4]);
    int32_t limit = boost::lexical_cast<int32_t>(cmd_vec[5]);
    const std::string& primary_key = cmd_vec[6];

    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);
    if (db == NULL) {
        std::cout << "open db " << db_name << " fail...\n";
        return -1;
    }

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(db, table_name);
    if (table == NULL) {
        std::cout << "open table " << table_name << " fail...\n";
        return -1;
    }

    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    search_req->primary_key = primary_key;
    search_req->limit = limit;
    search_req->start_timestamp = start_timestamp;
    if (end_timestamp == 0) {
        search_req->end_timestamp = get_micros();
    } else {
        search_req->end_timestamp = end_timestamp;
    }
    mdt::SearchResponse* search_resp = new mdt::SearchResponse;

    table->Get(search_req, search_resp);
    std::cout << "=============================================\n";
    std::cout << "              Get by Primary Key             \n";
    std::cout << "=============================================\n";
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        const std::string& pri_key = result.primary_key;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "###PrimaryKey :" << pri_key << ", ###Value :" << result.result_data_list[j] << std::endl;
        }
    }
    return 0;
}

int GetCmp(std::string& cmp_str) {
    if (cmp_str.compare(">") == 0) {
        return (int)mdt::kGreater;
    } else if (cmp_str.compare(">=") == 0) {
        return (int)mdt::kGreaterEqual;
    } else if (cmp_str.compare("<") == 0) {
        return (int)mdt::kLess;
    } else if (cmp_str.compare("<=") == 0) {
        return (int)mdt::kLessEqual;
    } else if (cmp_str.compare("==") == 0) {
        return (int)mdt::kEqualTo;
    } else {
        //std::cout << "cmp " << cmp_str << " not support\n";
        return -1;
    }
    return -1;
}

// Get dbname tablename start end limit [index_table [>,>=,==,<,<=] key]
int GetOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];
    uint64_t start_timestamp = boost::lexical_cast<uint64_t>(cmd_vec[3]);
    uint64_t end_timestamp = boost::lexical_cast<uint64_t>(cmd_vec[4]);
    int32_t limit = boost::lexical_cast<int32_t>(cmd_vec[5]);

    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);
    if (db == NULL) {
        std::cout << "open db " << db_name << " fail...\n";
        return -1;
    }

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(db, table_name);
    if (table == NULL) {
        std::cout << "open table " << table_name << " fail...\n";
        return -1;
    }

    // search test
    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    //search_req->primary_key = cmd_vec[];
    search_req->limit = limit;
    search_req->start_timestamp = start_timestamp;
    if (end_timestamp == 0) {
        search_req->end_timestamp = get_micros();
    } else {
        search_req->end_timestamp = end_timestamp;
    }
    int num_index = cmd_vec.size() - 6;
    if (num_index % 3 != 0) {
        std::cout << "num of condition index not match\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 3) {
        mdt::IndexCondition index;
        int cmp;
        index.index_name = cmd_vec[i + 6];
        cmp = GetCmp(cmd_vec[i + 7]);
        if (cmp == -1) {
            std::cout << "cmp " << cmd_vec[i + 7] << " not support\n";
            return -1;
        }
        index.comparator = (mdt::COMPARATOR)cmp;
        index.compare_value = cmd_vec[i + 8];
        search_req->index_condition_list.push_back(index);
    }

    mdt::SearchResponse* search_resp = new mdt::SearchResponse;

    std::cout << "=============================================\n";
    std::cout << "              Get by Index Key               \n";
    std::cout << "=============================================\n";
    // calulate time
    struct timeval now_ts, finish_ts;
    gettimeofday(&now_ts, NULL);
    table->Get(search_req, search_resp);
    gettimeofday(&finish_ts, NULL);

    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        const std::string& pri_key = result.primary_key;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "###PrimaryKey :" << pri_key << ", ###Value :" << result.result_data_list[j] << std::endl;
        }
    }
    std::cout << "\n=============================================\n";
    std::cout << "search time: begin: tv_sec " << now_ts.tv_sec << ", tv_usec " << now_ts.tv_usec
        << ", now: tv_sec " << finish_ts.tv_sec << ", tv_usec " << finish_ts.tv_usec;
    std::cout << "\n=============================================\n";
    return 0;
}

void StoreCallback_Test(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    bool* store_finish = (bool*)callback_param;
    *store_finish = true;
}

//Put <dbname> <tablename> <value> <primary_key> [<index_table> <key>]
int PutOp(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& db_name = cmd_vec[1];
    const std::string& table_name = cmd_vec[2];
    const std::string& value = cmd_vec[3];
    const std::string& primary_key = cmd_vec[4];

    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);
    if (db == NULL) {
        std::cout << "open db " << db_name << " fail...\n";
        return -1;
    }

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    table = OpenTable(db, table_name);
    if (table == NULL) {
        std::cout << "open table " << table_name << " fail...\n";
        return -1;
    }

    // insert data
    mdt::StoreRequest* store_req = new mdt::StoreRequest();
    store_req->primary_key = primary_key;
    store_req->timestamp = get_micros();
    store_req->data = value;

    int num_index = cmd_vec.size() - 5;
    if (num_index % 2 != 0) {
        std::cout << "put fail, [index_table key] not match!\n";
        return -1;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::Index index;
        index.index_name = cmd_vec[i + 5];
        index.index_key = cmd_vec[i + 6];
        store_req->index_list.push_back(index);
    }

    mdt::StoreResponse* store_resp = new mdt::StoreResponse();
    mdt::StoreCallback callback = StoreCallback_Test;

    bool store_finish = false;
    std::cout << "put ..." << std::endl;
    table->Put(store_req, store_resp, callback, &store_finish);
    while (!store_finish) sleep(1);
    return 0;
}

int CreateTableOp(std::vector<std::string>& cmd_vec) {
    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = cmd_vec[1];
    db = mdt::OpenDatabase(db_name);

    // create table
    std::cout << "create table ..." << std::endl;
    mdt::TableDescription table_desc;
    table_desc.table_name = cmd_vec[2];
    if (cmd_vec[3].compare("kBytes") == 0) {
        table_desc.primary_key_type = mdt::kBytes;
    } else if (cmd_vec[3].compare("kUInt64") == 0) {
        table_desc.primary_key_type = mdt::kUInt64;
    } else if (cmd_vec[3].compare("kInt64") == 0) {
        table_desc.primary_key_type = mdt::kInt64;
    } else if (cmd_vec[3].compare("kUInt32") == 0) {
        table_desc.primary_key_type = mdt::kUInt32;
    } else if (cmd_vec[3].compare("kInt32") == 0) {
        table_desc.primary_key_type = mdt::kInt32;
    } else {
        std::cout << "create table fail, primary key type not support!, support kBytes, kUInt64...\n";
        return 0;
    }

    int num_index = cmd_vec.size() - 4;
    if (num_index % 2 != 0) {
        std::cout << "create table fail, [index_name index_type] not match!\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::IndexDescription table_index;
        table_index.index_name = cmd_vec[i + 4];
        if (cmd_vec[i + 5].compare("kBytes") == 0) {
            table_index.index_key_type = mdt::kBytes;
        } else if (cmd_vec[i + 5].compare("kUInt64") == 0) {
            table_index.index_key_type = mdt::kUInt64;
        } else if (cmd_vec[i + 5].compare("kInt64") == 0) {
            table_index.index_key_type = mdt::kInt64;
        } else if (cmd_vec[i + 5].compare("kUInt32") == 0) {
            table_index.index_key_type = mdt::kUInt32;
        } else if (cmd_vec[i + 5].compare("kInt32") == 0) {
            table_index.index_key_type = mdt::kInt32;
        } else {
            std::cout << "create table fail, index key: " << cmd_vec[i + 4]
                << ", key type not support!\n";
            return 0;
        }
        table_desc.index_descriptor_list.push_back(table_index);
    }
    CreateTable(db, table_desc);
    return 0;
}

/////////////////////////////////////////
// update table, lg, cf property
/////////////////////////////////////////
int UpdateTableProperty(tera::Client* client,
            const std::string& table_name,
            const std::string& table_prop_key,
            const std::string& table_prop_value) {
    tera::ErrorCode error;
    tera::TableDescriptor* desc = client->GetTableDescriptor(table_name, &error);
    if (desc == NULL) {
        std::cout << "table: " << table_name << "'desc not exit\n";
        return -1;
    }
    if (table_prop_key == "keytype") {
        if (table_prop_value == "binary") {
            desc->SetRawKey(tera::kBinary);
        } else if (table_prop_value == "readable") {
            desc->SetRawKey(tera::kReadable);
        } else if (table_prop_value == "kv") {
            desc->SetRawKey(tera::kGeneralKv);
        } else {
            std::cout << "not support keytype " << table_prop_value << std::endl;
            return -1;
        }
    } else if (table_prop_key == "splitsize") {
        int64_t splitsize = (int64_t)atoi(table_prop_value.c_str());
        desc->SetSplitSize(splitsize);
    } else if (table_prop_key == "mergesize") {
        int64_t mergesize = (int64_t)atoi(table_prop_value.c_str());
        desc->SetMergeSize(mergesize);
    } else {
        std::cout << "not support " << table_prop_key << ", value " << table_prop_value << std::endl;
        return -1;
    }

    // remote update prop
    if (!client->UpdateTable(*desc, &error)) {
        std::cout << "remote update error\n";
        return -1;
    }
    return 0;
}

// cmd: UpdateSingleTable internaltablename <table_prop_key> <tale_prop_value>
int UpdateSingleTableProp(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& table_name = cmd_vec[1];
    const std::string& prop_key = cmd_vec[2];
    const std::string& prop_value = cmd_vec[3];

    // open client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }

    UpdateTableProperty(client, table_name, prop_key, prop_value);
    return 0;
}

// cmd: UpdateTable dbname tablename <table_prop_key> <tale_prop_value>
int UpdateMdtTableProp(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& db_name = cmd_vec[1];
    const std::string& table_name = cmd_vec[2];
    const std::string& prop_key = cmd_vec[3];
    const std::string& prop_value = cmd_vec[4];

    // open client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }
    // get schema
    std::string schema_table = db_name + "#SchemaTable#";
    tera::Table* table = client->OpenTable(schema_table, &error);
    if (table == NULL) {
        std::cout << "mdt: schema table " << schema_table << " not exist\n";
        return -1;
    }
    std::string schema_value;
    if (!table->Get(table_name, "", "", &schema_value, &error)) {
        delete table;
        std::cout << " Get index table name from " << schema_table << " fail\n";
        return -1;
    }
    delete table;
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);

    // update primary table
    std::string primary_table = db_name + "#pri#" + table_name;
    UpdateTableProperty(client, primary_table, prop_key, prop_value);

    // update index table
    for (int i = 0; i < (int)(schema.index_descriptor_list_size()); i++) {
        const mdt::IndexSchema& index = schema.index_descriptor_list(i);
        std::string index_name = index.index_name();
        std::string index_table = db_name + "#" + table_name + "#" + index_name;
        UpdateTableProperty(client, index_table, prop_key, prop_value);
    }

    // update ts table
    for (int i = 0; i < (int)(FLAGS_max_timestamp_tables); i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string timestamp_table = db_name + "#" + table_name + "#" + ts_name;
        UpdateTableProperty(client, timestamp_table, prop_key, prop_value);
    }
    return 0;
}

// update single lg schema
int UpdateLocalityGroupProperty(tera::Client* client,
            const std::string& table_name,
            const std::string& lg_name,
            const std::string& lg_prop_key,
            const std::string& lg_prop_value) {
    tera::ErrorCode error;
    tera::TableDescriptor* desc = client->GetTableDescriptor(table_name, &error);
    if (desc == NULL) {
        std::cout << "table: " << table_name << "'desc not exit\n";
        return -1;
    }
    tera::LocalityGroupDescriptor* lg_desc = const_cast<tera::LocalityGroupDescriptor*>(desc->LocalityGroup(lg_name));
    if (lg_desc == NULL) {
        std::cout << "lg: " << lg_name << " not exit\n";
        return -1;
    }

    if (lg_prop_key == "storetype") {
        if (lg_prop_value == "mem") {
            lg_desc->SetStore(tera::kInMemory);
        } else if (lg_prop_value == "flash") {
            lg_desc->SetStore(tera::kInFlash);
        } else {
            lg_desc->SetStore(tera::kInDisk);
        }
    } else if (lg_prop_key == "blocksize") {
        int blocksize = atoi(lg_prop_value.c_str());
        lg_desc->SetBlockSize(blocksize);
    } else if (lg_prop_key == "sstsize") {
        int32_t sstsize = atoi(lg_prop_value.c_str());
        lg_desc->SetSstSize(sstsize);
    } else {
        std::cout << "not support update " << lg_prop_key << " = " << lg_prop_value << std::endl;
        return -1;
    }

    // remote update prop
    if (!client->UpdateTable(*desc, &error)) {
        std::cout << "remote update error\n";
        return -1;
    }
    return 0;
}

// cmd: UpdateLG dbname tablename lgname <lg_prop_key> <lg_prop_value>
int UpdateMdtLgProp(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& db_name = cmd_vec[1];
    const std::string& table_name = cmd_vec[2];
    const std::string& lg_name = cmd_vec[3];
    const std::string& prop_key = cmd_vec[4];
    const std::string& prop_value = cmd_vec[5];

    // open client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }
    // get schema
    std::string schema_table = db_name + "#SchemaTable#";
    tera::Table* table = client->OpenTable(schema_table, &error);
    if (table == NULL) {
        std::cout << "mdt: schema table " << schema_table << " not exist\n";
        return -1;
    }
    std::string schema_value;
    if (!table->Get(table_name, "", "", &schema_value, &error)) {
        delete table;
        std::cout << " Get index table name from " << schema_table << " fail\n";
        return -1;
    }
    delete table;
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);

    // update primary table
    std::string primary_table = db_name + "#pri#" + table_name;
    UpdateLocalityGroupProperty(client, primary_table, lg_name, prop_key, prop_value);

    // update index table
    for (int i = 0; i < (int)(schema.index_descriptor_list_size()); i++) {
        const mdt::IndexSchema& index = schema.index_descriptor_list(i);
        std::string index_name = index.index_name();
        std::string index_table = db_name + "#" + table_name + "#" + index_name;
        UpdateLocalityGroupProperty(client, index_table, lg_name, prop_key, prop_value);
    }

    // update ts table
    for (int i = 0; i < (int)(FLAGS_max_timestamp_tables); i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string timestamp_table = db_name + "#" + table_name + "#" + ts_name;
        UpdateLocalityGroupProperty(client, timestamp_table, lg_name, prop_key, prop_value);
    }
    return 0;

}

// update single cf prop
int UpdateColumnFamilyProperty(tera::Client* client,
             const std::string& table_name,
             const std::string& lg_name,
             const std::string& cf_name,
             const std::string& cf_prop_key,
             const std::string& cf_prop_value) {
    tera::ErrorCode error;
    tera::TableDescriptor* desc = client->GetTableDescriptor(table_name, &error);
    if (desc == NULL) {
        std::cout << "table: " << table_name << "'desc not exit\n";
        return -1;
    }
    tera::LocalityGroupDescriptor* lg_desc = const_cast<tera::LocalityGroupDescriptor*>(desc->LocalityGroup(lg_name));
    if (lg_desc == NULL) {
        std::cout << "lg: " << lg_name << " not exit\n";
        return -1;
    }
    tera::ColumnFamilyDescriptor* cf_desc = const_cast<tera::ColumnFamilyDescriptor*>(desc->ColumnFamily(cf_name));
    if (cf_desc == NULL) {
        std::cout << "cf: " << cf_name << " not exit\n";
        return -1;
    }

    // update cf propperty
    if (cf_prop_key == "ttl") {
        // TODO: ttl int32_t enough?
        uint64_t ttl = (int32_t)atoi(cf_prop_value.c_str());
        cf_desc->SetTimeToLive((int32_t)ttl);
    } else if (cf_prop_key == "maxversions") {
        int32_t maxversions = (int32_t)atoi(cf_prop_value.c_str());
        cf_desc->SetMaxVersions(maxversions);
    } else if (cf_prop_key == "minversions") {
        int32_t minversions = (int32_t)atoi(cf_prop_value.c_str());
        cf_desc->SetMinVersions(minversions);
    } else {
        std::cout << "cf " << cf_prop_key << " not support\n";
        return -1;
    }

    // remote update prop
    if (!client->UpdateTable(*desc, &error)) {
        std::cout << "remote update error\n";
        return -1;
    }
    return 0;
}

// cmd: UpdateCF dbname tablename lgname cfname <cf_prop_key> <cf_prop_value>
int UpdateTableCF(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& db_name = cmd_vec[1];
    const std::string& table_name = cmd_vec[2];
    const std::string& lg_name = cmd_vec[3];
    const std::string& cf_name = cmd_vec[4];
    const std::string& prop_key = cmd_vec[5];
    const std::string& prop_value = cmd_vec[6];

    // open client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "test_update", &error);
    if (client == NULL) {
        std::cout << "new tera::Client error\n";
        return -1;
    }
    // Get schema
    std::string schema_table = db_name + "#SchemaTable#";
    tera::Table* table = client->OpenTable(schema_table, &error);
    if (table == NULL) {
        std::cout << "mdt: schema table " << schema_table << " not exist\n";
        return -1;
    }
    std::string schema_value;
    if (!table->Get(table_name, "", "", &schema_value, &error)) {
        delete table;
        std::cout << " Get index table name from " << schema_table << " fail\n";
        return -1;
    }
    delete table;
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);

    // update primary table
    std::string primary_table = db_name + "#pri#" + table_name;
    UpdateColumnFamilyProperty(client, primary_table, lg_name, cf_name, prop_key, prop_value);

    // update index table
    for (int i = 0; i < (int)(schema.index_descriptor_list_size()); i++) {
        const mdt::IndexSchema& index = schema.index_descriptor_list(i);
        std::string index_name = index.index_name();
        std::string index_table = db_name + "#" + table_name + "#" + index_name;
        UpdateColumnFamilyProperty(client, index_table, lg_name, cf_name, prop_key, prop_value);
    }

    // update ts table
    for (int i = 0; i < (int)(FLAGS_max_timestamp_tables); i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string timestamp_table = db_name + "#" + table_name + "#" + ts_name;
        UpdateColumnFamilyProperty(client, timestamp_table, lg_name, cf_name, prop_key, prop_value);
    }
    return 0;
}

void ParseFlagFile(const std::string& flagfile) {
    std::cout << "default configure path, ../conf/mdt.flag, ../conf/tera.flag\n";
    if (access(flagfile.c_str(), F_OK) || access(FLAGS_tera_flagfile.c_str(), F_OK)) {
        exit(-1);
    }

    int ac = 1;
    char** av = new char*[2];
    av[0] = (char*)"dummy";
    av[1] = NULL;
    std::string local_flagfile = FLAGS_flagfile;
    FLAGS_flagfile = flagfile;
    ::google::ParseCommandLineFlags(&ac, &av, true);
    delete av;
    FLAGS_flagfile = local_flagfile;
    return;
}

int DupNfsSterr() {
    int res = 0;
    int fd = open("../log/mdt-tool.sterr", O_WRONLY);
    if (fd < 0) {
        std::cout << "open ../log/mdt-tool.sterr file error\n";
        return -1;
    }
    //dup2(2, fd);
    dup2(1, fd);
    return res;
}

int main(int ac, char* av[]) {
    /*
    if (DupNfsSterr() < 0) {
        return -1;
    }
    */
    // Parse flagfile
    ParseFlagFile("../conf/mdt.flag");
    while (1) {
        char *line = readline("mdt:");
        char *cmd = StripWhite(line);
        std::string command(cmd, strlen(cmd));

        // split cmd
        std::vector<std::string> cmd_vec;
        boost::split(cmd_vec, command, boost::is_any_of(" "), boost::token_compress_on);
        if (cmd_vec.size() == 0) {
            free(line);
            continue;
        } else if (cmd_vec[0].size() == 0) {
            free(line);
            continue;
        } else if (cmd_vec[0].compare("quit") == 0) {
            std::cout << "bye\n";
            return 0;
        } else if (cmd_vec[0].compare("help") == 0) {
            HelpManue();
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("CreateTable") == 0 && cmd_vec.size() >= 4) {
            // cmd: CreateTable dbname tablename primary_key_type [index_name index_type]...
            std::cout << "create table: dbname " << cmd_vec[1]
                << "tablename " << cmd_vec[2]
                << "\n";
            CreateTableOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("Put") == 0 && cmd_vec.size() >= 6) {
            // cmd: Put dbname tablename value primary_key timestamp [index_name index_key]
            PutOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("Get") == 0 && cmd_vec.size() >= 6) {
            // cmd: Get dbname tablename start_ts(ignore) end_ts(ignore) limit(ignore) [index_name cmp(=, >=, >, <=, <) index_key]
            GetOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("GetPri") == 0 && cmd_vec.size() == 7) {
            // cmd: GetPri dbname tablename start_ts(ignore) end_ts(ignore) limit(ignore) primary_key
            SearchPrimaryKey(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("GetPri") == 0 && cmd_vec.size() == 7) {
            // cmd: GetPri dbname tablename start_ts(ignore) end_ts(ignore) limit(ignore) primary_key
            SearchPrimaryKey(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("UpdateTable") == 0 && cmd_vec.size()== 5) {
            // cmd: UpdateTable dbname tablename <table_prop_key> <tale_prop_value>
            UpdateMdtTableProp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("UpdateLG") == 0 && cmd_vec.size() == 6) {
            // cmd: UpdateLG dbname tablename lgname <lg_prop_key> <lg_prop_value>
            UpdateMdtLgProp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("UpdateCF") == 0 && cmd_vec.size() == 7) {
            // cmd: UpdateCF dbname tablename lgname cfname <cf_prop_key> <cf_prop_value>
            UpdateTableCF(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("showschema") == 0 && cmd_vec.size() == 3) {
            ShowTableSchema(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("UpdateSingleTable") == 0 && cmd_vec.size() == 4) {
            UpdateSingleTableProp(cmd_vec);
            add_history(line);
            free(line);
            continue;
    } else if (cmd_vec[0].compare("dumpcache") == 0 && cmd_vec.size() == 3) {
            DumpCacheOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else {
            std::cout << "cmd not known\n";
            free(line);
            continue;
        }

        std::cout << command
            << ", size " << cmd_vec.size()
            << ", cmd " << cmd_vec[0]
            << "\n";
        free(line);
    }
    return 0;
}
