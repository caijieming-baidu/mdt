#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/regex.hpp>
#include <boost/lexical_cast.hpp>
#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <iostream>
#include <vector>
#include <sys/types.h>
#include <time.h>
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
#include "utils/env.h"
#include "utils/coding.h"
#include "rpc/rpc_client.h"
#include "proto/kv.pb.h"
#include "proto/scheduler.pb.h"
#include "proto/galaxy_log.pb.h"

DEFINE_string(tool_mode, "", "mdt-tool cmd mode, --tool_mode=i for interactive");
DEFINE_string(cmd, "", "non interactive mode's cmd");

// add watch path cmd
DEFINE_string(cmd_agent_addr, "self", "in non interactive mode, agent addr");
DEFINE_string(cmd_log_dir, "", "in non interactive mode, add watch log dir");

// add watch module stream
DEFINE_string(cmd_module_name, "", "in non interactive mode, add watch module");
DEFINE_string(cmd_module_file_name, "", "in non interactive mode, add watch module file");

DEFINE_string(tera_flagfile, "../conf/tera.flag", "tera flagfile");
DEFINE_int64(max_timestamp_tables, 10, "max number of ts tables");

DECLARE_string(flagfile);

DECLARE_string(scheduler_addr);
DECLARE_string(agent_service_port);

DEFINE_string(cmd_db_name, "", "db name");
DEFINE_string(cmd_table_name, "", "table name");
DEFINE_string(cmd_start_ts, "2016-04-06-16:10:00", "start timestamp");
DEFINE_string(cmd_end_ts, "2016-04-06-16:15:00", "end timestamp");
DEFINE_string(cmd_limit, "0", "number of result");
DEFINE_string(cmd_index_list, "", "key1,==,val1,key2,>=,val2");

DEFINE_string(cmd_monitor_flagfile, "../conf/monitor.conf", "json configure file");
DEFINE_string(cmd_index_flagfile, "../conf/index.conf", "json configure file");

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

            // GetByTime dbname tablename start(year-month-day-hour:min:sec)  end(year-month-day-hour:min:sec) limit [index_table [>,>=,==,<,<=] key]
void HelpManue() {
    printf("========= usage ===========\n");
    printf("cmd: quit\n\n");
    printf("cmd: help\n\n");
    printf("cmd: CreateTable <dbname> <tablename> <primary_key_type> <table_ttl> "
            "[<index_table> <index_type=kBytes,kUInt64>]\n\n");
    printf("cmd: GetByTime <dbname> <tablename> start(year-month-day-hour:min:sec) end(year-month-day-hour:min:sec) <limit> "
            "[<index_name> cmp[==, >=, >, <=, <] <index_key>]\n\n");
    printf("cmd: Put <dbname> <tablename> <value> <primary_key> "
            "[<index_table> <key>]\n\n");
    printf("cmd: VPut <dbname> <tablename> <val1,val2> <primary_key> "
            "[<index_table> <key>]\n\n");
    printf("cmd: Get <dbname> <tablename> <start_ts> <end_ts> <limit> "
            "[<index_name> cmp[==, >=, >, <=, <] <index_key>]\n\n");
    printf("cmd: GetPri <dbname> <tablename> <start_ts> <end_ts> <limit> <primary_key>\n\n");
    printf("cmd: UpdateTable dbname tablename <table_prop_key> <tale_prop_value>\n\n");
    printf("cmd: UpdateLG dbname tablename lgname['lg'] <lg_prop_key> <lg_prop_value>\n\n");
    printf("cmd: UpdateCF dbname tablename lgname['lg'] cfname['Location', 'PrimaryKey'] "
            "<cf_prop_key> <cf_prop_value>\n\n");
    printf("cmd: ShowSchema dbname tablename\n\n");
    printf("cmd: UpdateSingleTable internaltablename <table_prop_key> <tale_prop_value>\n\n");
    printf("cmd: DumpCache dbname tablename\n\n");
    printf("cmd: AddWatchPath agent_addr[hostname:port or self] log_dir\n\n");
    printf("cmd: AddWatchModuleStream agent_addr[hostname:port or self] module_name file_name\n\n");
    printf("cmd: ShowAgent\n\n");
    printf("cmd: ShowCollector\n\n");
    printf("cmd: GalaxyShow <dbname> <tablename> start(year-month-day-hour:min:sec) end(year-month-day-hour:min:sec) <limit> [index cmp value]\n\n");
    printf("cmd: PushTraceLog <job_name> <work_dir> <user_log_dir> <db_name> <table_name> <parse_path_fn> <nexus_root_path> <master_path> <nexus_servers> \n\n");
    printf("cmd: SetMonitor <conf>\n\n");
    printf("===========================\n");
}

int GetClientAndTableList(const std::string& db_name,
                          const std::string& table_name,
                          tera::Client** client_ptr,
                          std::vector<std::string>& table_list) {
    // new tera client
    tera::ErrorCode error;
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    //delete table;
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
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    //delete table;
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

uint64_t TranslateTime(std::string ts_str) {
    std::vector<std::string> ts_vec;
    boost::split(ts_vec, ts_str, boost::is_any_of("-"));
    if (ts_vec.size() != 4) {
        return 0;
    }
    uint64_t ts_year = boost::lexical_cast<uint64_t>(ts_vec[0]);
    uint64_t ts_month = boost::lexical_cast<uint64_t>(ts_vec[1]);
    uint64_t ts_day = boost::lexical_cast<uint64_t>(ts_vec[2]);

    std::string hms_ts_str = ts_vec[3];
    std::vector<std::string> hms_ts_vec;
    boost::split(hms_ts_vec, hms_ts_str, boost::is_any_of(":"));
    if (hms_ts_vec.size() != 3) {
        return 0;
    }

    uint64_t ts_hour = boost::lexical_cast<uint64_t>(hms_ts_vec[0]);
    uint64_t ts_min = boost::lexical_cast<uint64_t>(hms_ts_vec[1]);
    uint64_t ts_sec = boost::lexical_cast<uint64_t>(hms_ts_vec[2]);

    // time convert
    time_t rawtime;
    struct tm * timeinfo;
    time( &rawtime );
    timeinfo = localtime(&rawtime);
    timeinfo->tm_year = (int)(ts_year - 1900);
    timeinfo->tm_mon = (int)(ts_month - 1);
    timeinfo->tm_mday = (int)ts_day;
    timeinfo->tm_hour = (int)ts_hour;
    timeinfo->tm_min = (int)ts_min;
    timeinfo->tm_sec = (int)ts_sec;
    time_t now_ts = mktime(timeinfo);

    struct timeval tv;
    gettimeofday(&tv, NULL);
    //std::cout << "year " << ts_year << ", month " << ts_month << ", day " << ts_day
    //    << ", hour " << ts_hour << ", min " << ts_min << ", sec " << ts_sec
    //    << ", convert to sec " << (uint64_t)now_ts << ", tv.sec " << tv.tv_sec << std::endl;
    return (uint64_t)(now_ts) * 1000000;
}

//  GalaxyShow <dbname> <tablename> start(year-month-day-hour:min:sec) end(year-month-day-hour:min:sec) <limit> [index cmp value]
int GalaxyShowOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];
    uint64_t start_timestamp = TranslateTime(cmd_vec[3]);
    uint64_t end_timestamp = TranslateTime(cmd_vec[4]);
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
    std::cout << "              Galaxy Show                    \n";
    std::cout << "=============================================\n";
    // calulate time
    struct timeval now_ts, finish_ts;
    gettimeofday(&now_ts, NULL);
    table->Get(search_req, search_resp);
    gettimeofday(&finish_ts, NULL);

    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            const std::string& pb_data = result.result_data_list[j];
            if (table_name == "TaskEvent") {
                ::baidu::galaxy::TaskEvent task_event;
                task_event.ParseFromString(pb_data);
                std::cout << task_event.DebugString()  << std::endl;
            } else if (table_name == "JobStat") {
                ::baidu::galaxy::JobStat job_stat;
                job_stat.ParseFromString(pb_data);
                std::cout << job_stat.DebugString()  << std::endl;
            } else if (table_name == "JobEvent") {
                ::baidu::galaxy::JobEvent job_event;
                job_event.ParseFromString(pb_data);
                std::cout << job_event.DebugString()  << std::endl;
            } else if (table_name == "PodStat") {
                ::baidu::galaxy::PodStat pod_stat;
                pod_stat.ParseFromString(pb_data);
                std::cout << pod_stat.DebugString()  << std::endl;
            } else if (table_name == "PodEvent") {
                ::baidu::galaxy::PodEvent pod_event;
                pod_event.ParseFromString(pb_data);
                std::cout << pod_event.DebugString()  << std::endl;
            } else if (table_name == "AgentStat") {
                ::baidu::galaxy::AgentStat agent_stat;
                agent_stat.ParseFromString(pb_data);
                std::cout << agent_stat.DebugString()  << std::endl;
            } else if (table_name == "AgentEvent") {
                ::baidu::galaxy::AgentEvent agent_event;
                agent_event.ParseFromString(pb_data);
                std::cout << agent_event.DebugString()  << std::endl;
            } else if (table_name == "ClusterStat") {
                ::baidu::galaxy::ClusterStat cluster_stat;
                cluster_stat.ParseFromString(pb_data);
                std::cout << cluster_stat.DebugString()  << std::endl;
            }
        }
    }
    std::cout << "\n=============================================\n";
    std::cout << "cost time(sec): " << finish_ts.tv_sec - now_ts.tv_sec;
    std::cout << "\n=============================================\n";
    return 0;
}

// GetByTime dbname tablename start(year-month-day-hour:min:sec)  end(year-month-day-hour:min:sec) limit [index_table [>,>=,==,<,<=] key]
int GetByTimeOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string db_name = cmd_vec[1];
    std::string table_name = cmd_vec[2];
    uint64_t start_timestamp = TranslateTime(cmd_vec[3]);
    uint64_t end_timestamp = TranslateTime(cmd_vec[4]);
    int32_t limit = boost::lexical_cast<int32_t>(cmd_vec[5]);

    // create db
    mdt::Database* db;
    db = mdt::OpenDatabase(db_name);
    if (db == NULL) {
        std::cout << "open db " << db_name << " fail...\n";
        return -1;
    }

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
    // calulate time
    struct timeval now_ts, finish_ts;
    gettimeofday(&now_ts, NULL);
    table->Get(search_req, search_resp);
    gettimeofday(&finish_ts, NULL);

    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        const std::string& pri_key = result.primary_key;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << pri_key << ":" << result.result_data_list[j] << std::endl;
        }
    }
    std::cout << "\n=============================================\n";
    //std::cout << "search time: begin: tv_sec " << now_ts.tv_sec << ", tv_usec " << now_ts.tv_usec
    //    << ", now: tv_sec " << finish_ts.tv_sec << ", tv_usec " << finish_ts.tv_usec;
    std::cout << "cost time(sec): " << finish_ts.tv_sec - now_ts.tv_sec;
    std::cout << "\n=============================================\n";
    return 0;
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
        std::cout << ">>> command format error:\n\texample: Get ktrace trace 0 0 1 [ac.tm > 0]\n";
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
            std::cout << pri_key << ":" << result.result_data_list[j] << std::endl;
        }
    }
    std::cout << "\n=============================================\n";
    //std::cout << "search time: begin: tv_sec " << now_ts.tv_sec << ", tv_usec " << now_ts.tv_usec
    //    << ", now: tv_sec " << finish_ts.tv_sec << ", tv_usec " << finish_ts.tv_usec;
    std::cout << "cost time: " << finish_ts.tv_sec - now_ts.tv_sec;
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

//VPut <dbname> <tablename> <value> <primary_key> [<index_table> <key>]
int VPutOp(std::vector<std::string>& cmd_vec) {
    // parse param
    const std::string& db_name = cmd_vec[1];
    const std::string& table_name = cmd_vec[2];
    const std::string& value = cmd_vec[3];
    std::vector<std::string> value_vec;
    boost::split(value_vec, value, boost::is_any_of(","), boost::token_compress_on);
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
    store_req->vec_data = value_vec;

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
// cmd: CreateTable dbname tablename primary_key_type ttl [index_name index_type]...
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

    int64_t ttl = atol(cmd_vec[4].c_str());
    table_desc.table_ttl = ttl > 0? ttl : 0;

    int num_index = cmd_vec.size() - 5;
    if (num_index % 2 != 0) {
        std::cout << "create table fail, [index_name index_type] not match!\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::IndexDescription table_index;
        table_index.index_name = cmd_vec[i + 5];
        if (cmd_vec[i + 6].compare("kBytes") == 0) {
            table_index.index_key_type = mdt::kBytes;
        } else if (cmd_vec[i + 6].compare("kUInt64") == 0) {
            table_index.index_key_type = mdt::kUInt64;
        } else if (cmd_vec[i + 6].compare("kInt64") == 0) {
            table_index.index_key_type = mdt::kInt64;
        } else if (cmd_vec[i + 6].compare("kUInt32") == 0) {
            table_index.index_key_type = mdt::kUInt32;
        } else if (cmd_vec[i + 6].compare("kInt32") == 0) {
            table_index.index_key_type = mdt::kInt32;
        } else {
            std::cout << "create table fail, index key: " << cmd_vec[i + 5]
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
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    //delete table;
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
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    //delete table;
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
    tera::Client* client = tera::Client::NewClient(FLAGS_tera_flagfile, "mdt", &error);
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
    mdt::BigQueryTableSchema schema;
    schema.ParseFromString(schema_value);
    LOG(INFO) << "old shcema " << schema.DebugString();
    if (prop_key == "ttl") {
        uint64_t ttl = (int32_t)atoi(prop_value.c_str());
        schema.set_table_ttl(ttl);
        LOG(INFO) << "new shcema " << schema.DebugString();
        std::string new_schema_str;
        schema.SerializeToString(&new_schema_str);
        table->Put(schema.table_name(), "", "", new_schema_str, &error);
        LOG(INFO) << "Put Schema: table name " << schema.table_name() << ", size " << schema_value.size()
            << ", error code " << tera::strerr(error);
    }
    //delete table;

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
    std::cout << "[default configure path: ../conf/trace.flag]\n";
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

// cmd::AddWatchModuleStream agent_addr[hostname:port or self] module_name file_name
int AddWatchModuleStreamOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string agent_addr = cmd_vec[1];
    const std::string& module_name = cmd_vec[2];
    const std::string& file_name = cmd_vec[3];

    if (agent_addr == "self") {
        char hostname[255];
        if (0 != gethostname(hostname, 256)) {
            LOG(FATAL) << "fail to report message";
        }
        std::string hostname_str = hostname;
        agent_addr = hostname_str + ":" + FLAGS_agent_service_port;
    }
    std::string scheduler_addr = FLAGS_scheduler_addr;

    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);
    mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* req = new mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest();
    mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* resp = new mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse();
    req->set_agent_addr(agent_addr);
    req->set_production_name(module_name);
    req->set_log_name(file_name);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcAddWatchModuleStream, req, resp);

    delete req;
    delete resp;
    delete service;
    return 0;

}

// cmd::AddWatchPath agent_addr[hostname:port or self] log_dir
int AddWatchPathOp(std::vector<std::string>& cmd_vec) {
    // parse param
    std::string agent_addr = cmd_vec[1];
    const std::string& log_dir = cmd_vec[2];
    //const std::string& module_name = cmd_vec[3];

    if (agent_addr == "self") {
        char hostname[255];
        if (0 != gethostname(hostname, 256)) {
            LOG(FATAL) << "fail to report message";
        }
        std::string hostname_str = hostname;
        agent_addr = hostname_str + ":" + FLAGS_agent_service_port;
    }
    std::string scheduler_addr = FLAGS_scheduler_addr;

    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);
    mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* req = new mdt::LogSchedulerService::RpcAddAgentWatchPathRequest();
    mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* resp = new mdt::LogSchedulerService::RpcAddAgentWatchPathResponse();
    req->set_agent_addr(agent_addr);
    req->set_watch_path(log_dir);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcAddAgentWatchPath, req, resp);

    delete req;
    delete resp;
    delete service;
    return 0;
}

int ShowAgent(std::vector<std::string>& cmd_vec) {
    std::string scheduler_addr = FLAGS_scheduler_addr;
    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);

    mdt::LogSchedulerService::RpcShowAgentInfoRequest* req = new mdt::LogSchedulerService::RpcShowAgentInfoRequest();
    mdt::LogSchedulerService::RpcShowAgentInfoResponse* resp = new mdt::LogSchedulerService::RpcShowAgentInfoResponse();
    req->set_id(1);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcShowAgentInfo, req, resp);

    char headers[2048] = {'\0'};
    snprintf(headers, sizeof(headers),
            "%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t",
            35, "agent_addr",
            35, "collector_addr",
            10, "qps_use",
            10, "bandwidth_use",
            10, "average_packet_size",
            10, "error_nr",
            10, "fd_using",
            10, "fd_overflow",
            10, "req_pending");
    std::string header_line;
    header_line.resize(strlen(headers) + 8);
    std::fill(header_line.begin(), header_line.end(), '-');

    std::cout << headers << std::endl
        << header_line << std::endl;
    for (int i = 0; i < resp->info_size(); i++) {
        const mdt::LogSchedulerService::AgentInformation& info = resp->info(i);
        const mdt::LogSchedulerService::AgentInfo& agent_info = info.agent_info();
        char line_str[1024] = {'\0'};
        snprintf(line_str, sizeof(line_str),
                "%-*s\t%-*s\t%-*ld\t%-*ld\t%-*ld\t%-*d\t%-*ld\t%-*ld\t%-*ld\t",
                50, info.agent_addr().c_str(),
                50, info.collector_addr().c_str(),
                5, agent_info.qps_use(),
                5, agent_info.bandwidth_use(),
                5, agent_info.average_packet_size(),
                5, agent_info.error_nr(),
                5, agent_info.nr_file_streams(),
                5, agent_info.history_fd_overflow_count(),
                5, agent_info.curr_pending_req());
        std::cout << line_str << std::endl;
    }

    delete req;
    delete resp;
    delete service;
    return 0;
}

int ShowCollector(std::vector<std::string>& cmd_vec) {
    std::string scheduler_addr = FLAGS_scheduler_addr;
    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);

    mdt::LogSchedulerService::RpcShowCollectorInfoRequest* req = new mdt::LogSchedulerService::RpcShowCollectorInfoRequest();
    mdt::LogSchedulerService::RpcShowCollectorInfoResponse* resp = new mdt::LogSchedulerService::RpcShowCollectorInfoResponse();
    req->set_id(1);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcShowCollectorInfo, req, resp);

    char headers[2048] = {'\0'};
    snprintf(headers, sizeof(headers),
            "%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t%-*s\t",
            35, "collector_addr",
            10, "nr_agent",
            10, "qps",
            10, "average_packet_size",
            10, "error_nr",
            10, "store_pending",
            10, "store_sched_ts",
            10, "store_task_ts",
            10, "store_task_num");
    std::string header_line;
    header_line.resize(strlen(headers) + 8);
    std::fill(header_line.begin(), header_line.end(), '-');

    std::cout << headers << std::endl
        << header_line << std::endl;
    for (int i = 0; i < resp->info_size(); i++) {
        const mdt::LogSchedulerService::CollectorInformation& info = resp->info(i);
        const mdt::LogSchedulerService::CollectorInfo& collector_info = info.collector_info();
        char line_str[1024] = {'\0'};
        snprintf(line_str, sizeof(line_str),
                "%-*s\t%-*ld\t%-*ld\t%-*ld\t%-*ld\t%-*ld\t%-*ld\t%-*ld\t%-*ld\t",
                50, info.collector_addr().c_str(),
                5, info.nr_agents(),
                5, collector_info.qps(),
                5, collector_info.average_packet_size(),
                5, info.error_nr(),
                5, collector_info.store_pending(),
                5, collector_info.store_sched_ts(),
                5, collector_info.store_task_ts(),
                5, collector_info.store_task_num());
        std::cout << line_str << std::endl;
    }

    delete req;
    delete resp;
    delete service;
    return 0;
}

// cmd: PushTraceLog <job_name> <work_dir> <user_log_dir> <db_name> <table_name> <parse_path_fn> <nexus_root_path> <master_path> <nexus_servers>
int PushTraceLog(std::vector<std::string>& cmd_vec) {
    std::string job_name = cmd_vec[1];
    std::string work_dir = cmd_vec[2];
    std::string user_log_dir = cmd_vec[3];
    std::string db_name = cmd_vec[4];
    std::string table_name = cmd_vec[5];
    int64_t parse_path_fn = atol(cmd_vec[6].c_str());
    std::string nexus_root_path = cmd_vec[7];
    std::string master_path = cmd_vec[8];
    std::string nexus_servers = cmd_vec[9];

    std::string scheduler_addr = FLAGS_scheduler_addr;
    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);

    mdt::LogSchedulerService::RpcTraceGalaxyAppRequest* req = new mdt::LogSchedulerService::RpcTraceGalaxyAppRequest();
    mdt::LogSchedulerService::RpcTraceGalaxyAppResponse* resp = new mdt::LogSchedulerService::RpcTraceGalaxyAppResponse();
    req->set_job_name(job_name);
    req->set_work_dir(work_dir);
    req->set_user_log_dir(user_log_dir);
    req->set_db_name(db_name);
    req->set_table_name(table_name);
    req->set_nexus_root_path(nexus_root_path);
    req->set_master_path(master_path);
    req->set_nexus_servers(nexus_servers);
    req->set_parse_path_fn(parse_path_fn);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcTraceGalaxyApp, req, resp);
    delete req;
    delete resp;
    delete service;
    return 0;
}

void ParseJsonRule(boost::property_tree::ptree& ptree, mdt::LogSchedulerService::Rule* rule) {
    try {
        // parse expression
        boost::property_tree::ptree expr_ptree = ptree.get_child("expression");
        mdt::LogSchedulerService::Expression* expr = rule->mutable_expr();
        expr->set_type(expr_ptree.get<std::string>("type"));
        expr->set_expr(expr_ptree.get<std::string>("expr"));
        expr->set_column_delim(expr_ptree.get<std::string>("col_delim"));
        expr->set_column_idx(expr_ptree.get<std::int64_t>("col_idx"));

        // parse record
        BOOST_FOREACH(boost::property_tree::ptree::value_type &v, ptree.get_child("record")) {
            mdt::LogSchedulerService::Record* record = rule->add_record_vec();
            record->set_op(v.second.get<std::string>("op"));
            record->set_type(v.second.get<std::string>("type"));
            record->set_key(v.second.get<std::string>("key"));
            record->set_key_name(v.second.get<std::string>("key_name"));
        }
    } catch (boost::property_tree::ptree_error& e) {}
    return;
}

void ParseJsonToRequest(const std::string& flagfile, mdt::LogSchedulerService::RpcMonitorRequest* req) {
    try {
        boost::property_tree::ptree ptree;
        boost::property_tree::read_json(flagfile, ptree);
        std::string db_name = ptree.get<std::string>("db_name");
        std::string table_name = ptree.get<std::string>("table_name");
        // set db and table
        req->set_db_name(db_name);
        req->set_table_name(table_name);

        // parse mail list
        std::vector<std::string> mail_list;
        BOOST_FOREACH(boost::property_tree::ptree::value_type &v, ptree.get_child("mail_list")) {
            std::string* owner = req->add_moduler_owner();
            *owner = v.second.get<std::string>("owner");
        }

        // parse result
        boost::property_tree::ptree rule_result = ptree.get_child("rule_set.result");
        mdt::LogSchedulerService::Rule* rule = req->mutable_rule_set()->mutable_result();
        ParseJsonRule(rule_result, rule);

        // parse rule list
        BOOST_FOREACH(boost::property_tree::ptree::value_type &v1, ptree.get_child("rule_set.rule_list")) {
            mdt::LogSchedulerService::Rule* rule_v = req->mutable_rule_set()->add_rule_list();
            ParseJsonRule(v1.second, rule_v);
        }
        std::cout << req->DebugString() << std::endl;
    } catch (boost::property_tree::ptree_error& e) {}
    return;
}

int SetMonitor(std::vector<std::string>& cmd_vec) {
    std::string jsonfile = cmd_vec[1];

    std::string scheduler_addr = FLAGS_scheduler_addr;
    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);

    mdt::LogSchedulerService::RpcMonitorRequest* req = new mdt::LogSchedulerService::RpcMonitorRequest();
    mdt::LogSchedulerService::RpcMonitorResponse* resp = new mdt::LogSchedulerService::RpcMonitorResponse();
    ParseJsonToRequest(jsonfile, req);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcMonitor, req, resp);
    delete req;
    delete resp;
    delete service;
    return 0;
}

void ParseJsonToUpdateIndexRequest(const std::string& flagfile, mdt::LogSchedulerService::RpcUpdateIndexRequest* req) {
    try {
        boost::property_tree::ptree ptree;
        boost::property_tree::read_json(flagfile, ptree);
        std::string db_name = ptree.get<std::string>("db_name");
        std::string table_name = ptree.get<std::string>("table_name");
        std::string primary_key = ptree.get<std::string>("primary_key");
        std::string timestamp = ptree.get<std::string>("timestamp");

        // set db and table
        req->set_db_name(db_name);
        req->set_table_name(table_name);
        req->set_primary_key(primary_key);
        req->set_timestamp(timestamp);

        // parse rule list
        BOOST_FOREACH(boost::property_tree::ptree::value_type &v1, ptree.get_child("rule_list")) {
            mdt::LogSchedulerService::Rule* rule_v = req->add_rule_list();
            ParseJsonRule(v1.second, rule_v);
        }
        std::cout << req->DebugString() << std::endl;
    } catch (boost::property_tree::ptree_error& e) {}
    return;
}

int UpdateIndex(std::vector<std::string>& cmd_vec) {
    std::string jsonfile = cmd_vec[1];

    std::string scheduler_addr = FLAGS_scheduler_addr;
    mdt::RpcClient* rpc_client = new mdt::RpcClient;
    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client->GetMethodList(scheduler_addr, &service);

    mdt::LogSchedulerService::RpcUpdateIndexRequest* req = new mdt::LogSchedulerService::RpcUpdateIndexRequest();
    mdt::LogSchedulerService::RpcUpdateIndexResponse* resp = new mdt::LogSchedulerService::RpcUpdateIndexResponse();
    ParseJsonToUpdateIndexRequest(jsonfile, req);

    rpc_client->SyncCall(service, &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcUpdateIndex, req, resp);
    delete req;
    delete resp;
    delete service;
    return 0;
}

int main(int ac, char* av[]) {
    /*
    if (DupNfsSterr() < 0) {
        return -1;
    }
    */
    // parse cmd in interactive mode
    ::google::ParseCommandLineFlags(&ac, &av, true);
    // Parse flagfile
    ParseFlagFile("../conf/trace.flag");
    if (FLAGS_tool_mode == "") {
        std::vector<std::string> non_interactive_cmd_vec;
        if (FLAGS_cmd == "AddWatchPath") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_agent_addr);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_log_dir);
            std::cout << "add watch path: agent_addr " << non_interactive_cmd_vec[1]
                << ", log dir " << non_interactive_cmd_vec[2] << "\n";
            AddWatchPathOp(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "AddWatchModuleStream") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_agent_addr);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_module_name);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_module_file_name);
            std::cout << "add module stream: agent_adr " << non_interactive_cmd_vec[1]
                << ", module name " << non_interactive_cmd_vec[2]
                << ", file name " << non_interactive_cmd_vec[3] << "\n";
            AddWatchModuleStreamOp(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "ShowAgent") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            ShowAgent(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "ShowCollector") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            ShowCollector(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "SetMonitor") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_monitor_flagfile);
            SetMonitor(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "UpdateIndex") {
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_index_flagfile);
            UpdateIndex(non_interactive_cmd_vec);
        } else if (FLAGS_cmd == "GetByTime") {
            // cmd: GetByTime <dbname> <tablename> start(year-month-day-hour:min:sec) end(year-month-day-hour:min:sec) <limit>
            // key1,>=,value1,key2,==,value2
            non_interactive_cmd_vec.push_back(FLAGS_cmd);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_db_name);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_table_name);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_start_ts);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_end_ts);
            non_interactive_cmd_vec.push_back(FLAGS_cmd_limit);

            std::vector<std::string> index_vec;
            boost::split(index_vec, FLAGS_cmd_index_list, boost::is_any_of(" "));
            if ((index_vec.size() % 3) == 0) {
                for (uint32_t idx = 0; idx < index_vec.size(); idx += 3) {
                    non_interactive_cmd_vec.push_back(index_vec[idx + 0]);
                    non_interactive_cmd_vec.push_back(index_vec[idx + 1]);
                    non_interactive_cmd_vec.push_back(index_vec[idx + 2]);
                }
            }
            GetByTimeOp(non_interactive_cmd_vec);
        } else {
            std::cout << "interactive mode, cmd " << FLAGS_cmd << " not know\n";
            HelpManue();
            exit(-1);
        }
        return 0;
    }

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
        } else if (cmd_vec[0].compare("AddWatchPath") == 0 && cmd_vec.size() >= 3) {
            // cmd::AddWatchPath agent_addr[hostname:port or self] log_dir
            std::cout << "add watch path: agent_addr " << cmd_vec[1]
                << ", log dir " << cmd_vec[2] << "\n";
            AddWatchPathOp(cmd_vec);
            add_history(line);
            continue;
        } else if (cmd_vec[0].compare("CreateTable") == 0 && cmd_vec.size() >= 5) {
            // cmd: CreateTable dbname tablename primary_key_type ttl [index_name index_type]...
            std::cout << "create table: dbname " << cmd_vec[1]
                << "tablename " << cmd_vec[2]
                << "\n";
            CreateTableOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("Put") == 0 && cmd_vec.size() >= 5) {
            // cmd: Put dbname tablename value primary_key timestamp [index_name index_key]
            PutOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("VPut") == 0 && cmd_vec.size() >= 5) {
            // cmd: VPut dbname tablename value,val1,val2 primary_key timestamp [index_name index_key]
            VPutOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("GetByTime") == 0 && cmd_vec.size() >= 6) {
            // GetByTime dbname tablename start(year-month-day-hour:min:sec)  end(year-month-day-hour:min:sec) limit [index_table [>,>=,==,<,<=] key]
            GetByTimeOp(cmd_vec);
            add_history(line);
            continue;
        } else if (cmd_vec[0].compare("GalaxyShow") == 0 && cmd_vec.size() >= 6) {
            //  GalaxyShow <dbname> <tablename> start(year-month-day-hour:min:sec) end(year-month-day-hour:min:sec) <limit> [index cmp value]
            GalaxyShowOp(cmd_vec);
            add_history(line);
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
        } else if (cmd_vec[0].compare("ShowSchema") == 0 && cmd_vec.size() == 3) {
            ShowTableSchema(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("UpdateSingleTable") == 0 && cmd_vec.size() == 4) {
            UpdateSingleTableProp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("DumpCache") == 0 && cmd_vec.size() == 3) {
            DumpCacheOp(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("ShowAgent") == 0 && cmd_vec.size() == 1) {
            ShowAgent(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("ShowCollector") == 0 && cmd_vec.size() == 1) {
            ShowCollector(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else if ((cmd_vec[0].compare("PushTraceLog") == 0) && (cmd_vec.size() == 10)) {
            PushTraceLog(cmd_vec);
            add_history(line);
            free(line);
            continue;
        } else {
            std::cout << "cmd not known\n";
            add_history(line);
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
