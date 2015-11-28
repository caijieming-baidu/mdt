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

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include "sdk/sdk.h"
#include "sdk/db.h"
#include "sdk/table.h"

#include "util/env.h"
#include "util/coding.h"

DECLARE_string(flagfile);

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
    printf("cmd: quit\n");
    printf("cmd: help\n");
    printf("cmd: CreateTable <dbname> <tablename> <primary_key_type> "
            "[<index_table> <index_type=kBytes,kUInt64>]\n");
    printf("cmd: Put <dbname> <tablename> <value> <primary_key> "
            "[<index_table> <key>]\n");
    printf("cmd: Get <dbname> <tablename> <start_ts> <end_ts> <limit> "
            "[<index_name> cmp[==, >=, >, <=, <] <index_key>]\n");
    printf("cmd: GetPri <dbname> <tablename> <start_ts> <end_ts> <limit> <primary_key>\n");
    printf("===========================\n");
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
    table->Get(search_req, search_resp);
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        const std::string& pri_key = result.primary_key;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "###PrimaryKey :" << pri_key << ", ###Value :" << result.result_data_list[j] << std::endl;
        }
    }
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

void ParseFlagFile(const std::string& flagfile) {
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
    std::cout << "default configure path, ../conf/mdt.flag\n";
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
        } else if (cmd_vec[0].size() == 0) {
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
