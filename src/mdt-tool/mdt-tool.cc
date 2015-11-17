#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <iostream>
#include <vector>

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

void HelpManue() {
    printf("========= usage ===========\n");
    printf("cmd: quit\n");
    printf("cmd: help\n");
    printf("cmd: CreateTable dbname tablename primary_key_type [index_name index_type]...\n");
    printf("cmd(TEST): Put dbname tablename value primary_key timestamp(ingore) [index_name index_key]...\n");
    printf("cmd: Get dbname tablename start_ts(ignore) end_ts(ignore) limit(ignore) "
            "[index_name cmp(=, >=, >, <=, <) index_key]...\n");
    printf("===========================\n");
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

int GetOp(std::vector<std::string>& cmd_vec) {
    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = cmd_vec[1];
    db = mdt::OpenDatabase(db_name);

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    std::string table_name = cmd_vec[2];
    table = OpenTable(db, table_name);

    // search test
    mdt::SearchRequest* search_req = new mdt::SearchRequest;
    //search_req->primary_key = cmd_vec[];
    search_req->limit = 10;
    search_req->start_timestamp = 0;
    search_req->end_timestamp = time(NULL);
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
            return 0;
        }
        index.comparator = (mdt::COMPARATOR)cmp;
        index.compare_value = cmd_vec[i + 8];
        search_req->index_condition_list.push_back(index);
    }

    mdt::SearchResponse* search_resp = new mdt::SearchResponse;

    std::cout << "get ..." << std::endl;
    table->Get(search_req, search_resp);
    for (uint32_t i = 0; i < search_resp->result_stream.size(); i++) {
        const mdt::ResultStream& result = search_resp->result_stream[i];
        std::cout << "primary key: " << result.primary_key << std::endl;
        for (uint32_t j = 0; j < result.result_data_list.size(); j++) {
            std::cout << "        data: " << result.result_data_list[j] << std::endl;
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

int PutOp(std::vector<std::string>& cmd_vec) {
    // create db
    std::cout << "open db ..." << std::endl;
    mdt::Database* db;
    std::string db_name = cmd_vec[1];
    db = mdt::OpenDatabase(db_name);

    std::cout << "open table ..." << std::endl;
    mdt::Table* table;
    std::string table_name = cmd_vec[2];
    table = OpenTable(db, table_name);

    if (cmd_vec[3].compare("1") != 0) {
        std::cout << "value type not support, now support value=1\n";
        return 0;
    }

    // insert data
    mdt::StoreRequest* store_req = new mdt::StoreRequest();
    store_req->primary_key = cmd_vec[4];
    store_req->timestamp = time(NULL);

    int num_index = cmd_vec.size() - 6;
    if (num_index % 2 != 0) {
        std::cout << "put fail, [index_name index_type] not match!\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::Index index;
        index.index_name = cmd_vec[i + 6];
        index.index_key = cmd_vec[i + 7];
        store_req->index_list.push_back(index);
    }

    if (cmd_vec[3].compare("1") != 0) {
        std::string str(10240, 'y');
        store_req->data = str;
    }
    mdt::StoreResponse* store_resp = new mdt::StoreResponse();
    mdt::StoreCallback callback = StoreCallback_Test;

    bool store_finish = false;
    std::cout << "put ..." << std::endl;
    table->Put(store_req, store_resp, callback, &store_finish);
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
    if (cmd_vec[3].compare("kBytes") != 0) {
        std::cout << "create table fail, primary key type not support!\n";
        return 0;
    }
    table_desc.primary_key_type = mdt::kBytes;

    int num_index = cmd_vec.size() - 4;
    if (num_index % 2 != 0) {
        std::cout << "create table fail, [index_name index_type] not match!\n";
        return 0;
    }
    for (int i = 0; i < num_index; i += 2) {
        mdt::IndexDescription table_index;
        table_index.index_name = cmd_vec[i + 4];
        if (cmd_vec[i + 5].compare("kBytes") != 0) {
            std::cout << "create table fail, index key: " << cmd_vec[i + 4]
                << ", key type not support!\n";
            return 0;
        }
        table_index.index_key_type = mdt::kBytes;
        table_desc.index_descriptor_list.push_back(table_index);
    }
    CreateTable(db, table_desc);
    return 0;
}

int main(int ac, char* av[]) {
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
            free(line);
            continue;
        } else if (cmd_vec[0].compare("CreateTable") == 0 && cmd_vec.size() >= 4) {
            // cmd: CreateTable dbname tablename primary_key_type [index_name index_type]...
            std::cout << "create table: dbname " << cmd_vec[1]
                << "tablename " << cmd_vec[2]
                << "\n";
            CreateTableOp(cmd_vec);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("Put") == 0 && cmd_vec.size() >= 6) {
            // cmd: Put dbname tablename value primary_key timestamp [index_name index_key]
            PutOp(cmd_vec);
            free(line);
            continue;
        } else if (cmd_vec[0].compare("Get") == 0 && cmd_vec.size() >= 6) {
            // cmd: Get dbname tablename start_ts(ignore) end_ts(ignore) limit(ignore) [index_name cmp(=, >=, >, <=, <) index_key]
            GetOp(cmd_vec);
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
