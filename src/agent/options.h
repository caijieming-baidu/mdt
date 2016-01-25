#ifndef AGENT_OPTIONS_H_
#define AGENT_OPTIONS_H_

#include <stdlib.h>
#include <set>
#include <map>
#include <string>
#include <iostream>
#include "leveldb/db.h"

namespace mdt {
namespace agent {

struct EventMask {
    unsigned int flag;
    const char* name;
};

enum DBTYPE {
    DISKDB = 1,
    MEMDB = 2,
};
struct LogOptions {
    DBTYPE db_type;
    std::string db_dir;
    leveldb::DB* db;
};

struct AgentInfo {
    int64_t qps_use;
    int64_t qps_quota;
    int64_t bandwidth_use;
    int64_t bandwidth_quota;

    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t error_nr;
    std::string collector_addr;
};

// log schema define here
struct LineHandlerConfigure {
    int64_t configure_id;

    // log line parse relatively
    std::vector<std::string> string_delims; // special split line use string
    std::string line_delims; // general split line into items list

    // every log item has unique key
    std::string primary_key;
    // use for time parse
    std::string user_time;
    // type = 1: for second+micro-second
    int time_type;

    int parser_type;
    // kv parse method 1: self parse
    std::string kv_delims; // parse kv from item
    std::set<std::string> index_list;
    std::map<std::string, std::string> alias_index_map;
    // kv parse method 2: fixed parse
    std::map<std::string, int> fixed_index_list;
};

}
}
#endif
