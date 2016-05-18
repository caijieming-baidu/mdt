#ifndef AGENT_OPTIONS_H_
#define AGENT_OPTIONS_H_

#include <string>
#include <iostream>
#include "leveldb/db.h"
#include "utils/counter.h"

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

    ::leveldb::Env* kMemEnv;
    ::leveldb::DB* kMemDB;

    ::mdt::CounterMap* counter_map;
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

    // set by log agent
    int64_t nr_file_streams;
    int64_t history_fd_overflow_count;
    int64_t curr_pending_req;
};

struct FileStruct {
    std::string filename;
};

}
}
#endif
