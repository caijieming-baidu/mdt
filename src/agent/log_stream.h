// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef AGENT_LOG_STREAM_H_
#define AGENT_LOG_STREAM_H_

#include <sys/time.h>

#include <iostream>
#include <map>
#include <queue>
#include <vector>

#include "agent/options.h"
#include "leveldb/db.h"
#include "proto/agent.pb.h"
#include "proto/query.pb.h"
#include "proto/scheduler.pb.h"
#include "rpc/rpc_client.h"
#include "utils/counter.h"
#include "utils/event.h"
#include "utils/thread_pool.h"

namespace mdt {
namespace agent {

struct DBKey {
    //std::string module_name;
    std::string filename;
    uint64_t ino;
    uint64_t timestamp;
    uint64_t offset;
    Counter ref;
};

struct FileStreamProfile {
    uint64_t ino;
    std::string filename;
    uint64_t nr_pending;
    uint64_t current_offset;
};

class FileStream {
public:
    explicit FileStream(std::string module_name, LogOptions log_options,
                        std::string filename,
                        uint64_t ino,
                        int* success);
    bool InodeToFileName(uint64_t ino, const std::string& filename, std::string* newname);
    bool FindLostInode(uint64_t ino, const std::string& dir, std::string* newname);
    ~FileStream();
    std::string GetFileName() {
        return filename_;
    }
    void SetFileName(const std::string& filename) {
        filename_ = filename;
    }
    void GetRedoList(std::map<uint64_t, uint64_t>* redo_list);
    void ReSetFileStreamCheckPoint();
    int RecoveryCheckPoint(int64_t fsize);
    void GetCheckpoint(DBKey* key, uint64_t* offset, uint64_t* size);
    //ssize_t ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec);
    ssize_t ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec, bool read_half_line);
    int Read(std::vector<std::string>* line_vec, DBKey** key);
    int LogCheckPoint(uint64_t offset, uint64_t size);
    int DeleteCheckoutPoint(DBKey* key);
    int CheckPointRead(std::vector<std::string>* line_vec, DBKey** key,
                       uint64_t offset, uint64_t size);
    int HanleFailKey(DBKey* key);
    int MarkDelete();
    int OpenFile();
    void Profile(FileStreamProfile* profile);

private:
    void EncodeUint64BigEndian(uint64_t value, std::string* str);
    void MakeKeyValue(const std::string& module_name,
                      const std::string& filename,
                      uint64_t ino,
                      uint64_t offset,
                      std::string* key,
                      uint64_t size,
                      std::string* value);
    void ParseKeyValue(const leveldb::Slice& key,
                       const leveldb::Slice& value,
                       uint64_t* ino,
                       uint64_t* offset, uint64_t* size);

    void MakeCurrentOffsetKey(const std::string& module_name,
                              const std::string& filename,
                              uint64_t ino,
                              uint64_t offset,
                              std::string* key,
                              std::string* value);

    void MakeContextKey(const std::string& db_name, uint64_t ino, std::string* key,
                        uint64_t offset, uint64_t size, char* buf, std::string* val);
    bool ParseContextKey(std::string* db_name, uint64_t* ino, std::string* key,
                         uint64_t* offset, uint64_t* size, std::string* buf, std::string* val);
public:
    // profile info
    Counter kreq_success;
    Counter kreq_fail;
    Counter kfile_read_success;
    Counter kfile_read_fail;
    Counter kleveldb_put_success;
    Counter kleveldb_put_fail;
    Counter kleveldb_delete_success;
    Counter kleveldb_delete_fail;
    Counter kleveldb_reopen_fail;
    Counter kcheckpoint_read_num;

private:
    std::string module_name_;
    std::string filename_; // abs path
    uint64_t ino_;
    LogOptions log_options_;
    int fd_;
    // current send point
    uint64_t current_offset_;

    // wait commit list
    pthread_spinlock_t lock_;
    std::map<uint64_t, uint64_t> mem_checkpoint_list_; // <offset, size>
    std::map<uint64_t, uint64_t> redo_list_; // <offset, size>, use leveldb to recovery start point
};

class LogStream {
public:
    LogStream(std::string module_name, LogOptions log_options,
              RpcClient* rpc_client, pthread_spinlock_t* server_addr_lock,
              AgentInfo* info);
    ~LogStream();

    int AddTableName(const std::string& log_name);
    int AddWriteEvent(std::string filename);
    int AddWriteEvent(std::string filename, uint64_t ino);
    int DeleteWatchEvent(std::string filename, bool need_wakeup);
    int DeleteWatchEvent(std::string filename, uint64_t ino, bool need_wakeup);
    void Run();

    // support monitor
    int AddMonitor(const mdt::LogAgentService::RpcMonitorRequest* request);
    int UpdateIndex(const mdt::LogAgentService::RpcUpdateIndexRequest* request);

private:
    bool CheckReadable(FileStream* file_stream);
    void EncodeUint64BigEndian(uint64_t value, std::string* str);
    void DumpWriteEvent(const std::string& filename, uint64_t ino);
    void EraseWriteEvent(const std::string& filename, uint64_t ino, std::string* key);
    void RecoverWriteEvent(std::vector<std::pair<std::string, uint64_t> >* event_vec);

    int CollectorMeta(const mdt::LogAgentService::LogMeta& meta,
                     std::map<std::string, std::string>* kv);
    bool CheckTimeStampValid(const std::string& time_str);
    int SearchIndex(const std::string& line, const std::string& table_name,
                    mdt::SearchEngine::RpcStoreRequest* req);
    int InternalSearchIndex(const std::string& line,
                            const mdt::LogAgentService::Rule& rule,
                            std::map<std::string, std::string>* kv);

    void GetTableName(std::string file_name, std::string* table_name);
    uint64_t ParseTime(const std::string& time_str);
    std::string TimeToStringWithTid(struct timeval* filetime);
    std::string GetUUID();
    int ParseMdtRequest(const std::string table_name,
                        std::vector<std::string>& line_vec,
                        std::vector<mdt::SearchEngine::RpcStoreRequest* >* req_vec,
                        std::vector<std::string>* monitor_vec);
    void ApplyRedoList(FileStream* file_stream);
    int AsyncPush(std::vector<mdt::SearchEngine::RpcStoreRequest*>& req_vec, DBKey* key);
    void AsyncPushCallback(const mdt::SearchEngine::RpcStoreRequest* req,
                           mdt::SearchEngine::RpcStoreResponse* resp,
                           bool failed, int error,
                           mdt::SearchEngine::SearchEngineService_Stub* service,
                           DBKey* key);
    void HandleDelayFailTask(DBKey* key);

    // support monitor
    void AsyncPushMonitorCallback(const mdt::LogSchedulerService::RpcMonitorStreamRequest* req,
                                  mdt::LogSchedulerService::RpcMonitorStreamResponse* resp,
                                  bool failed, int error,
                                  mdt::LogSchedulerService::LogSchedulerService_Stub* service);
    int AsyncPushMonitor(const std::string& table_name, const std::vector<std::string>& monitor_vec);
    bool MonitorHasEvent(const std::string& table_name, const std::string& line);
    bool CheckJson(const std::string& line, const mdt::LogAgentService::Rule& rule);
    bool CheckRegex(const std::string& line, const mdt::LogAgentService::Rule& rule);
    bool CheckRecord(const std::string& key, const mdt::LogAgentService::Record& record);

    void StreamMakeContextKey(const std::string& db_name, uint64_t ino, std::string* key,
                              uint64_t offset, uint64_t size, char* buf, std::string* val);
    void DeleteMagicAndOffset(uint64_t ino, const std::string& filename);
    int64_t GetCurrentOffset(uint64_t ino, const std::string& filename);
    void StreamMakeCurrentOffsetKey(const std::string& module_name,
                              const std::string& filename,
                              uint64_t ino,
                              uint64_t offset,
                              std::string* key,
                              std::string* value);
    void StreamMakeKeyValue(const std::string& module_name,
                      const std::string& filename,
                      uint64_t ino,
                      uint64_t offset,
                      std::string* key,
                      uint64_t size,
                      std::string* value);
    void StreamParseKeyValue(const leveldb::Slice& key,
                       const leveldb::Slice& value,
                       uint64_t* ino,
                       uint64_t* offset, uint64_t* size);
    void ReclaimOrphanInode(const std::string& db_name);
    bool StreamInodeToFileName(uint64_t ino, const std::string& filename, std::string* newname);
    bool StreamFindLostInode(uint64_t ino, const std::string& dir, std::string* newname);
    int AddCtrlEvent(uint64_t event_id);

public:
    Counter kseq_send_num;
    Counter kseq_nonsend_num;
    Counter kseq_send_success;
    Counter kseq_send_fail;
    Counter kindex_filter_num;
    Counter kkeyword_filter_num;
    Counter kfile_miss_num;

private:
    std::string hostname_;

    std::string module_name_;
    std::set<std::string> log_name_prefix_; // use for table name

    // all modules use the same db
    LogOptions log_options_;
    // rpc data send
    RpcClient* rpc_client_;
    pthread_spinlock_t* server_addr_lock_;
    AgentInfo* info_;
    //std::string* server_addr_;

    std::string db_name_; // DISCARD: useless
    std::string table_name_; // DISCARD: useless

    // line filter
    std::vector<std::string> string_filter_;

    // log line parse relatively
    std::vector<std::string> string_delims_; // special split line use string
    std::string line_delims_; // general split line into items list
    // kv parse method 1: self parse
    std::string kv_delims_; // parse kv from item
    bool enable_index_filter_; // use index list filter log line, default value = false
    std::set<std::string> index_list_;
    std::map<std::string, std::string> alias_index_map_;
    // kv parse method 2: fixed parse
    bool use_fixed_index_list_;
    std::map<std::string, int> fixed_index_list_;
    // every log item has unique key
    std::string primary_key_;
    // use for time parse
    std::string user_time_;
    // type = 1: for second+micro-second
    int time_type_;

    // use for thead wait
    pthread_t tid_;
    volatile bool stop_;
    AutoResetEvent thread_event_;

    //std::map<std::string, FileStream*> file_streams_;
    std::map<uint64_t, FileStream*> file_streams_; // ino, filestream map
    uint32_t total_stream_offset_;
    // all event queue
    pthread_spinlock_t lock_;
    //std::set<std::string> delete_event_;
    //std::set<std::string> write_event_;
    std::map<uint64_t, std::string> delete_event_;
    std::map<uint64_t, std::string> write_event_; // [inode, filename]
    std::queue<DBKey*> key_queue_;
    std::queue<DBKey*> failed_key_queue_;
    std::map<uint64_t, uint64_t> ctrl_event_;
    ThreadPool fail_delay_thread_;

    // collector info
    int64_t last_update_time_;
    int64_t last_ino_check_ts_;

    // monitor relatively
    pthread_spinlock_t monitor_lock_;
    // <table name, monitor>
    std::map<std::string, mdt::LogAgentService::RpcMonitorRequest> monitor_handler_set_;
    std::map<std::string, mdt::LogAgentService::RpcUpdateIndexRequest> index_set_;
};

}
}

#endif
