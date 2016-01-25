#ifndef AGENT_LOG_STREAM_H_
#define AGENT_LOG_STREAM_H_

#include <iostream>
#include <map>
#include <queue>
#include <vector>
#include "leveldb/db.h"
#include "agent/options.h"
#include "proto/query.pb.h"
#include "rpc/rpc_client.h"
#include "util/event.h"
#include <sys/time.h>
#include "util/counter.h"
#include "util/thread_pool.h"

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

class FileStream {
public:
    explicit FileStream(std::string module_name, LogOptions log_options,
                        std::string filename,
                        uint64_t ino,
                        int* success);
    ~FileStream();
    std::string GetFileName() {
        return filename_;
    }
    void SetFileName(const std::string& filename) {
        filename_ = filename;
    }
    void GetRedoList(std::map<uint64_t, uint64_t>* redo_list);
    void ReSetFileStreamCheckPoint();
    int RecoveryCheckPoint();
    void GetCheckpoint(DBKey* key, uint64_t* offset, uint64_t* size);
    ssize_t ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec);
    int Read(std::vector<std::string>* line_vec, DBKey** key);
    int LogCheckPoint(uint64_t offset, uint64_t size);
    int DeleteCheckoutPoint(DBKey* key);
    int CheckPointRead(std::vector<std::string>* line_vec, DBKey** key,
                       uint64_t offset, uint64_t size);
    int HanleFailKey(DBKey* key);
    int MarkDelete();

    int MatchAndSetConfigure(const std::string& log_name, LineHandlerConfigure* conf);

private:
    void EncodeUint64BigEndian(uint64_t value, std::string* str);
    void MakeKeyValue(const std::string& module_name,
                      const std::string& filename,
                      uint64_t offset,
                      std::string* key,
                      uint64_t size,
                      std::string* value);
    void ParseKeyValue(const leveldb::Slice& key,
                       const leveldb::Slice& value,
                       uint64_t* offset, uint64_t* size);

private:
    std::string module_name_;
    std::string filename_; // abs path
    uint64_t ino_;

    pthread_spinlock_t conf_lock_;
    std::map<int64_t, LineHandlerConfigure*> line_parser_conf_; // each line has many parser rules

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

    int AddWriteEvent(std::string filename);
    int DeleteWatchEvent(std::string filename, bool need_wakeup);
    void Run();

private:
    uint64_t ParseTime(int time_type, const std::string& time_str);
    std::string TimeToString(struct timeval* filetime);
    int ParseMdtRequest(std::vector<std::string>& line_vec,
                        std::vector<mdt::SearchEngine::RpcStoreRequest* >* req_vec);
    void ApplyRedoList(FileStream* file_stream);
    int AsyncPush(std::vector<mdt::SearchEngine::RpcStoreRequest*>& req_vec, DBKey* key);
    void AsyncPushCallback(const mdt::SearchEngine::RpcStoreRequest* req,
                           mdt::SearchEngine::RpcStoreResponse* resp,
                           bool failed, int error,
                           mdt::SearchEngine::SearchEngineService_Stub* service,
                           DBKey* key);
    void HandleDelayFailTask(DBKey* key);

private:
    std::string module_name_;
    // all modules use the same db
    LogOptions log_options_;
    // rpc data send
    RpcClient* rpc_client_;
    pthread_spinlock_t* server_addr_lock_;
    AgentInfo* info_;
    //std::string* server_addr_;

    std::string db_name_;
    std::string table_name_;
    /*
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
    */
    std::map<int64_t, LineHandlerConfigure*> default_line_parser_;

    // use for thead wait
    pthread_t tid_;
    volatile bool stop_;
    AutoResetEvent thread_event_;

    //std::map<std::string, FileStream*> file_streams_;
    std::map<uint64_t, FileStream*> file_streams_; // ino, filestream map
    // all event queue
    pthread_spinlock_t lock_;
    //std::set<std::string> delete_event_;
    //std::set<std::string> write_event_;
    std::map<uint64_t, std::string> delete_event_;
    std::map<uint64_t, std::string> write_event_; // [inode, filename]
    std::queue<DBKey*> key_queue_;
    std::queue<DBKey*> failed_key_queue_;
    ThreadPool fail_delay_thread_;
    std::vector<std::pair<std::string, LineHandlerConfigure*> > conf_event_;
};

}
}

#endif
