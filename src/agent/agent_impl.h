#ifndef AGENT_AGENT_IMPL_H_
#define AGENT_AGENT_IMPL_H_

#include <iostream>
#include <map>
#include <pthread.h>
#include <unistd.h>
#include <sys/inotify.h>
#include <sofa/pbrpc/pbrpc.h>
#include "proto/agent.pb.h"
#include "proto/scheduler.pb.h"
#include "agent/log_stream.h"
#include "util/event.h"
#include <stdio.h>

namespace mdt {
namespace agent {

class AgentImpl;
struct FileSystemInotify {
    std::string log_dir;
    int watch_fd;
    int inotify_fd;
    FILE* inotify_FD;
    int inotify_flag;
    pthread_t tid;
    volatile bool stop;
    AgentImpl* agent;

    FileSystemInotify()
        : watch_fd(-1),
        inotify_fd(-1),
        inotify_FD(NULL),
        inotify_flag(-1),
        stop(true),
        agent(NULL) {}
};

class AgentImpl : public ::mdt::LogAgentService::LogAgentService {
public:
    AgentImpl();
    ~AgentImpl();
    int Init();
    void GetServerAddr();
    void WatchLogDir(FileSystemInotify* fs_inotify);

    // rpc service
    void Echo(::google::protobuf::RpcController* controller,
         const mdt::LogAgentService::EchoRequest* request,
         mdt::LogAgentService::EchoResponse* response,
         ::google::protobuf::Closure* done);

    void RpcAddWatchPath(::google::protobuf::RpcController* controller,
                         const mdt::LogAgentService::RpcAddWatchPathRequest* request,
                         mdt::LogAgentService::RpcAddWatchPathResponse* response,
                         ::google::protobuf::Closure* done);

    void RpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                 const mdt::LogAgentService::RpcAddWatchModuleStreamRequest* request,
                                 mdt::LogAgentService::RpcAddWatchModuleStreamResponse* response,
                                 ::google::protobuf::Closure* done);

    void RpcAddLineParser(::google::protobuf::RpcController* controller,
                                 const mdt::LogAgentService::RpcAddLineParserRequest* request,
                                 mdt::LogAgentService::RpcAddLineParserResponse* response,
                                 ::google::protobuf::Closure* done);

private:
    int AddLineParser(const mdt::LogAgentService::RpcAddLineParserRequest* request);
    int TranslateLineParser(const mdt::LogAgentService::RpcLineParserConfigure& conf,
                        LineHandlerConfigure* line_parser);

    void ParseLogDir(std::vector<std::string>& log_vec);
    void ParseModuleName(const std::string& filename, std::string* module_name);
    int FilterFileByMoudle(const std::string& filename, std::string* expect_module_name);
    LogStream* AddWatchModuleStream(const std::string& module_name, const std::string& log_name);

    // watch event
    void DestroyWatchPath(FileSystemInotify* fs_inotify);
    int AddWatchPath(const std::string& dir);
    int AddWriteEvent(const std::string& logdir, const std::string& filename, inotify_event* event);
    int DeleteWatchEvent(const std::string& logdir, const std::string& filename, inotify_event* event);
    int WaitInotifyFDReadable(int fd);
    int FreadEvent(void* dest, size_t size, FILE* file);

    // cluster find
    void GetServerAddrCallback(const mdt::LogSchedulerService::GetNodeListRequest* req,
                               mdt::LogSchedulerService::GetNodeListResponse* resp,
                               bool failed, int error,
                               mdt::LogSchedulerService::LogSchedulerService_Stub* service);

private:
    pthread_spinlock_t lock_;
    std::map<std::string, FileSystemInotify*> inotify_; // log dir notify
    std::map<std::string, LogStream*> log_streams_; // each module has its log stream
    std::map<std::string, std::string> module_file_set_; // use for filter useless log file

    // all modules use the same db
    LogOptions log_options_;

    // agent rpc service
    RpcClient* rpc_client_;
    pthread_t scheduler_tid_;
    volatile bool stop_scheduler_thread_;
    AutoResetEvent server_addr_event_;
    pthread_spinlock_t server_lock_;
    AgentInfo info_;
    //std::string server_addr_;
};

}
}
#endif
