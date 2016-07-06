// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef AGENT_AGENT_IMPL_H_
#define AGENT_AGENT_IMPL_H_

#include <pthread.h>
#include <stdio.h>
#include <sys/inotify.h>
#include <unistd.h>

#include <iostream>
#include <map>

#include <sofa/pbrpc/pbrpc.h>

#include "agent/log_stream.h"
#include "proto/agent.pb.h"
#include "proto/scheduler.pb.h"
#include "utils/event.h"

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

    void RpcStoreSpan(::google::protobuf::RpcController* controller,
                      const mdt::LogAgentService::RpcStoreSpanRequest* request,
                      mdt::LogAgentService::RpcStoreSpanResponse* response,
                      ::google::protobuf::Closure* done);

    void RpcTraceGalaxyApp(::google::protobuf::RpcController* controller,
                           const mdt::LogAgentService::RpcTraceGalaxyAppRequest* request,
                           mdt::LogAgentService::RpcTraceGalaxyAppResponse* response,
                           ::google::protobuf::Closure* done);

    void RpcMonitor(::google::protobuf::RpcController* controller,
                    const mdt::LogAgentService::RpcMonitorRequest* request,
                    mdt::LogAgentService::RpcMonitorResponse* response,
                    ::google::protobuf::Closure* done);

    void RpcUpdateIndex(::google::protobuf::RpcController* controller,
                    const mdt::LogAgentService::RpcUpdateIndexRequest* request,
                    mdt::LogAgentService::RpcUpdateIndexResponse* response,
                    ::google::protobuf::Closure* done);

private:
    void InitMemDB(LogOptions* opt);
    void ParseLogDir(std::vector<std::string>& log_vec);
    void ParseModuleName(const std::string& filename, std::string* module_name);
    int FilterFileByMoudle(const std::string& filename, std::string* expect_module_name);
    int AddWatchModuleStream(const std::string& module_name, const std::string& log_name);
    void AddOldFile(const std::string& filename);

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

    // add monitor
    int AddMonitor(const mdt::LogAgentService::RpcMonitorRequest* request);

    int UpdateIndex(const mdt::LogAgentService::RpcUpdateIndexRequest* request);

private:
    std::string hostname_;

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
