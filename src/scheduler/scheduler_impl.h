// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef SCHEDULER_SCHEDULER_IMPL_H_
#define SCHEDULER_SCHEDULER_IMPL_H_

#include <pthread.h>

#include <map>

#include <boost/shared_ptr.hpp>
#include <galaxy.h>
#include <google/protobuf/service.h>
#include <sofa/pbrpc/pbrpc.h>

#include "leveldb/db.h"
#include "mail/mail.h"
#include "proto/agent.pb.h"
#include "proto/scheduler.pb.h"
#include "rpc/rpc_client.h"
#include "utils/counter.h"
#include "utils/thread_pool.h"

namespace mdt {
namespace scheduler {

enum AgentState {
    AGENT_ACTIVE = 1,
    AGENT_INACTIVE = 2,
};

struct AgentInfo {
    //std::string agent_addr;
    // report by agent
    int64_t qps_use;
    int64_t qps_quota;
    int64_t bandwidth_use;
    int64_t bandwidth_quota;

    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t error_nr;

    // manage by scheduler
    int64_t ctime;
    std::string collector_addr;
    AgentState state;
    Counter counter;

    // set by log agent
    int64_t nr_file_streams;
    int64_t history_fd_overflow_count;
    int64_t curr_pending_req;
};

enum CollectorState {
    COLLECTOR_ACTIVE = 1,
    COLLECTOR_INACTIVE = 2,
};

struct CollectorInfo {
    //std::string collector_addr;
    // info report by collector
    int64_t qps;
    int64_t max_packet_size;
    int64_t min_packet_size;
    int64_t average_packet_size;

    int64_t store_pending;
    int64_t store_sched_ts;
    int64_t store_task_ts;
    int64_t store_task_num;

    // state info manage by scheduler
    int64_t nr_agents;
    int64_t ctime;
    int64_t error_nr;
    CollectorState state; // 1 = active, 2 = inactive
};

enum TraceState {
    ENABLE_TRACE = 1,
    DISABLE_TRACE = 2,
};

struct TraceInfo {
    std::string job_name;
    std::string job_id;
    ::baidu::galaxy::Galaxy* galaxy;
    uint64_t flag;
    Counter ref;
    mdt::LogSchedulerService::RpcTraceGalaxyAppRequest configure;

    TraceInfo() : galaxy(NULL) {}
    ~TraceInfo() { }
};

class SchedulerImpl : public mdt::LogSchedulerService::LogSchedulerService {
public:
    SchedulerImpl();
    ~SchedulerImpl();

    // rpc service
    void Echo(::google::protobuf::RpcController* controller,
         const mdt::LogSchedulerService::EchoRequest* request,
         mdt::LogSchedulerService::EchoResponse* response,
         ::google::protobuf::Closure* done);

    void RegisterNode(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RegisterNodeRequest* request,
                 mdt::LogSchedulerService::RegisterNodeResponse* response,
                 ::google::protobuf::Closure* done);
    void BgHandleCollectorInfo();
    void BgHandleAgentInfo();

    void GetNodeList(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::GetNodeListRequest* request,
                 mdt::LogSchedulerService::GetNodeListResponse* response,
                 ::google::protobuf::Closure* done);

    void RpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done);

    void RpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                 const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                                 mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                                 ::google::protobuf::Closure* done);
    void RpcShowAgentInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                          ::google::protobuf::Closure* done);

    void RpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done);
    void RpcTraceGalaxyApp(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcTraceGalaxyAppRequest* request,
                          mdt::LogSchedulerService::RpcTraceGalaxyAppResponse* response,
                          ::google::protobuf::Closure* done);

    void RpcShowCounter(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCounterRequest* request,
                          mdt::LogSchedulerService::RpcShowCounterResponse* response,
                          ::google::protobuf::Closure* done);

    // suport monitor
    void RpcMonitor(::google::protobuf::RpcController* controller,
                    const mdt::LogSchedulerService::RpcMonitorRequest* request,
                    mdt::LogSchedulerService::RpcMonitorResponse* response,
                    ::google::protobuf::Closure* done);
    void RpcMonitorStream(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcMonitorStreamRequest* request,
                          mdt::LogSchedulerService::RpcMonitorStreamResponse* response,
                          ::google::protobuf::Closure* done);

    void RpcUpdateIndex(::google::protobuf::RpcController* controller,
                       const mdt::LogSchedulerService::RpcUpdateIndexRequest* request,
                       mdt::LogSchedulerService::RpcUpdateIndexResponse* response,
                       ::google::protobuf::Closure* done);

    void RpcUpdateConfigure(::google::protobuf::RpcController* controller,
                       const mdt::LogSchedulerService::RpcUpdateConfigureRequest* request,
                       mdt::LogSchedulerService::RpcUpdateConfigureResponse* response,
                       ::google::protobuf::Closure* done);

private:
    void AsyncTraceGalaxyAppCallback(const mdt::LogAgentService::RpcTraceGalaxyAppRequest* req,
                mdt::LogAgentService::RpcTraceGalaxyAppResponse* resp,
                bool failed, int error,
                mdt::LogAgentService::LogAgentService_Stub* service,
                boost::shared_ptr<TraceInfo> trace_info);
    void DoRpcTraceGalaxyApp(boost::shared_ptr<TraceInfo> trace_info);

    void DoRegisterNode(::google::protobuf::RpcController* controller,
                                       const mdt::LogSchedulerService::RegisterNodeRequest* request,
                                       mdt::LogSchedulerService::RegisterNodeResponse* response,
                                       ::google::protobuf::Closure* done);

    void DoRpcShowCounter(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCounterRequest* request,
                          mdt::LogSchedulerService::RpcShowCounterResponse* response,
                          ::google::protobuf::Closure* done);

    void DoUpdateAgentInfo(::google::protobuf::RpcController* controller,
                           const mdt::LogSchedulerService::GetNodeListRequest* request,
                           mdt::LogSchedulerService::GetNodeListResponse* response,
                           ::google::protobuf::Closure* done);
    void SelectAndUpdateCollector(AgentInfo info, std::string* select_server_addr);

    void RepeatedAddAgentWatchPath(std::string agent_addr);
    void DoRpcAddAgentWatchPath(::google::protobuf::RpcController* controller,
                 const mdt::LogSchedulerService::RpcAddAgentWatchPathRequest* request,
                 mdt::LogSchedulerService::RpcAddAgentWatchPathResponse* response,
                 ::google::protobuf::Closure* done);

    void RepeatedAddAgentWatchModuleStream(std::string agent_addr);
    void DoRpcAddWatchModuleStream(::google::protobuf::RpcController* controller,
                                   const mdt::LogSchedulerService::RpcAddWatchModuleStreamRequest* request,
                                   mdt::LogSchedulerService::RpcAddWatchModuleStreamResponse* response,
                                   ::google::protobuf::Closure* done);

    void DoRpcShowAgentInfo(::google::protobuf::RpcController* controller,
                      const mdt::LogSchedulerService::RpcShowAgentInfoRequest* request,
                      mdt::LogSchedulerService::RpcShowAgentInfoResponse* response,
                      ::google::protobuf::Closure* done);

    void DoRpcShowCollectorInfo(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcShowCollectorInfoRequest* request,
                          mdt::LogSchedulerService::RpcShowCollectorInfoResponse* response,
                          ::google::protobuf::Closure* done);

    // support monitor
    void GetMonitorName(const std::string& db_name, const std::string& table_name, std::string* monitor_name);
    void PackMail(const std::string& to, const mdt::LogSchedulerService::RpcMonitorStreamRequest* request);
    void DelaySendMail(std::string to);
    void InternalSendMail(const std::string& to, std::vector<mdt::LogSchedulerService::RpcMonitorStreamRequest>& local_queue);
    void RepeatedMonitor(std::string name);
    void DoRpcMonitorStream(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcMonitorStreamRequest* request,
                          mdt::LogSchedulerService::RpcMonitorStreamResponse* response,
                          ::google::protobuf::Closure* done);

    void AsyncPushMonitorCallback(const mdt::LogAgentService::RpcMonitorRequest* req,
                                  mdt::LogAgentService::RpcMonitorResponse* resp,
                                  bool failed, int error,
                                  mdt::LogAgentService::LogAgentService_Stub* service);
    void CopyRule(const mdt::LogSchedulerService::Rule& r2, mdt::LogAgentService::Rule* r);
    void TranslateMonitorRequest(const mdt::LogSchedulerService::RpcMonitorRequest* request,
                                 mdt::LogAgentService::RpcMonitorRequest* req);
    void DoRpcMonitor(::google::protobuf::RpcController* controller,
                    const mdt::LogSchedulerService::RpcMonitorRequest* request,
                    mdt::LogSchedulerService::RpcMonitorResponse* response,
                    ::google::protobuf::Closure* done);

    // support index push down
    void TranslateUpdateIndexRequest(const mdt::LogSchedulerService::RpcUpdateIndexRequest* request,
                                    mdt::LogAgentService::RpcUpdateIndexRequest* req);
    void AsyncUpdateIndexCallback(const mdt::LogAgentService::RpcUpdateIndexRequest* req,
                                  mdt::LogAgentService::RpcUpdateIndexResponse* resp,
                                  bool failed, int error,
                                  mdt::LogAgentService::LogAgentService_Stub* service);
    void GetIndexConfigureName(const std::string& db_name, const std::string& table_name, std::string* dest_name);
    void ParseIndexConfigureName(const std::string& name, std::string* db_name, std::string* table_name);
    void RepeatedUpdateIndex(std::string name);
    void DoRpcUpdateIndex(::google::protobuf::RpcController* controller,
                          const mdt::LogSchedulerService::RpcUpdateIndexRequest* request,
                          mdt::LogSchedulerService::RpcUpdateIndexResponse* response,
                          ::google::protobuf::Closure* done);

    void DoRpcUpdateConfigure(::google::protobuf::RpcController* controller,
                       const mdt::LogSchedulerService::RpcUpdateConfigureRequest* request,
                       mdt::LogSchedulerService::RpcUpdateConfigureResponse* response,
                       ::google::protobuf::Closure* done);

private:
    // cluster manager:
    // db.Put(SYS.DB.ktrace, "");                                  => (Set, Get, Del, Add)DB
    //
    // db.Put(SYS.ktrace.Table.trace, "");                         => (Set, Get, Del, Add)Table
    //
    // db.Put(SYS.ktrace.trace.Module.ac, "");                     => (Set, Get, Del, Add)Module
    //
    // db.Put(SYS.ktrace.trace.ac.Index, index);                => (Set, Get, Del, Add)Index
    // db.Put(SYS.ktrace.trace.ac.Monitor, monitor);            => (Set, Get, Del, Add)SetMonitor
    // db.Put(SYS.ktrace.trace.ac.Dir.dir, "");                    => (Set, Get, Del, Add)SetDir
    // db.Put(SYS.ktrace.trace.ac.File.file, "");                  => (Set, Get, Del, Add)SetFile
    // db.Put(SYS.ktrace.trace.ac.Hostname.hostname, "");          => (Set, Get, Del, Add)SetHostname
    std::string db_dir_;
    pthread_spinlock_t db_lock_;
    leveldb::DB* disk_db_;

    ::mdt::Mutex counter_scan_lock_;
    ::mdt::CounterMap counter_map_;

    RpcClient* rpc_client_;

    ThreadPool agent_thread;
    pthread_spinlock_t agent_lock_;
    std::map<std::string, AgentInfo> agent_map_;
    ThreadPool agent_thread_;

    ThreadPool collector_thread_;
    pthread_spinlock_t collector_lock_;
    std::map<std::string, CollectorInfo> collector_map_;

    ThreadPool ctrl_thread_;

    pthread_t collector_tid_;
    volatile bool collector_thread_stop_;

    pthread_t agent_tid_;
    volatile bool agent_thread_stop_;



    // use for galaxy configure app trace path
    ThreadPool galaxy_trace_pool_;
    pthread_spinlock_t galaxy_trace_lock_;
    std::map<std::string, boost::shared_ptr<TraceInfo> > galaxy_trace_rule_;
    // <ip, path_vec, db#table_vec> map
    std::map<std::string, std::map<std::string, int> > path_map_;
    std::map<std::string, std::map<std::string, int> > db_map_;

    // use for monitor send mail
    pthread_spinlock_t monitor_lock_;
    // <db_name.table_name, monitor>
    std::map<std::string, mdt::LogSchedulerService::RpcMonitorRequest> monitor_handler_set_;
    std::map<std::string, mdt::LogSchedulerService::RpcUpdateIndexRequest> index_set_;

    ThreadPool monitor_thread_;
    Mail mail_;
    std::map<std::string, std::vector<mdt::LogSchedulerService::RpcMonitorStreamRequest> > mail_queue_;
};

}
}

#endif
