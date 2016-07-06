// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sofa/pbrpc/pbrpc.h>

#include "collector/query_service.h"
#include "utils/status.h"

DECLARE_string(se_service_port);
DECLARE_string(flagfile);
DECLARE_string(log_dir);

DECLARE_int64(collector_rpc_work_thread);
DECLARE_int64(collector_rpc_nr_io_server);
DECLARE_int64(collector_rpc_max_pending_size);

void SetupLog(const std::string& name) {
    std::string program_name = "collector";
    if (name.empty()) {
        program_name = name;
    }

    if (FLAGS_log_dir.size() == 0) {
        if (access("../log", F_OK)) {
            FLAGS_log_dir = "../log";
        } else {
            FLAGS_log_dir = "./log";
        }
    }

    if (access(FLAGS_log_dir.c_str(), F_OK)) {
        mkdir(FLAGS_log_dir.c_str(), 0777);
    }

    std::string log_filename = FLAGS_log_dir + "/" + program_name + ".INFO.";
    std::string wf_filename = FLAGS_log_dir + "/" + program_name + ".WARNING.";
    google::SetLogDestination(google::INFO, log_filename.c_str());
    google::SetLogDestination(google::WARNING, wf_filename.c_str());
    google::SetLogDestination(google::ERROR, "");
    google::SetLogDestination(google::FATAL, "");

    google::SetLogSymlink(google::INFO, program_name.c_str());
    google::SetLogSymlink(google::WARNING, program_name.c_str());
    google::SetLogSymlink(google::ERROR, "");
    google::SetLogSymlink(google::FATAL, "");
}

static pthread_once_t glog_once = PTHREAD_ONCE_INIT;
static void InternalSetupGoogleLog() {
    // init param, setup log
    std::string log_prefix = "collector";
    ::google::InitGoogleLogging(log_prefix.c_str());
    SetupLog(log_prefix);
    tera::Client::SetGlogIsInitialized();
    LOG(INFO) << "start loging...";
}

void SetupGoogleLog() {
    pthread_once(&glog_once, InternalSetupGoogleLog);
}

class QueryEntry {
public:
    QueryEntry():se_(NULL), se_service_(NULL), rpc_server_(NULL) {}
    ~QueryEntry() {}
    int InitSearchEngine();
private:
    ::mdt::SearchEngineImpl* se_;
    ::mdt::SearchEngineServiceImpl* se_service_;
    sofa::pbrpc::RpcServer* rpc_server_;
};

int QueryEntry::InitSearchEngine() {
    se_ = new mdt::SearchEngineImpl();
    ::mdt::Status s = se_->InitSearchEngine();
    if (!s.ok()) {
        std::cout << "init se error, conf file not found\n";
        return -1;
    }

    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_options.work_thread_num = FLAGS_collector_rpc_work_thread;
    rpc_options.io_service_pool_size = FLAGS_collector_rpc_nr_io_server;
    rpc_options.max_throughput_in = -1;
    rpc_options.max_throughput_out = -1;
    rpc_options.max_pending_buffer_size = FLAGS_collector_rpc_max_pending_size;
    rpc_server_ = new sofa::pbrpc::RpcServer(rpc_options);

    std::string server_addr = "0.0.0.0:" + FLAGS_se_service_port;
    if (!rpc_server_->Start(server_addr)) {
        std::cout << "start rpc server ERROR\n";
        return -1;
    }

    se_service_ = new mdt::SearchEngineServiceImpl(se_);
    rpc_server_->RegisterService(se_service_);
    rpc_server_->Run();
    //rpc_server_->Stop();
    return 0;
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
    //SetupGoogleLog();
    QueryEntry entry;
    if (FLAGS_flagfile.size() == 0) {
        std::cout << "flagfile not set.\n";
        return -1;
    }
    if (access(FLAGS_flagfile.c_str(), F_OK) != 0) {
        std::cout << "flagfile not exist.\n";
        return -1;
    }
    if (entry.InitSearchEngine() < 0) {
        std::cout << "init query entry error\n";
        return -1;
    }
    return 0;
}

