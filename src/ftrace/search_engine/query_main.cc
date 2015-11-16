#include <gflags/gflags.h>
#include "ftrace/search_engine/query_service.h"

DECLARE_string(se_service_port);

class QueryEntry {
public:
    QueryEntry():se_(NULL), se_service_(NULL), rpc_server_(NULL) {}
    ~QueryEntry() {}
    int InitSearchEngine(const std::string& conf);
private:
    ::mdt::SearchEngineImpl* se_;
    ::mdt::SearchEngineServiceImpl* se_service_;
    sofa::pbrpc::RpcServer* rpc_server_;
}

int QueryEntry::InitSearchEngine() {
    se_ = new mdt::SearchEngineImpl();
    if (se_.InitSearchEngine()) {
        std::cout << "init se error, conf file not found\n";
        return -1;
    }

    sofa::pbrpc::RpcServerOptions rpc_options;
    rpc_options.max_throughput_in = -1;
    rpc_options.min_throughput_out = -1;
    rpc_server_ = new sofa::pbrpc::RpcServer(rpc_options);

    std::string server_addr = "0.0.0.0" + ":" + FLAGS_se_service_port;
    se_service_ = new mdt::SearchEngineServiceImpl(se_);
    rpc_server_->RegisterService(se_service_);
    if (!rpc_server_->Start(server_addr)) {
        std::cout << "start rpc server ERROR\n";
        return -1;
    }
    return 0;
}

int main(int ac, char* av[]) {
    ::google::ParseCommandLineFlags(&ac, &av, true);
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

