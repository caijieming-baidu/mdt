#ifndef MDT_SRC_FTRACE_SEARCH_ENGINE_H_
#define MDT_SRC_FTRACE_SEARCH_ENGINE_H_

namespace mdt {

class SearchEngineImpl {
public:
    SearchEngineImpl();
    ~SearchEngineImpl();
    Status InitSearchEngine();
    Status OpenDatabase(const std::string& db_name);
    Status OpenTable(const std::string& db_name, const std::string& table_name);
    Status Search(cont RpcSearchRequest* req, RpcSearchResponse* resp);
private:
    ::mdt::Table* GetTable(const std::string& db_name, const std::string& table_name);

private:
    Mutex mu_;
    std::map<std::string, ::mdt::Database*> db_map_;
    std::map<std::string, ::mdt::Table*> table_map_;
};

class SearchEngineServiceImpl : public SearchEngineService {
public:
    explicit SearchEngineServiceImpl(SearchEngineImpl* se);
    ~SearchEngineServiceImpl();
    int StartServer();
    int StopServer();

    void Search(::google::protobuf::RpcController* ctrl,
                     const RpcSearchRequest* req,
                     RpcSearchResponse* resp,
                     ::google::protobuf::Closure* done);
    void OpenTable(::google::protobuf::RpcController* ctrl,
                   const RpcOpenTableRequest* req,
                   RpcOpenTableResponse* resp,
                   ::google::protobuf::Closure* done);
    void OpenDatabase(::google::protobuf::RpcController* ctrl,
                   const RpcOpenDatabaseRequest* req,
                   RpcOpenDatabaseResponse* resp,
                   ::google::protobuf::Closure* done);

private:
    SearchEngineImpl* se_;
    ThreadPool* se_thread_pool_; // operate on se's method
};

}
#endif

