#include <gflags/gflags.h>
#include "ftrace/search_engine/query_service.h"

DECLARE_int32(se_num_threads);
DECLARE_bool(mdt_flagfile_set);

namespace mdt {

// operation
SearchEngineImpl::SearchEngineImpl() {}
SearchEngineImpl::~SearchEngineImpl() {}

// init mdt.flag
Status SearchEngineImpl::InitSearchEngine() {
    if (!FLAGS_mdt_flagfile_set) {
        return Status::NotFound("not mdt.flag");
    }
    if (access(FLAGS_mdt_flagfile.c_str(), F_OK) != 0) {
        return Status::NotFound("not mdt.flag");
    }
    int ac = 1;
    char** av = new char*[2];
    av[0] = (char*)"dummy";
    av[1] = NULL;
    std::string local_flagfile = FLAGS_flagfile;
    FLAGS_flagfile = FLAGS_mdt_flagfile;
    ::google::ParseCommandLineFlags(&ac, &av, true);
    delete av;
    FLAGS_flagfile = local_flagfile;
    return Status::OK();
}

Status SearchEngineImpl::OpenDatabase(const std::string& db_name) {
    MutexLock lock(&mu_);
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, ::mdt::Database*>::iterator it = db_map_.find(db_name);
    if (it == db_map_.end()) {
        db_ptr = ::mdt::OpenDatabase(db_name);
        if (db_ptr == NULL) {
            return Status::IOError("db cannot open");
        }
        db_map_.insert(std::pair<std::string, ::mdt::Database*>(db_name, db_ptr));
    }
    return Status::OK();
}

Status SearchEngineImpl::OpenTable(const std::string& db_name, const std::string& table_name) {
    MutexLock lock(&mu_);
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, ::mdt::Database*>::iterator it = db_map_.find(db_name);
    if (it == db_map_.end()) {
        return Status::NotFound("db not found");
    }
    db_ptr = it->second;

    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        table_ptr = ::mdt::OpenTable(db_ptr, tablename);
        if (table_ptr == NULL) {
            return Status::NotFound("table cannot open");
        }
        tablemap.insert(std::pair<std::string, ::mdt::Table*>(internal_tablename, table_ptr));
    }
    return Status::OK();
}

::mdt::Table* SearchEngineImpl::GetTable(const std::string& db_name, const std::string& table_name) {
    MutexLock lock(&mu_);
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, ::mdt::Database*>::iterator it = db_map_.find(db_name);
    if (it == db_map_.end()) {
        return NULL;
    }
    db_ptr = it->second;

    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        return NULL;
    }
    table_ptr = table_it->second;
    return table_ptr;
}

// rpc service
SearchEngineServiceImpl::SearchEngineServiceImpl(SearchEngineImpl* se)
    : se_ = se,
      se_thread_pool_(new ThreadPool(FLAGS_se_num_threads)) {
}

SearchEngineServiceImpl::~SearchEngineServiceImpl() {
    delete se_thread_pool_;
}

int SearchEngineServiceImpl::StartServer() {

}

}
