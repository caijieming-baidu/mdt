// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/bind.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "collector/query_service.h"
#include "proto/scheduler.pb.h"
#include "utils/counter.h"
#include "utils/timer.h"

DECLARE_bool(mdt_flagfile_set);
DECLARE_int32(se_num_threads);
DECLARE_int64(collector_ktera_pending_count);
DECLARE_int64(collector_store_max_pending);
DECLARE_int64(collector_tera_max_pending);
DECLARE_string(flagfile);
DECLARE_string(mdt_flagfile);
DECLARE_string(scheduler_addr);
DECLARE_string(se_service_port);

namespace mdt {

int64_t klast_time_warning;
int64_t last_time_collect_info;
::mdt::Counter store_qps;
::mdt::Counter store_max_packet_size;
::mdt::Counter store_min_packet_size;
::mdt::Counter store_average_packet_size;

::mdt::Counter store_thread_pool_pending;
::mdt::Counter store_thread_pool_sched_ts;
::mdt::Counter store_thread_pool_task_ts;
::mdt::Counter store_thread_pool_task_num;

::mdt::Counter tera_thread_pool_pending;

::mdt::Counter ktera_pending_count;

void* ReportThread(void* arg) {
    SearchEngineImpl* se = (SearchEngineImpl*)arg;
    se->ReportMessage();
    return NULL;
}

// operation
SearchEngineImpl::SearchEngineImpl()
    : stop_report_message_(false) {
    rpc_client_ = new RpcClient();
    pthread_create(&report_tid_, NULL, ReportThread, this);
}
SearchEngineImpl::~SearchEngineImpl() {}

void SearchEngineImpl::ReportMessage() {
    char hostname[255];
    if (0 != gethostname(hostname, 256)) {
        LOG(FATAL) << "fail to report message";
    }
    std::string hostname_str = hostname;
    last_time_collect_info = timer::get_micros();
    while (1) {
        if (stop_report_message_) {
            return;
        }
        std::string local_addr = hostname_str + ":" + FLAGS_se_service_port;
        VLOG(50) << "hostip " << local_addr;
        std::string scheduler_addr = FLAGS_scheduler_addr;

        // report addr
        mdt::LogSchedulerService::LogSchedulerService_Stub* service;
        rpc_client_->GetMethodList(scheduler_addr, &service);
        mdt::LogSchedulerService::RegisterNodeRequest* req = new mdt::LogSchedulerService::RegisterNodeRequest();
        req->set_server_addr(local_addr);
        mdt::LogSchedulerService::CollectorInfo* info = req->mutable_info();

        int64_t nr_sec = (timer::get_micros() - last_time_collect_info) / 1000000;
        nr_sec = nr_sec > 0 ? nr_sec : 1;
        info->set_qps(store_qps.Get() / nr_sec);
        info->set_min_packet_size(store_min_packet_size.Get());
        info->set_max_packet_size(store_max_packet_size.Get());
        info->set_average_packet_size(store_average_packet_size.Get() / (store_qps.Get() + 1));

        info->set_store_pending(store_thread_pool_pending.Get());
        info->set_store_sched_ts(store_thread_pool_sched_ts.Get());
        info->set_store_task_ts(store_thread_pool_task_ts.Get());
        info->set_store_task_num(store_thread_pool_task_num.Get());

        last_time_collect_info = timer::get_micros();
        store_qps.Set(0);
        store_min_packet_size.Set(1000000000);
        store_max_packet_size.Set(0);
        store_average_packet_size.Set(0);

        std::string mkey;
        mkey = "Collector." + local_addr + "." + "kstore_busy_count";
        kcounter_map.Add(mkey, kstore_busy_count.Clear());
        mkey = "Collector." + local_addr + "." + "ktera_busy_count";
        kcounter_map.Add(mkey, ktera_busy_count.Clear());
        mkey = "Collector." + local_addr + "." + "kdb_open_error";
        kcounter_map.Add(mkey, kdb_open_error.Clear());
        mkey = "Collector." + local_addr + "." + "ktable_open_error";
        kcounter_map.Add(mkey, ktable_open_error.Clear());
        mkey = "Collector." + local_addr + "." + "kstore_num";
        kcounter_map.Add(mkey, kstore_num.Clear());
        mkey = "Collector." + local_addr + "." + "ktera_pending_count";
        kcounter_map.Add(mkey, ktera_pending_count.Get());

        // set state info
        std::string key;
        Counter val;
        while (kcounter_map.ScanAndDelete(&key, &val, true) >= 0) {
            int64_t value = val.Get();
            mdt::LogSchedulerService::CounterMap* c = info->add_counter_map();
            c->set_key(key);
            c->set_val(value);
            VLOG(50) << "<<<<<<<<<< key " << key << ", val " << value;

            value = val.Get() / nr_sec;
            c = info->add_counter_map();
            key += "_average";
            c->set_key(key);
            c->set_val(value);
            VLOG(50) << "<<<<<<<<<< key " << key << ", val " << value;
        }

        mdt::LogSchedulerService::RegisterNodeResponse* resp = new mdt::LogSchedulerService::RegisterNodeResponse();

        boost::function<void (const mdt::LogSchedulerService::RegisterNodeRequest*,
                mdt::LogSchedulerService::RegisterNodeResponse*,
                bool, int)> callback =
            boost::bind(&SearchEngineImpl::ReportMessageCallback,
                    this, _1, _2, _3, _4, service);
        rpc_client_->AsyncCall(service,
                               &mdt::LogSchedulerService::LogSchedulerService_Stub::RegisterNode,
                               req, resp, callback);
        report_event_.Wait();
        sleep(2);
    }
}

void SearchEngineImpl::ReportMessageCallback(const mdt::LogSchedulerService::RegisterNodeRequest* req,
                                             mdt::LogSchedulerService::RegisterNodeResponse* resp,
                                             bool failed, int error,
                                             mdt::LogSchedulerService::LogSchedulerService_Stub* service) {
    delete req;
    delete resp;
    delete service;
    report_event_.Set();
}

// init mdt.flag
Status SearchEngineImpl::InitSearchEngine() {
#if 0
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
    FLAGS_flagfile = FLAGS_mdt_flagfile;
    ::google::ParseCommandLineFlags(&ac, &av, true);
    delete av;
#endif
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
        if (klast_time_warning + 60000000 < timer::get_micros()) {
            klast_time_warning = timer::get_micros();
            LOG(WARNING) << "open db error, db " << db_name << ", table " << table_name;
        }
        return Status::NotFound("db not found");
    }
    db_ptr = it->second;

    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        table_ptr = ::mdt::OpenTable(db_ptr, table_name);
        if (table_ptr == NULL) {
            if (klast_time_warning + 60000000 < timer::get_micros()) {
                klast_time_warning = timer::get_micros();
                LOG(WARNING) << "open table error, db " << db_name << ", table " << table_name;
            }
            return Status::NotFound("table cannot open");
        }
        table_map_.insert(std::pair<std::string, ::mdt::Table*>(internal_tablename, table_ptr));
    }
    return Status::OK();
}

::mdt::Table* SearchEngineImpl::GetTable(const std::string& db_name, const std::string& table_name) {
    MutexLock lock(&mu_);
    ::mdt::Table* table_ptr = NULL;
    std::string internal_tablename = db_name + "#" + table_name;
    std::map<std::string, ::mdt::Table*>::iterator table_it = table_map_.find(internal_tablename);
    if (table_it == table_map_.end()) {
        return NULL;
    }
    table_ptr = table_it->second;
    return table_ptr;
}

void RpcRequestToMdtRequest(const ::mdt::SearchEngine::RpcSearchRequest* req, ::mdt::SearchRequest* req2) {
    req2->primary_key = req->primary_key();
    req2->start_timestamp = req->start_timestamp();
    req2->end_timestamp = req->end_timestamp();
    req2->limit = req->limit();
    for (int i = 0; i < req->condition_size(); i++) {
        const ::mdt::SearchEngine::RpcIndexCondition& idx = req->condition(i);
        ::mdt::IndexCondition idx2;
        idx2.index_name = idx.index_table_name();
        if (idx.cmp() == ::mdt::SearchEngine::RpcGreater) {
            idx2.comparator = kGreater;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcEqualTo) {
            idx2.comparator = kEqualTo;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcNotEqualTo) {
            idx2.comparator = kNotEqualTo;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcLess) {
            idx2.comparator = kLess;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcLessEqual) {
            idx2.comparator = kLessEqual;
        } else if (idx.cmp() == ::mdt::SearchEngine::RpcGreaterEqual) {
            idx2.comparator = kGreaterEqual;
        } else {
            // cmp type not know
            idx2.comparator = (COMPARATOR)idx.cmp();
        }
        idx2.compare_value = idx.cmp_key();
        req2->index_condition_list.push_back(idx2);
    }
}

void MdtResponseToRpcResponse(::mdt::SearchResponse* resp2, ::mdt::SearchEngine::RpcSearchResponse* resp) {
    for (int i = 0; i < (int)resp2->result_stream.size(); i++) {
        ::mdt::SearchEngine::RpcResultStream* stream = resp->add_result_list();
        const ::mdt::ResultStream& stream2 = resp2->result_stream[i];
        stream->set_primary_key(stream2.primary_key);
        for (int j = 0; j < (int)stream2.result_data_list.size(); j++) {
            std::string* str = stream->add_data_list();
            *str = stream2.result_data_list[j];
            //stream->set_data_list(j, stream2.result_data_list[j]);
        }
    }
}

void SearchEngineImpl::Search(::google::protobuf::RpcController* ctrl,
                                const ::mdt::SearchEngine::RpcSearchRequest* req,
                                ::mdt::SearchEngine::RpcSearchResponse* resp,
                                ::google::protobuf::Closure* done) {
    Status s = OpenDatabase(req->db_name());
    if (!s.ok()) {
        kdb_open_error.Inc();
        done->Run();
        return;
    }
    s = OpenTable(req->db_name(), req->table_name());
    if (!s.ok()) {
        ktable_open_error.Inc();
        done->Run();
        return;
    }
    ::mdt::Table* table = GetTable(req->db_name(), req->table_name());
    ::mdt::SearchRequest request;
    ::mdt::SearchResponse response;
    RpcRequestToMdtRequest(req, &request);
    table->Get(&request, &response, NULL, NULL);
    MdtResponseToRpcResponse(&response, resp);
    done->Run();
    return;
}

// store service
void RpcStoreRequestToMdtRequest(const ::mdt::SearchEngine::RpcStoreRequest* req,
                                 ::mdt::StoreRequest* request) {
    request->primary_key = req->primary_key();
    request->timestamp = req->timestamp();
    request->data = req->data();
    for (int i = 0; i < req->index_list_size(); i++) {
        const ::mdt::SearchEngine::RpcStoreIndex& idx = req->index_list(i);
        ::mdt::Index index;
        index.index_name = idx.index_table();
        index.index_key = idx.key();
        request->index_list.push_back(index);
    }
}

void MdtResponseToRpcStoreResponse(::mdt::StoreResponse* response,
                                   ::mdt::SearchEngine::RpcStoreResponse* resp) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
}

struct StoreCallback_param {
    ::mdt::SearchEngine::RpcStoreResponse* resp;
    ::google::protobuf::Closure* done;
};

void StoreCallback_dump(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    VLOG(30) << "store callback, pkey " << request->primary_key << ", req " << (uint64_t)request << ", resp " << (uint64_t)response;
    StoreCallback_param* param = (StoreCallback_param*)callback_param;
    MdtResponseToRpcStoreResponse(response, param->resp);
    param->done->Run();
    ktera_pending_count.Dec();
    delete request;
    delete response;
    delete param;
}
void SearchEngineImpl::Store(::google::protobuf::RpcController* ctrl,
                             const ::mdt::SearchEngine::RpcStoreRequest* req,
                             ::mdt::SearchEngine::RpcStoreResponse* resp,
                             ::google::protobuf::Closure* done) {
    resp->set_status(mdt::SearchEngine::RpcOK);
    Status s = OpenDatabase(req->db_name());
    if (!s.ok()) {
        kdb_open_error.Inc();
        VLOG(30) << "open db error, db " << req->db_name() << ", table " << req->table_name();
        done->Run();
        return;
    }
    s = OpenTable(req->db_name(), req->table_name());
    if (!s.ok()) {
        ktable_open_error.Inc();
        VLOG(30) << "open table error, db " << req->db_name() << ", table " << req->table_name();
        done->Run();
        return;
    }

    store_qps.Inc();
    store_average_packet_size.Add((int64_t)(req->data().size()));
    if (store_max_packet_size.Get() < (int64_t)(req->data().size())) {
        store_max_packet_size.Set((int64_t)(req->data().size()));
    }
    if (store_min_packet_size.Get() > (int64_t)(req->data().size())) {
        store_min_packet_size.Set((int64_t)(req->data().size()));
    }

    VLOG(30) << "store, pkey " << req->primary_key();

    ::mdt::Table* table = GetTable(req->db_name(), req->table_name());
    ::mdt::TableProfile profile;
    table->Profile(&profile);
    tera_thread_pool_pending.Set(profile.tera_nr_pending);
    if (profile.tera_nr_pending > (uint64_t)FLAGS_collector_tera_max_pending) {
        ktera_busy_count.Inc();
        resp->set_status(mdt::SearchEngine::ServerBusy);
        done->Run();
        if (klast_time_warning + 60000000 < timer::get_micros()) {
            klast_time_warning = timer::get_micros();
            LOG(WARNING) << "server busy, nr_pending profile.tera_nr_pending " << profile.tera_nr_pending;
        }
        return;
    }

    if (ktera_pending_count.Get() > FLAGS_collector_ktera_pending_count) {
        ktera_busy_count.Inc();
        resp->set_status(mdt::SearchEngine::ServerBusy);
        done->Run();
        if (klast_time_warning + 60000000 < timer::get_micros()) {
            klast_time_warning = timer::get_micros();
            LOG(WARNING) << "server busy, ktera_pending_count " << ktera_pending_count.Get();
        }
        return;
    }
    ktera_pending_count.Inc();
    VLOG(30) << "store key " << req->primary_key();

    ::mdt::StoreRequest* request = new ::mdt::StoreRequest();
    ::mdt::StoreResponse* response = new ::mdt::StoreResponse();
    StoreCallback_param* param = new StoreCallback_param();
    param->resp = resp;
    param->done = done;
    ::mdt::StoreCallback callback = StoreCallback_dump;
    RpcStoreRequestToMdtRequest(req, request);

    int64_t cost_ts = timer::get_micros();
    std::string pkey = req->primary_key();
    table->Put(request, response, callback, param);
    kstore_num.Inc();
    VLOG(5) << "store, pkey " << pkey << ", cost time " << timer::get_micros() - cost_ts;
    return;
}

/////////////////////////////////
//      rpc service            //
/////////////////////////////////
SearchEngineServiceImpl::SearchEngineServiceImpl(SearchEngineImpl* se)
    : se_(se),
      se_thread_pool_(new ThreadPool(FLAGS_se_num_threads)),
      se_read_thread_pool_(new ThreadPool(FLAGS_se_num_threads)),
      se_info_thread_pool_(new ThreadPool(2)) {

    klast_time_warning = timer::get_micros();
    ThreadPool::Task task = boost::bind(&SearchEngineServiceImpl::BGInfoCollector, this);
    se_info_thread_pool_->DelayTask(1000, task);
}

SearchEngineServiceImpl::~SearchEngineServiceImpl() {
    delete se_thread_pool_;
    delete se_read_thread_pool_;
    delete se_info_thread_pool_;
}

void SearchEngineServiceImpl::BGInfoCollector() {
    LOG(INFO) << "Thread pool[Store] " << se_thread_pool_->ProfilingLog()
        << ", pending req(store) " << se_thread_pool_->PendingNum()
        << ", [Search] " << se_read_thread_pool_->ProfilingLog()
        << ", pending req(search) " << se_read_thread_pool_->PendingNum();

    store_thread_pool_pending.Set(se_thread_pool_->PendingNum());
    std::vector<std::string> profile;
    std::string profile_str = se_thread_pool_->ProfilingLog();
    boost::split(profile, profile_str, boost::is_any_of(" "));
    int64_t sched_ts = atol(profile[0].c_str());
    int64_t task_ts = atol(profile[1].c_str());
    int64_t task_num = atol(profile[2].c_str());
    store_thread_pool_sched_ts.Set(sched_ts);
    store_thread_pool_task_ts.Set(task_ts);
    store_thread_pool_task_num.Set(task_num);

    ThreadPool::Task task = boost::bind(&SearchEngineServiceImpl::BGInfoCollector, this);
    se_info_thread_pool_->DelayTask(1000, task);
}

void SearchEngineServiceImpl::Search(::google::protobuf::RpcController* ctrl,
                                     const ::mdt::SearchEngine::RpcSearchRequest* req,
                                     ::mdt::SearchEngine::RpcSearchResponse* resp,
                                     ::google::protobuf::Closure* done) {
    ThreadPool::Task task = boost::bind(&SearchEngineImpl::Search, se_, ctrl, req, resp, done);
    se_read_thread_pool_->AddTask(task);
}

void SearchEngineServiceImpl::Store(::google::protobuf::RpcController* ctrl,
                                    const ::mdt::SearchEngine::RpcStoreRequest* req,
                                    ::mdt::SearchEngine::RpcStoreResponse* resp,
                                    ::google::protobuf::Closure* done) {
    resp->set_status(mdt::SearchEngine::RpcOK);
    //VLOG(30) << "store, pkey " << req->primary_key();
    if (se_thread_pool_->PendingNum() > FLAGS_collector_store_max_pending) {
        se_->kstore_busy_count.Inc();
        resp->set_status(mdt::SearchEngine::ServerBusy);
        done->Run();
        if (klast_time_warning + 60000000 < timer::get_micros()) {
            klast_time_warning = timer::get_micros();
            LOG(WARNING) << "server busy, store busy, nr_pending " << se_thread_pool_->PendingNum();
        }
        return;
    }
    ThreadPool::Task task = boost::bind(&SearchEngineImpl::Store, se_, ctrl, req, resp, done);
    se_thread_pool_->AddTask(task);
}

void SearchEngineServiceImpl::OpenTable(::google::protobuf::RpcController* ctrl,
                                        const ::mdt::SearchEngine::RpcOpenTableRequest* req,
                                        ::mdt::SearchEngine::RpcOpenTableResponse* resp,
                                        ::google::protobuf::Closure* done) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
    done->Run();
}

void SearchEngineServiceImpl::OpenDatabase(::google::protobuf::RpcController* ctrl,
                                           const ::mdt::SearchEngine::RpcOpenDatabaseRequest* req,
                                           ::mdt::SearchEngine::RpcOpenDatabaseResponse* resp,
                                           ::google::protobuf::Closure* done) {
    resp->set_status(::mdt::SearchEngine::RpcOK);
    done->Run();
}

}
