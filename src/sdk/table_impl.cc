// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <deque>
#include <sstream>

#include <boost/bind.hpp>
#include <glog/logging.h>
#include <tera.h>

#include "proto/kv.pb.h"
#include "sdk/sdk.h"
#include "sdk/table_impl.h"
#include "utils/string_util.h"

DECLARE_int64(concurrent_write_handle_num);
DECLARE_int64(max_write_handle_seq);
DECLARE_int64(data_size_per_sync);
DECLARE_bool(use_tera_async_write);
DECLARE_int64(write_batch_queue_size);
DECLARE_int64(request_queue_flush_internal);
DECLARE_int64(max_timestamp_table_num);
DECLARE_int64(read_file_thread_num);
DECLARE_int64(cleaner_thread_num);
DECLARE_bool(enable_multi_version_read);
DECLARE_bool(read_by_index_filter);
DECLARE_bool(enable_scan_control);
DECLARE_int64(batch_scan_buffer_size);
DECLARE_bool(enable_qu_range);
DECLARE_int64(tera_scan_pack_interval);
DECLARE_bool(enable_number_limit);
DECLARE_int64(scan_number_limit);
DECLARE_bool(enable_async_read);
DECLARE_int64(async_read_thread_num);
DECLARE_int64(tera_table_ttl);
DECLARE_int64(gc_interval);
DECLARE_int64(tera_span_size);
DECLARE_bool(delete_unknow_file);
DECLARE_bool(enable_data_compress);
DECLARE_bool(enable_async_index_write);
DECLARE_int64(async_tera_writer_num);
DECLARE_bool(ignore_tera_write_error);
DECLARE_bool(sdk_disable_tera_ts_write);
DECLARE_int64(switch_file_time);

namespace mdt {
static ThreadPool GLOBAL_read_row_data_threads(FLAGS_read_file_thread_num);
static ThreadPool GLOBAL_cleaner_thread(FLAGS_cleaner_thread_num);
static ThreadPool GLOBAL_read_cell_data_threads(FLAGS_async_read_thread_num);
static ThreadPool GLOBAL_async_tera_write_thread(FLAGS_async_tera_writer_num);
static ThreadPool GLOBAL_info_collector_threads(1);

static int64_t ksdk_last_warning_ts = 0;

std::ostream& operator << (std::ostream& o, const FileLocation& location) {
    o << "file: " << location.fname_
      << " offset: " << location.offset_
      << " size: " << location.size_;
    return o;
}

Status TableImpl::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr) {
    const std::string& table_name = table_desc.table_name;
    // init fs adapter
    FilesystemAdapter fs_adapter;
    fs_adapter.root_path_ = fs_opt.fs_path_ + "/" + table_name + "/";
    LOG(INFO) << "data file dir: " << fs_adapter.root_path_;
    fs_adapter.env_ = fs_opt.env_;

    ksdk_last_warning_ts = timer::get_micros();

    //init tera adapter
    TeraAdapter tera_adapter;
    tera_adapter.opt_ = tera_opt;
    tera_adapter.table_prefix_ = db_name;

    TableImpl* table = new TableImpl(table_desc, tera_adapter, fs_adapter);
    Status s = table->Init();
    if (s.ok()) {
        *table_ptr = table;
    }
    return s;
}

Status TableImpl::Init() {
    // create fs dir
    fs_.env_->CreateDir(fs_.root_path_);

    // read cache
    seq_cnt_ = 1;

    // init write handle list
    nr_write_handle_ = (int)FLAGS_concurrent_write_handle_num;
    cur_write_handle_id_ = 0;
    cur_write_handle_seq_ = 0;
    write_handle_list_.clear();
    WriteHandle write_handle;
    write_handle.write_queue_.clear();
    write_handle.writer_ = NULL;
    for (int i = 0; i < nr_write_handle_; ++i) {
        write_handle_list_.push_back(write_handle);
    }

    tera::ErrorCode error_code;
    int nr_error = 0;
    // open primary key table
    std::string primary_table_name = tera_.table_prefix_ + "#pri#" + table_desc_.table_name;
    LOG(INFO) << "open primary table: " << primary_table_name;
    tera::Table* primary_table = tera_.opt_.client_->OpenTable(primary_table_name, &error_code);
    if (primary_table == NULL) {
        nr_error++;
        LOG(WARNING) << "open table " << table_desc_.table_name << " fail, no such table\n";
    }
    tera_.tera_table_map_[primary_table_name] = primary_table;

    // open index key table
    std::vector<IndexDescription>::iterator it;
    for (it = table_desc_.index_descriptor_list.begin();
         it != table_desc_.index_descriptor_list.end();
         ++it) {
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + it->index_name;
        //std::string index_table_name = GetIndexTable(it->index_name);
        LOG(INFO) << "open index table: " << index_table_name;
        tera::Table* index_table = tera_.opt_.client_->OpenTable(index_table_name, &error_code);
        if (index_table == NULL) {
            nr_error++;
            LOG(WARNING) << "open index table " << it->index_name << " fail, no such table\n";
        }
        tera_.tera_table_map_[index_table_name] = index_table;
    }

    // open timestamp index table
    nr_timestamp_table_ = (int)FLAGS_max_timestamp_table_num;
    cur_timestamp_table_id_ = 0;
    for (int i = 0; i < nr_timestamp_table_; i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + ts_name;
        LOG(INFO) << "open index table: " << index_table_name;
        tera::Table* index_table = tera_.opt_.client_->OpenTable(index_table_name, &error_code);
        if (index_table == NULL) {
            nr_error++;
            LOG(WARNING) << "open ts index table " << ts_name << " fail, no such table\n";
        }
        tera_.tera_table_map_[index_table_name] = index_table;
    }

    // do some error
    if (nr_error) {
        FreeTeraTable();
        return Status::NotFound("index table not found");
    }

    return Status::OK();
}

void TableImpl::FreeTeraTable() {
    std::map<std::string, tera::Table*>::iterator it = tera_.tera_table_map_.begin();
    for (; it != tera_.tera_table_map_.end(); ++it) {
        tera::Table* table_ptr = it->second;
        if (table_ptr != NULL) {
            delete table_ptr;
            it->second = NULL;
        }
    }
}

// TableImpl ops
::leveldb::Options SanitizeOptions(const ::leveldb::InternalKeyComparator* icmp,
                                   const ::leveldb::Options& src) {
  ::leveldb::Options result = src;
  result.comparator = icmp;
  return result;
}
TableImpl::TableImpl(const TableDescription& table_desc,
                     const TeraAdapter& tera_adapter,
                     const FilesystemAdapter& fs_adapter)
    : internal_comparator_(::leveldb::Options().comparator),
    leveldb_options_(SanitizeOptions(&internal_comparator_, ::leveldb::Options())),
    table_desc_(table_desc),
    tera_(tera_adapter),
    fs_(fs_adapter),
    thread_pool_(&GLOBAL_read_row_data_threads),
    cleaner_thread_(&GLOBAL_cleaner_thread),
    async_read_thread_(&GLOBAL_read_cell_data_threads),
    queue_timer_stop_(false),
    queue_timer_cv_(&queue_timer_mu_),
    async_tera_writer_(&GLOBAL_async_tera_write_thread),
    info_collector_thread_(&GLOBAL_info_collector_threads) {
    // create timer
    assert(pthread_create(&timer_tid_, NULL, &TableImpl::TimerThreadWrapper, this) >= 0);

    // create gc
    //ttl_ = FLAGS_tera_table_ttl;
    ttl_ = table_desc.table_ttl;
    gc_stop_ = false;
    assert(pthread_create(&gc_tid_, NULL, &TableImpl::GarbageCleanThreadWrapper, this) >= 0);

    // background info collector
    ThreadPool::Task task = boost::bind(&TableImpl::BGInfoCollector, this);
    info_collector_thread_->DelayTask(1000, task);
}

TableImpl::~TableImpl() {
    // free write handle
    //ReleaseDataWriter();
    // TODO: write queue release
    thread_pool_->Stop(false);
    FreeTeraTable();

    cleaner_thread_->Stop(false);
    async_read_thread_->Stop(false);

    // stop timer, flush request
    queue_timer_mu_.Lock();
    queue_timer_stop_ = true;
    queue_timer_cv_.Wait();
    queue_timer_mu_.Unlock();

    std::vector<WriteContext*> local_queue;
    WriteContext* context = NULL;
    GetAllRequest(&context, &local_queue);
    // Batch write
    if (context) {
        InternalBatchWrite(context, local_queue);
        delete context;
    }

    // gc stop
    gc_stop_ = true;
    //pthread_join(gc_tid_, NULL);
    return;
}

void TableImpl::Profile(TableProfile* profile) {
    profile->tera_nr_pending = async_tera_writer_->PendingNum();
    return;
}

void TableImpl::BGInfoCollector() {
    LOG(INFO) << "Thread pool[async write] " << async_tera_writer_->ProfilingLog()
        << ", pending req(async write) " << async_tera_writer_->PendingNum();

    ThreadPool::Task task = boost::bind(&TableImpl::BGInfoCollector, this);
    info_collector_thread_->DelayTask(1000, task);
}

void TableImpl::CleanerThread(tera::ResultStream* stream) {
    delete stream;
    VLOG(30) << "do something to clean up";
}

/////////  batch write /////////////
struct DefaultBatchWriteCallbackParam {
    CondVar* cond_;
    BatchWriteContext* ctx_;
    Counter counter_; // last one wakeup wait thread
};

void DefaultBatchWriteCallback(Table* table, StoreRequest* request,
                       StoreResponse* response,
                       void* callback_param) {
    DefaultBatchWriteCallbackParam* param = (DefaultBatchWriteCallbackParam*)callback_param;
    if (response->error != 0) {
        param->ctx_->error = response->error;
    }
    if (param->counter_.Dec() == 0) {
        if (param->ctx_->callback == NULL) {
            param->cond_->Signal();
        } else {
            param->ctx_->callback(table, param->ctx_);
            delete param;
        }
    }
}

int TableImpl::BatchWrite(BatchWriteContext* ctx) {
    if (ctx == NULL || ctx->nr_batch <= 0) {
        return 0;
    }
    ctx->error = 0;

    // if sync write, construct default callback
    Mutex mu;
    CondVar cond(&mu);
    DefaultBatchWriteCallbackParam param;
    param.cond_ = &cond;

    StoreCallback internal_callback = DefaultBatchWriteCallback;
    void* internal_param = NULL;
    if (ctx->callback == NULL) {
        // sync batchwrite
        param.ctx_ = ctx;
        param.counter_.Set(ctx->nr_batch);
        internal_param = &param;
    } else {
        // async batchwrite
        DefaultBatchWriteCallbackParam* async_param = new DefaultBatchWriteCallbackParam;
        async_param->ctx_ = ctx;
        async_param->counter_.Set(ctx->nr_batch);
        internal_param = async_param;
    }

    // first request need batch control
    WriteContext* context = new WriteContext(&write_mutex_);
    context->req_ = &(ctx->req[0]);
    context->resp_ = &(ctx->resp[0]);
    context->callback_ = internal_callback;
    context->callback_param_ = internal_param;
    context->sync_ = true;
    context->is_wait_ = false;
    context->done_ = false;

    std::vector<WriteContext*> local_queue;
    for (int i = 1; i < ctx->nr_batch; i++) {
        WriteContext* write_ctx = new WriteContext(&write_mutex_);
        write_ctx->req_ = &(ctx->req[i]);
        write_ctx->resp_ = &(ctx->resp[i]);
        write_ctx->callback_ = internal_callback;
        write_ctx->callback_param_ = internal_param;
        write_ctx->sync_ = true;
        write_ctx->is_wait_ = false;
        write_ctx->done_ = false;
        local_queue.push_back(write_ctx);
    }

    // exec batch write
    InternalBatchWrite(context, local_queue);

    // sync wait
    if (context->callback_ == DefaultBatchWriteCallback) {
        param.cond_->Wait();
    }
    delete context;
    return 0;
}

///////////  High concurrency write interface ////////////////
// cache req optimise for write file system with high latency in ASYNC write tera mode
bool TableImpl::SubmitRequest(WriteContext* context, std::vector<WriteContext*>* local_queue) {
    queue_mutex_.Lock();
    // only async write can use batch
    VLOG(25) << "Batch Queue size " << batch_queue_.size() << ", new context " << (uint64_t)context;
    if (FLAGS_use_tera_async_write && (batch_queue_.size() < (uint32_t)FLAGS_write_batch_queue_size)) {
        // req and resp will be free, after Put return
        batch_queue_.push_back(context);
        queue_mutex_.Unlock();
        return true;
    }
    // copy to local queue
    std::vector<WriteContext*>::iterator wc_it = batch_queue_.begin();
    for (; wc_it != batch_queue_.end(); ++wc_it) {
        local_queue->push_back(*wc_it);
    }
    batch_queue_.clear();
    queue_mutex_.Unlock();
    return false;
}

void TableImpl::GetAllRequest(WriteContext** context_ptr, std::vector<WriteContext*>* local_queue) {
    *context_ptr = NULL;
    local_queue->clear();
    queue_mutex_.Lock();
    std::vector<WriteContext*>::iterator it = batch_queue_.begin();
    for (; it != batch_queue_.end(); ++it) {
        if (*context_ptr == NULL) {
            *context_ptr = *it;
        } else {
            local_queue->push_back(*it);
        }
    }
    batch_queue_.clear();
    queue_mutex_.Unlock();
    return;
}

void* TableImpl::TimerThreadWrapper(void* arg) {
    reinterpret_cast<TableImpl*>(arg)->QueueTimerFunc();
    return NULL;
}

void TableImpl::QueueTimerFunc() {
    while (1) {
        // Get request from queue
        std::vector<WriteContext*> local_queue;
        WriteContext* context = NULL;
        GetAllRequest(&context, &local_queue);
        // Batch write
        if (context) {
            VLOG(25) << "queue timer get req " << (uint64_t)context << ", local queue size "
                << local_queue.size();
            InternalBatchWrite(context, local_queue);
            delete context;
        }

        // timer controller
        queue_timer_mu_.Lock();
        if (queue_timer_stop_) {
            queue_timer_cv_.Signal();
            queue_timer_mu_.Unlock();
            return;
        }
        queue_timer_mu_.Unlock();
        usleep(FLAGS_request_queue_flush_internal);
    }
    return;
}

// timer context
int TableImpl::InternalBatchWrite(WriteContext* context, std::vector<WriteContext*>& ctx_queue) {
    // new feature, data compress
    if (FLAGS_enable_data_compress) {
        return InternalCompressBatchWrite(context, ctx_queue);
    }

    // Get write handle
    WriteHandle* write_handle = GetWriteHandle();

    VLOG(20) << ">>>>> begin put, ctx " << (uint64_t)(context);
    write_mutex_.Lock();
    write_handle->write_queue_.push_back(context);
    std::vector<WriteContext*>::iterator wc_it = ctx_queue.begin();
    for (; wc_it != ctx_queue.end(); ++wc_it) {
        write_handle->write_queue_.push_back(*wc_it);
    }
    while (!context->done_ && (context != write_handle->write_queue_.front())) {
        VLOG(20) << "===== waitlock put, ctx " << (uint64_t)(context);
        context->is_wait_ = true;
        context->cv_.Wait();
    }
    if (context->done_) {
        write_mutex_.Unlock();
        //delete context;
        return 0;
    }

    // merge WriteContext
    WriteBatch wb;
    std::deque<WriteContext*>::iterator iter = write_handle->write_queue_.begin();
    for (; iter != write_handle->write_queue_.end(); ++iter) {
        wb.Append(*iter);
    }

    // unlock do io
    write_mutex_.Unlock();
    VLOG(20) << ">>>>> lock put, ctx " << (uint64_t)(context);

    int32_t write_retry = 3;
    bool is_success = false;
    while (write_retry > 0) {
        // batch write file system
        FileLocation location;
        DataWriter* writer = GetDataWriter(write_handle);
        if (writer->AddRecord(wb.rep_, &location) == 0) {
            // write index table
            std::vector<WriteContext*>::iterator it = wb.context_list_.begin();
            for (; it != wb.context_list_.end(); ++it) {
                WriteContext* ctx = *it;
                FileLocation data_location = location;
                data_location.size_ = ctx->req_->data.size();
                data_location.offset_ += ctx->offset_;
                VLOG(20) << "put record: offset " << data_location.offset_ << ", size " << data_location.size_
                    << ", pri key " << ctx->req_->primary_key;
                WriteIndexTable(ctx->req_, ctx->resp_, ctx->callback_, ctx->callback_param_, data_location);
            }
            is_success = true;
            break;
        } else {
            // file system error
            ReleaseDataWriter(write_handle);
        }
        write_retry--;
        LOG(INFO) << "fs retry " << write_retry;
    }
    if (!is_success) {
        LOG(WARNING) << "retry 3 times, but fail";
        std::vector<WriteContext*>::iterator it = wb.context_list_.begin();
        for (; it != wb.context_list_.end(); ++it) {
            WriteContext* ctx = *it;
            ctx->callback_(this, (StoreRequest*)ctx->req_, ctx->resp_,
                            ctx->callback_param_);
        }
    }

    // lock, resched other WriteContext
    VLOG(30) << "<<<<< unlock put";
    write_mutex_.Lock();
    WriteContext* last_writer = wb.context_list_.back();
    while (true) {
        WriteContext* ready = write_handle->write_queue_.front();
        write_handle->write_queue_.pop_front();
        if (ready != context) {
            ready->done_ = true;
            if (ready->is_wait_) {
                // wait up suspend thread, so that it can do something cleanup
                ready->cv_.Signal();
            } else {
                // this write context has no context, help it free memory
                VLOG(25) << "no write context " << (uint64_t)ready;
                delete ready;
            }
        }
        // cannot access ready any more
        if (ready == last_writer) {
            break;
        }
    }
    if (!write_handle->write_queue_.empty()) {
        write_handle->write_queue_.front()->cv_.Signal();
    }

    write_mutex_.Unlock();
    VLOG(20) << "<<<<< finish put, ctx " << (uint64_t)(context);
    //delete context;
    return 0;
}

int TableImpl::InternalCompressBatchWrite(WriteContext* context, std::vector<WriteContext*>& ctx_queue) {
    // Get write handle
    WriteHandle* write_handle = GetWriteHandle();

    VLOG(20) << ">>>>> begin put, ctx " << (uint64_t)(context);
    write_mutex_.Lock();
    write_handle->write_queue_.push_back(context);
    std::vector<WriteContext*>::iterator wc_it = ctx_queue.begin();
    for (; wc_it != ctx_queue.end(); ++wc_it) {
        write_handle->write_queue_.push_back(*wc_it);
    }
    while (!context->done_ && (context != write_handle->write_queue_.front())) {
        VLOG(20) << "===== waitlock put, ctx " << (uint64_t)(context);
        context->is_wait_ = true;
        context->cv_.Wait();
    }
    if (context->done_) {
        write_mutex_.Unlock();
        //delete context;
        return 0;
    }

    // merge WriteContext
    BlockBuilder* sort_block = new BlockBuilder(&internal_comparator_, &leveldb_options_);
    std::map<std::string, BlockBuilder*> index_block;
    std::map<std::string, std::vector<WriteContext*> > primary_key_map;
    std::map<std::string, int64_t> timestamp_map;
    WriteContext* last_writer = NULL;
    std::deque<WriteContext*>::iterator iter = write_handle->write_queue_.begin();
    for (; iter != write_handle->write_queue_.end(); ++iter) {
        last_writer = *iter;
        std::string type_primary_key;
        StringToTypeString("pri", last_writer->req_->primary_key, &type_primary_key);
        if (last_writer->req_->data.size()) {
            sort_block->Add(type_primary_key, last_writer->req_->data);
        }
        // support data vec write
        for (uint32_t nr_data = 0; nr_data < last_writer->req_->vec_data.size(); nr_data++) {
            sort_block->Add(type_primary_key, last_writer->req_->vec_data[nr_data]);
        }

        // aggre primary key
        std::vector<WriteContext*>& write_vec = primary_key_map[last_writer->req_->primary_key];
        write_vec.push_back(last_writer);

        BlockBuilder* iblock = NULL;
        if (index_block.find(last_writer->req_->primary_key) == index_block.end()) {
            iblock = new BlockBuilder(&internal_comparator_, &leveldb_options_);
            index_block[last_writer->req_->primary_key] = iblock;
        } else {
            iblock = index_block[last_writer->req_->primary_key];
        }

        std::vector<Index>::const_iterator tmp_it;
        for (tmp_it = last_writer->req_->index_list.begin(); tmp_it != last_writer->req_->index_list.end(); ++tmp_it) {
            const std::string& index_name = tmp_it->index_name;
            const std::string& index_key = tmp_it->index_key;
            // convert string into type string
            std::string type_index_key;
            StringToTypeString(index_name, index_key, &type_index_key);
            if (index_name == "" || index_key == "") {
                LOG(WARNING) << "invalid index : " << index_name << " : " << index_key;
                continue;
            }
            iblock->Add(index_name, type_index_key);
        }

        if ((timestamp_map.find(last_writer->req_->primary_key) == timestamp_map.end()) ||
            (timestamp_map[last_writer->req_->primary_key] < last_writer->req_->timestamp)) {
            timestamp_map[last_writer->req_->primary_key] = last_writer->req_->timestamp;
        }
    }
    // unlock do io
    write_mutex_.Unlock();
    VLOG(20) << ">>>>> lock put, ctx " << (uint64_t)(context);

    // construct sorted block, and write into fs
    int64_t cost_ts;
    cost_ts = timer::get_micros();
    sort_block->Finish();
    FileLocation location;
    DataWriter* writer = GetDataWriter(write_handle);
    writer->AddCompressRecord(sort_block->GetCompressionBlock(), &location);
    delete sort_block;
    VLOG(30) << "write fs cost time " << timer::get_micros() - cost_ts;

    // batch write index table
    cost_ts = timer::get_micros();
    std::map<std::string, std::vector<WriteContext*> >::iterator it = primary_key_map.begin();
    for (; it != primary_key_map.end(); ++it) {
        (index_block[it->first])->Finish();
        std::vector<WriteContext*>& vec_ctx = it->second;

        // construct batch write b_context
        BatchIndexContext* b_context = new BatchIndexContext();
        uint64_t ref = 2 + ((index_block[it->first])->NumEntries());
        b_context->SetReference(ref);
        b_context->SetTable(this);
        std::vector<WriteContext*>::iterator ctx_it = vec_ctx.begin();
        for (; ctx_it != vec_ctx.end(); ++ctx_it) {
            WriteContext* wc = *ctx_it;
            b_context->PushBack(wc->req_, wc->resp_, wc->callback_, wc->callback_param_);
        }

        if (FLAGS_enable_async_index_write) {
            ThreadPool::Task task = boost::bind(&TableImpl::AsyncWriteBatchIndexTable, this,
                    it->first, timestamp_map[it->first], b_context, index_block[it->first], location);
            async_tera_writer_->AddTask(task);
        } else {
            WriteBatchIndexTable(it->first, timestamp_map[it->first], b_context, index_block[it->first], location);
            delete (index_block[it->first]);
        }
    }
    VLOG(30) << "write tera cost time " << timer::get_micros() - cost_ts;

    // lock, resched other WriteContext
    VLOG(30) << "<<<<< unlock put";
    write_mutex_.Lock();
    //WriteContext* last_writer = wb.context_list_.back();
    while (true) {
        WriteContext* ready = write_handle->write_queue_.front();
        write_handle->write_queue_.pop_front();
        if (ready != context) {
            ready->done_ = true;
            if (ready->is_wait_) {
                // wait up suspend thread, so that it can do something cleanup
                ready->cv_.Signal();
            } else {
                // this write context has no context, help it free memory
                VLOG(25) << "no write context " << (uint64_t)ready;
                delete ready;
            }
        }
        // cannot access ready any more
        if (ready == last_writer) {
            break;
        }
    }
    if (!write_handle->write_queue_.empty()) {
        write_handle->write_queue_.front()->cv_.Signal();
    }

    write_mutex_.Unlock();
    VLOG(20) << "<<<<< finish put, ctx " << (uint64_t)(context);
    //delete context;
    return 0;
}
struct SyncWriteCallbackParam {
    CondVar* cond_;
};

void SyncWriteCallback(Table* table, StoreRequest* request,
                       StoreResponse* response,
                       void* callback_param) {
    SyncWriteCallbackParam* param = (SyncWriteCallbackParam*)callback_param;
    delete request;
    delete response;
    param->cond_->Signal();
}
// Concurrence Put interface:
//      1. wait and lock
//      2. merge request while wait
//      3. unlock, write batch
//      4. for each raw write: PutIndex()
//      5. lock, resume other write
int TableImpl::Put(const StoreRequest* req, StoreResponse* resp,
                   StoreCallback callback, void* callback_param) {
    // try to construct Default callback
    Mutex mu;
    CondVar cond(&mu);
    SyncWriteCallbackParam param;
    param.cond_ = &cond;
    if (callback == NULL && !FLAGS_use_tera_async_write) {
        // write tera sync
        callback = SyncWriteCallback;
        callback_param = &param;
    }

    // construct WriteContext
    WriteContext* context = new WriteContext(&write_mutex_);
    StoreRequest* internal_req = (StoreRequest*)req;
    StoreResponse* internal_resp = resp;
    if (callback == NULL || callback == SyncWriteCallback) {
        // sync Put: sync or async write tera
        internal_req = new StoreRequest;
        internal_resp = new StoreResponse;
        *internal_req = *req;
        *internal_resp = *resp;
    }
    context->req_ = internal_req;
    context->resp_ = internal_resp;
    context->callback_ = callback;
    context->callback_param_ = callback_param;
    context->sync_ = true;
    context->is_wait_ = false;
    context->done_ = false;

    // internal batch help enhance single thread
    std::vector<WriteContext*> local_queue;
    if (SubmitRequest(context, &local_queue)) {
        return 0;
    }

    InternalBatchWrite(context, local_queue);
    // write finish, if sync write, just wait callback
    if (callback == SyncWriteCallback) {
        param.cond_->Wait();
    }
    delete context;
    return 0;
}

void ReleasePutContext(PutContext* context) {
    // the last one invoke user callback
    if (context->counter_.Dec() == 0) {
        if (context->callback_) {
            context->callback_(context->table_, (StoreRequest*)context->req_, context->resp_,
                context->callback_param_);
        } else {
            // callback is null, free req and resp
            VLOG(25) << "tera async write finish, delete req " << (uint64_t)context->req_
                << ", resp " << (uint64_t)context->resp_;
            delete context->req_;
            delete context->resp_;
        }
        VLOG(12) << "put callback";
        delete context;
    }
}

void PutCallback(tera::RowMutation* row) {
    ::tera::ErrorCode error = row->GetError();
    if (error.GetType() != ::tera::ErrorCode::kOK) {
        LOG(WARNING) << "write index error " << error.GetReason() << ", rowkey " << row->RowKey();
    }
    PutContext* context = (PutContext*)row->GetContext();
    ReleasePutContext(context);
    delete row;
}

int TableImpl::WriteIndexTable(const StoreRequest* req, StoreResponse* resp,
                               StoreCallback callback, void* callback_param,
                               FileLocation& location) {
    std::string null_value;
    null_value.clear();
    // index table include: primary table, index tables, ts table
    PutContext* context = new PutContext(this, req, resp, callback, callback_param);
    context->counter_.Set(2 + req->index_list.size());

    // sort indexes
    std::map<std::string, std::string> index_map;
    std::vector<Index>::const_iterator it;
    for (it = req->index_list.begin(); it != req->index_list.end(); ++it) {
        const std::string& index_name = it->index_name;
        const std::string& index_key = it->index_key;
        if (index_name == "" || index_key == "") {
            VLOG(30) << "invalid index : " << index_name << " : " << index_key;
            ReleasePutContext(context);
            continue;
        }
        index_map[index_name] = index_key;
    }

    // convert indexes to buffer
    std::string index_pack;
    std::map<std::string, std::string>::iterator map_it;
    for (map_it = index_map.begin(); map_it != index_map.end(); ++map_it) {
        const std::string& index_name = map_it->first;
        const std::string& index_key = map_it->second;
        std::string typed_index_key;
        StringToTypeString(index_name, index_key, &typed_index_key);
        uint32_t index_len = index_name.size() + 1 + typed_index_key.size();
        index_pack.append((char*)&index_len, sizeof(index_len));
        index_pack.append(index_name);
        index_pack.append(":");
        index_pack.append(typed_index_key);
    }
    // write small span into tera, tera value MUST in the last
    if (req->data.size() < (uint32_t)FLAGS_tera_span_size) {
        uint32_t value_len = kTeraValue.size() + 1 + req->data.size();
        index_pack.append((char*)&value_len, sizeof(value_len));
        index_pack.append(kTeraValue);
        index_pack.append(":");
        index_pack.append(req->data);
    }

    // update primary table
    std::string primary_key;
    StringToTypeString("pri", req->primary_key, &primary_key);
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    VLOG(12) << " write pri table, primary key: " << req->primary_key
        << ", typed_primary_key " << DebugString(primary_key);
    tera::RowMutation* primary_row = primary_table->NewRowMutation(primary_key);
    primary_row->Put(kPrimaryTableColumnFamily, location.SerializeToString(), req->timestamp, index_pack);
    primary_row->SetContext(context);
    primary_row->SetCallBack(PutCallback);
    primary_table->ApplyMutation(primary_row);

    // write index tables
    char buf[8];
    EncodeBigEndian(buf, req->timestamp);
    std::string ts_key(buf, sizeof(buf));
    std::string time_primay_key = ts_key + primary_key;
    for (map_it = index_map.begin(); map_it != index_map.end(); ++map_it) {
        const std::string& index_name = map_it->first;
        const std::string& index_key = map_it->second;
        tera::Table* index_table = GetIndexTable(index_name);
        if (index_table == NULL) {
            VLOG(30) << "write index table: " << index_name << ", no such table";
            ReleasePutContext(context);
            continue;
        }
        std::string typed_index_key;
        StringToTypeString(index_name, index_key, &typed_index_key);
        VLOG(12) << " write index table: " << index_name << ", index key: " << index_key
            << ", typed_index_key " << DebugString(typed_index_key);
        tera::RowMutation* index_row = index_table->NewRowMutation(typed_index_key);
        index_row->Put(kIndexTableColumnFamily, time_primay_key, req->timestamp, null_value);
        index_row->SetContext(context);
        index_row->SetCallBack(PutCallback);
        index_table->ApplyMutation(index_row);
    }

    // write timestamp table
    tera::Table* ts_table = GetTimestampTable();
    VLOG(12) << " write ts table: " << std::hex << ts_table << ", timestamp: "<< req->timestamp
             << ", ts string: " << DebugString(ts_key);
    tera::RowMutation* ts_row = ts_table->NewRowMutation(ts_key);
    ts_row->Put(kIndexTableColumnFamily, primary_key, req->timestamp, null_value);
    ts_row->SetContext(context);
    ts_row->SetCallBack(PutCallback);
    ts_table->ApplyMutation(ts_row);

    return 0;
}

void TableImpl::AsyncWriteBatchIndexTable(std::string primary_key, uint64_t timestamp,
                                          BatchIndexContext* context,
                                          BlockBuilder* index_builder,
                                          FileLocation location) {
    WriteBatchIndexTable(primary_key, timestamp, context, index_builder, location);
    delete index_builder;
}

void BatchIndexContext::PushBack(const StoreRequest* req, StoreResponse* resp,
                            StoreCallback callback, void* callback_param) {
    req_vec.push_back(req);
    resp_vec.push_back(resp);
    callback_vec.push_back(callback);
    param_vec.push_back(callback_param);
}

void BatchIndexContext::Release() {
    if (DecReference() == 0) {
        for (uint32_t i = 0; i < req_vec.size(); i++) {
            StoreCallback callback = callback_vec[i];
            if (callback) {
                VLOG(30) << "store callback, req " << (uint64_t)req_vec[i] << ", resp " << resp_vec[i];
                callback(table, (StoreRequest*)req_vec[i], resp_vec[i], param_vec[i]);
            } else {
                // callback is null, free req and resp
                delete req_vec[i];
                delete resp_vec[i];
            }
        }
        delete this;
    }
}

#define BatchWriteContext_ts_table (0)
void BatchIndexCallback(tera::RowMutation* row) {
    ::tera::ErrorCode error = row->GetError();
    BatchIndexContext* context = (BatchIndexContext*)row->GetContext();
    if (error.GetType() != ::tera::ErrorCode::kOK) {
        if (ksdk_last_warning_ts + 60000000 < timer::get_micros()) {
            ksdk_last_warning_ts = timer::get_micros();
            LOG(WARNING) << "write index error " << error.GetReason() << ", rowkey " << row->RowKey();
        }
        const ::tera::RowMutation::Mutation& mu = row->GetMutation(0);

        if (FLAGS_ignore_tera_write_error) {
            // ignore write error
            context->Release();
        } else {
            context->mu.Lock();
            // resend write req
            if (context->row_table_map[(uint64_t)(row)] == (uint64_t)BatchWriteContext_ts_table) {
                // timestamp table write error, scheduler to other timestamp table
                tera::Table* ts_table = context->table->GetTimestampTable();
                context->row_table_map.erase((uint64_t)(row));

                tera::RowMutation* ts_row = ts_table->NewRowMutation(row->RowKey());
                context->row_table_map[((uint64_t)(ts_row))] = (uint64_t)BatchWriteContext_ts_table;
                ts_row->Put(mu.family, mu.qualifier, mu.timestamp, mu.value);
                ts_row->SetContext(context);
                ts_row->SetCallBack(BatchIndexCallback);
                context->mu.Unlock();

                ts_table->ApplyMutation(ts_row);
            } else {
                tera::Table* index_table = (tera::Table*)(context->row_table_map[(uint64_t)(row)]);
                context->row_table_map.erase((uint64_t)(row));

                tera::RowMutation* index_row = index_table->NewRowMutation(row->RowKey());
                context->row_table_map[((uint64_t)(index_row))] = (uint64_t)(index_table);
                index_row->Put(mu.family, mu.qualifier, mu.timestamp, mu.value);
                index_row->SetContext(context);
                index_row->SetCallBack(BatchIndexCallback);
                context->mu.Unlock();

                index_table->ApplyMutation(index_row);
            }
        }
    } else {
        // write succ
        context->Release();
    }
    delete row;
}
int TableImpl::WriteBatchIndexTable(const std::string& primary_key, uint64_t timestamp,
                                    BatchIndexContext* context,
                                    BlockBuilder* index_builder,
                                    FileLocation& location) {
    std::string null_value;
    null_value.clear();
    ::leveldb::Slice index_block = index_builder->GetCompressionBlock();

    // update primary table
    std::string type_primary_key;
    StringToTypeString("pri", primary_key, &type_primary_key);
    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    VLOG(12) << " write pri table, primary key: " << primary_key
        << ", typed_primary_key " << DebugString(type_primary_key);
    tera::RowMutation* primary_row = primary_table->NewRowMutation(type_primary_key);
    context->row_table_map[((uint64_t)(primary_row))] = (uint64_t)(primary_table);
    primary_row->Put(kPrimaryTableColumnFamily, location.SerializeToString(), timestamp, index_block.ToString());
    primary_row->SetContext(context);
    primary_row->SetCallBack(BatchIndexCallback);
    primary_table->ApplyMutation(primary_row);

    // write index tables
    char buf[8];
    EncodeBigEndian(buf, timestamp);
    std::string ts_key(buf, sizeof(buf));
    std::string time_primay_key = ts_key + type_primary_key;

    ::leveldb::BlockContents block_contents;
    ::leveldb::ReadOptions read_options;
    read_options.verify_checksums = true;
    IndexBlock::ConstructBlockContents(index_block, &block_contents, read_options);
    IndexBlock iblock(block_contents, leveldb_options_);

    ::leveldb::Iterator* iter = iblock.NewIterator();
    iter->SeekToFirst();
    while (iter->Valid()) {
        // get user's index table(key, value)
        ::leveldb::Slice key = iter->key();
        ::leveldb::Slice value = iter->value();
        tera::Table* index_table = GetIndexTable(key.ToString());
        if (index_table == NULL) {
            context->Release();
            iter->Next();
            continue;
        }

        VLOG(12) << " write index table: " << key.ToString()
            << ", typed_index_key " << DebugString(value.ToString());
        tera::RowMutation* index_row = index_table->NewRowMutation(value.ToString());
        context->row_table_map[((uint64_t)(index_row))] = (uint64_t)(index_table);
        index_row->Put(kIndexTableColumnFamily, time_primay_key, timestamp, null_value);
        index_row->SetContext(context);
        index_row->SetCallBack(BatchIndexCallback);
        index_table->ApplyMutation(index_row);

        iter->Next();
    }
    delete iter;

    if (FLAGS_sdk_disable_tera_ts_write) {
        // disable write ts table, for seq write throughout
        context->Release();
    } else {
        // write timestamp table
        tera::Table* ts_table = GetTimestampTable();
        VLOG(12) << " write ts table: " << std::hex << ts_table << ", timestamp: "<< timestamp
            << ", ts string: " << DebugString(ts_key);
        tera::RowMutation* ts_row = ts_table->NewRowMutation(ts_key);
        context->row_table_map[((uint64_t)(ts_row))] = (uint64_t)BatchWriteContext_ts_table;
        ts_row->Put(kIndexTableColumnFamily, type_primary_key, timestamp, null_value);
        ts_row->SetContext(context);
        ts_row->SetCallBack(BatchIndexCallback);
        ts_table->ApplyMutation(ts_row);
    }

    return 0;

}

Status TableImpl::StringToTypeString(const std::string& index_table,
                                     const std::string& key,
                                     std::string* type_key) {
    // primary key
    if (index_table == "pri") {
        if (table_desc_.primary_key_type == ::mdt::kBytes) {
            *type_key = key;
            return Status::OK();
        } else if (table_desc_.primary_key_type == ::mdt::kUInt32 ||
                   table_desc_.primary_key_type == ::mdt::kUInt64 ||
                   table_desc_.primary_key_type == ::mdt::kInt32 ||
                   table_desc_.primary_key_type == ::mdt::kInt64) {
            uint64_t ikey = (uint64_t)atol(key.c_str());
            char buf[8];
            EncodeBigEndian(buf, ikey);
            std::string ikey_str(buf, sizeof(buf));
            *type_key = ikey_str;
            return Status::OK();
        } else {
            *type_key = key;
            return Status::OK();
        }
    } else {
        std::vector<IndexDescription>::iterator it = table_desc_.index_descriptor_list.begin();
        for (; it != table_desc_.index_descriptor_list.end(); ++it) {
            IndexDescription& desc = *it;
            if (desc.index_name == index_table) {
                if (desc.index_key_type == ::mdt::kBytes) {
                    *type_key = key;
                    return Status::OK();
                } else if (desc.index_key_type == ::mdt::kUInt32 ||
                           desc.index_key_type == ::mdt::kUInt64 ||
                           desc.index_key_type == ::mdt::kInt32 ||
                           desc.index_key_type == ::mdt::kInt64) {
                    uint64_t ikey = (uint64_t)atol(key.c_str());
                    char buf[8];
                    EncodeBigEndian(buf, ikey);
                    std::string ikey_str(buf, sizeof(buf));
                    *type_key = ikey_str;
                    return Status::OK();
                } else {
                    *type_key = key;
                    return Status::OK();
                }
            }
        }
    }
    *type_key = key;
    return Status::OK();
}

Status TableImpl::TypeStringToString(const std::string& index_table,
                                     const std::string& type_key,
                                     std::string* key) {
    if (index_table == "pri") {
        if (table_desc_.primary_key_type == ::mdt::kBytes) {
            *key = type_key;
            return Status::OK();
        } else if (table_desc_.primary_key_type == ::mdt::kUInt32 ||
                   table_desc_.primary_key_type == ::mdt::kUInt64 ||
                   table_desc_.primary_key_type == ::mdt::kInt32 ||
                   table_desc_.primary_key_type == ::mdt::kInt64) {
            uint64_t ikey = DecodeBigEndain(type_key.c_str());
            std::ostringstream convert;
            convert << ikey;
            *key = convert.str();
            return Status::OK();
        }
    } else {
        std::vector<IndexDescription>::iterator it = table_desc_.index_descriptor_list.begin();
        for (; it != table_desc_.index_descriptor_list.end(); ++it) {
            IndexDescription& desc = *it;
            if (desc.index_name == index_table) {
                if (desc.index_key_type == ::mdt::kBytes) {
                    *key = type_key;
                    return Status::OK();
                } else if (desc.index_key_type == ::mdt::kUInt32 ||
                           desc.index_key_type == ::mdt::kUInt64 ||
                           desc.index_key_type == ::mdt::kInt32 ||
                           desc.index_key_type == ::mdt::kInt64) {
                    uint64_t ikey = DecodeBigEndain(type_key.c_str());
                    std::ostringstream convert;
                    convert << ikey;
                    *key = convert.str();
                    return Status::OK();
                } else {
                    *key = type_key;
                    return Status::OK();
                }
            }
        }
    }
    *key = type_key;
    return Status::OK();
}

// search interface
Status TableImpl::Get(const SearchRequest* req, SearchResponse* resp, SearchCallback callback,
                      void* callback_param) {
    Status s;
    if (!req->primary_key.empty()) {
        // convert primary key
        std::string type_primary_key;
        StringToTypeString("pri", req->primary_key, &type_primary_key);
        GetByPrimaryKey(type_primary_key,
                        req->start_timestamp, req->end_timestamp,
                        &resp->result_stream);
    } else if (req->index_condition_list.size() > 0) {
        // convert index key
        std::vector<struct IndexCondition> index_condition_list = req->index_condition_list;
        std::vector<struct IndexCondition>::iterator it = index_condition_list.begin();
        for (; it != index_condition_list.end(); ++it) {
            IndexCondition& index = *it;
            std::string type_index_key;
            StringToTypeString(index.index_name, index.compare_value, &type_index_key);
            index.compare_value = type_index_key;
        }
        GetByIndex(index_condition_list, req->start_timestamp, req->end_timestamp,
                   req->limit, &resp->result_stream);
    } else {
        GetByTimestamp(req->start_timestamp, req->end_timestamp,
                       req->limit, &resp->result_stream);
    }

    if (resp->result_stream.size() == 0) {
        s = Status::NotFound("not found");
    } else {
        std::vector<ResultStream>& result_stream = resp->result_stream;
        std::vector<ResultStream>::iterator it = result_stream.begin();
        for (; it != result_stream.end(); ++it) {
            ResultStream& ret = *it;
            std::string primary_key_readable;
            TypeStringToString("pri", ret.primary_key, &primary_key_readable);
            ret.primary_key = primary_key_readable;
        }
    }
    return s;
}

// with timestamp
Status TableImpl::GetByPrimaryKey(const std::string& primary_key,
                                  int64_t start_timestamp, int64_t end_timestamp,
                                  std::vector<ResultStream>* result_list) {
    VLOG(10) << "get by primary key(with timestamp range): " << DebugString(primary_key);
    ResultStream result;
    Status s = GetSingleRow(primary_key, &result,
                            start_timestamp, end_timestamp,
                            NULL, NULL, NULL, NULL);
    if (s.ok()) {
        result_list->push_back(result);
    }
    return s;
}

Status TableImpl::GetByIndex(const std::vector<IndexCondition>& index_condition_list,
                             int64_t start_timestamp, int64_t end_timestamp, int32_t limit,
                             std::vector<ResultStream>* result_list) {
    VLOG(10) << "extend index conditions";
    std::vector<IndexConditionExtend> index_cond_ex_list;
    Status s = ExtendIndexCondition(index_condition_list, &index_cond_ex_list);

    if (s.ok()) {
        VLOG(10) << "get by extend index conditions";
        s = GetByExtendIndex(index_cond_ex_list, start_timestamp, end_timestamp,
                             limit, result_list);
    }

    return s;
}

Status TableImpl::GetByTimestamp(int64_t start_timestamp, int64_t end_timestamp,
                                 int32_t limit, std::vector<ResultStream>* result_list) {
    std::vector<tera::Table*> ts_table_list;
    GetAllTimestampTables(&ts_table_list);
    if (ts_table_list.size() == 0) {
        result_list->clear();
        return Status::NotFound("Timestamp Table not found");
    }

    char buf[8];
    EncodeBigEndian(buf, start_timestamp);
    std::string start_ts_key(buf, sizeof(buf));
    EncodeBigEndian(buf, end_timestamp);
    std::string end_ts_key(buf, sizeof(buf));
    VLOG(10) << "get by timestamp table: " << DebugString(start_ts_key) << " ~ "
             << DebugString(end_ts_key);
    // read trace row from ts table
    std::set<std::string> primary_key_set;
    for (size_t i = 0; (int32_t)result_list->size() < limit && i < ts_table_list.size(); i++) {
        tera::Table* ts_table = ts_table_list[i];
        tera::ScanDescriptor* scan_desc = new tera::ScanDescriptor(start_ts_key);
        scan_desc->SetEnd(end_ts_key + '\0');
        scan_desc->AddColumnFamily(kIndexTableColumnFamily);
	if (FLAGS_enable_number_limit) {
	    scan_desc->SetNumberLimit(FLAGS_scan_number_limit);
	}

        VLOG(10) << "scan timestamp table: " << i;
        tera::ErrorCode err;
        tera::ResultStream* result = ts_table->Scan(*scan_desc, &err);

        std::vector<std::string> primary_key_list;
        while ((int32_t)result_list->size() < limit && !result->Done(&err)) {
            const std::string& primary_key = result->Qualifier();
            VLOG(12) << "select op, primary key: " << primary_key;

            // if primary_key has not been read, read it.
            if (primary_key_set.find(primary_key) == primary_key_set.end()) {
                primary_key_set.insert(primary_key);
                primary_key_list.push_back(primary_key);
            }
            if ((int32_t)primary_key_list.size() >= limit) {
                GetRows(primary_key_list, limit - result_list->size(),
                        result_list);
                primary_key_list.clear();
            }
            result->Next();
        }
        if (primary_key_list.size() > 0) {
            //CHECK(result->Done(&err));
            CHECK_LT((int32_t)result_list->size(), limit);
            GetRows(primary_key_list, limit - result_list->size(),
                    result_list);
        }
        //delete result;
	cleaner_thread_->AddTask(boost::bind(&TableImpl::CleanerThread, this, result));
        delete scan_desc;
    }

    return Status::OK();
}

Status TableImpl::ExtendIndexCondition(const std::vector<IndexCondition>& index_condition_list,
                                       std::vector<IndexConditionExtend>* index_condition_ex_list) {
    std::map<std::string, IndexConditionExtend> dedup_map;
    std::vector<IndexCondition>::const_iterator it = index_condition_list.begin();
    for (; it != index_condition_list.end(); ++it) {
        const IndexCondition& index_cond = *it;
        std::map<std::string, IndexConditionExtend>::iterator ex_it;
        ex_it = dedup_map.find(index_cond.index_name);
        VLOG(10) << "sanity index : " << index_cond.index_name;
        if (ex_it == dedup_map.end()) {
            IndexConditionExtend& index_cond_ex = dedup_map[index_cond.index_name];
            index_cond_ex.index_name = index_cond.index_name;
            index_cond_ex.comparator = index_cond.comparator;
            index_cond_ex.compare_value1 = index_cond.compare_value;
            continue;
        }

        IndexConditionExtend& index_cond_ex = ex_it->second;
        if (index_cond_ex.comparator == (COMPARATOR)kBetween) {
            // invalid param
            return Status::InvalidArgument("");
        }

        if (index_cond.comparator == kLess || index_cond.comparator == kLessEqual) {
            if (index_cond_ex.comparator != kGreater && index_cond_ex.comparator != kGreaterEqual) {
                return Status::InvalidArgument("");
            }
            index_cond_ex.comparator = (COMPARATOR)kBetween;
            index_cond_ex.compare_value2 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond_ex.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond.comparator == kLessEqual);
        } else if (index_cond.comparator == kGreater || index_cond.comparator == kGreaterEqual) {
            if (index_cond_ex.comparator != kLess && index_cond_ex.comparator != kLessEqual) {
                return Status::InvalidArgument("");
            }
            index_cond_ex.comparator = (COMPARATOR)kBetween;
            index_cond_ex.compare_value2 = index_cond_ex.compare_value1;
            index_cond_ex.compare_value1 = index_cond.compare_value;
            index_cond_ex.flag1 = (index_cond.comparator == kGreaterEqual);
            index_cond_ex.flag2 = (index_cond_ex.comparator == kLessEqual);
        } else {
            // invalid param
            return Status::InvalidArgument("");
        }
    }

    std::map<std::string, IndexConditionExtend>::const_iterator ex_it = dedup_map.begin();
    for (; ex_it != dedup_map.end(); ++ex_it) {
        index_condition_ex_list->push_back(ex_it->second);
    }

    return Status::OK();
}

Status TableImpl::GetByExtendIndex(const std::vector<IndexConditionExtend>& index_condition_ex_list,
                                   int64_t start_timestamp, int64_t end_timestamp,
                                   int32_t limit, std::vector<ResultStream>* result_list) {
    size_t nr_index_table = index_condition_ex_list.size();
    if (nr_index_table == 0) {
        result_list->clear();
        return Status::OK();
    }

    std::vector<tera::Table*> index_table_vec(nr_index_table);
    std::vector<tera::ScanDescriptor*> scan_desc_vec(nr_index_table);
    std::vector<tera::ResultStream*> scan_stream_vec(nr_index_table);
    std::vector<std::vector<std::string> > pri_vec(nr_index_table);

    int valid_nr_index_table = 0;
    for (size_t i = 0; i < nr_index_table; i++) {
        bool skip_scan = false;
        const IndexConditionExtend& index_cond_ex = index_condition_ex_list[i];
        const std::string& index_name = index_cond_ex.index_name;
        tera::Table* index_table = GetIndexTable(index_name);
        if (index_table == NULL) {
            VLOG(12) << "index table " << index_name << " not exit";
            continue;
        }

        VLOG(10) << "select op, create scan stream of index: " << index_name;
        tera::ScanDescriptor* scan_desc = NULL;
        switch ((int)index_cond_ex.comparator) {
        case kEqualTo:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            scan_desc->SetEnd(index_cond_ex.compare_value1 + '\0');
            VLOG(30) << "select val = " << index_cond_ex.compare_value1;
            break;
        case kNotEqualTo:
            LOG(WARNING) << "Scan not support !=";
            skip_scan = true;
            break;
        case kLess:
            scan_desc = new tera::ScanDescriptor("");
            scan_desc->SetEnd(index_cond_ex.compare_value1);
            break;
        case kLessEqual:
            scan_desc = new tera::ScanDescriptor("");
            scan_desc->SetEnd(index_cond_ex.compare_value1 + '\0');
            break;
        case kGreater:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1 + '\0');
            scan_desc->SetEnd("");
            break;
        case kGreaterEqual:
            scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            scan_desc->SetEnd("");
            break;
        case kBetween:
            if (index_cond_ex.flag1) {
                scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1);
            } else {
                scan_desc = new tera::ScanDescriptor(index_cond_ex.compare_value1 + '\0');
            }
            if (index_cond_ex.flag2) {
                scan_desc->SetEnd(index_cond_ex.compare_value2 + '\0');
            } else {
                scan_desc->SetEnd(index_cond_ex.compare_value2);
            }
            break;
        default:
            LOG(WARNING) << "Scan: unknown cmp";
            skip_scan = true;
            break;
        }
        if (skip_scan) continue;

        scan_desc->AddColumnFamily(kIndexTableColumnFamily);
        scan_desc->SetTimeRange(end_timestamp, start_timestamp);
        if (FLAGS_enable_qu_range) {
            char sbuf[8], ebuf[8];
            EncodeBigEndian(sbuf, start_timestamp);
            EncodeBigEndian(ebuf, end_timestamp);
            std::string start_qu(sbuf, sizeof(sbuf));
            std::string end_qu(ebuf, sizeof(ebuf));
            scan_desc->AddQualifierRange(kIndexTableColumnFamily, start_qu, end_qu);
        }
	if (FLAGS_enable_number_limit) {
	    scan_desc->SetNumberLimit(FLAGS_scan_number_limit);
	}
        scan_desc->SetBufferSize(FLAGS_batch_scan_buffer_size);
        scan_desc->SetPackInterval(FLAGS_tera_scan_pack_interval);

        index_table_vec[valid_nr_index_table] = index_table;
        scan_desc_vec[valid_nr_index_table] = scan_desc;
        scan_stream_vec[valid_nr_index_table] = NULL;
        valid_nr_index_table++;
    }

    if (FLAGS_read_by_index_filter) {
        MultiIndexParam* multi_param = new MultiIndexParam;
        multi_param->limit = limit;
        multi_param->counter = 0;// num of primary data read totally
        multi_param->finish = false; // set true after any index table scanned done
        multi_param->ref = valid_nr_index_table;

        std::map<std::string, ResultStream> result_map;
        ThreadPool scan_threads(valid_nr_index_table);
        for (int i = 0; i < valid_nr_index_table; i++) {
            scan_threads.AddTask(boost::bind(&TableImpl::GetByFilterIndex, this,
                                             index_table_vec[i], scan_desc_vec[i],
                                             multi_param,
                                             &index_condition_ex_list, &result_map));
        }
        scan_threads.Stop(true);
        std::map<std::string, ResultStream>::iterator it = result_map.begin();
        for (; (int32_t)result_list->size() < limit && it != result_map.end(); ++it) {
            if (it->second.primary_key != "") {
                result_list->push_back(it->second);
            }
        }
    } else {
        while ((int32_t)result_list->size() < limit
               && ScanMultiIndexTables(&index_table_vec[0], &scan_desc_vec[0], &scan_stream_vec[0],
                                       &pri_vec[0], valid_nr_index_table, limit)) {
            // merge sort
            std::vector<std::string> primary_key_list;
            PrimaryKeyMergeSort(pri_vec, &primary_key_list);
            VLOG(10) << "select op, primary key merge sort: " << primary_key_list.size();
            GetRows(primary_key_list, limit - result_list->size(), result_list);
        }
    }

    for (int i = 0; i < valid_nr_index_table; i++) {
        delete scan_desc_vec[i];
        delete scan_stream_vec[i];
    }
    return Status::OK();
}

struct FilterIndexParam {
    MultiIndexParam* multi_param;
    int32_t pending_count;
    int32_t user_leave;
    CondVar cond;

    FilterIndexParam(MultiIndexParam* param)
        : multi_param(param), user_leave(0), cond(&(multi_param->mutex)) {}
};

void FilterIndexCallback(Status s, ResultStream* result, void* callback_param) {
    FilterIndexParam* param = (FilterIndexParam*)callback_param;
    MultiIndexParam* multi_param = param->multi_param;
    multi_param->mutex.Lock();
    if (s.ok()) {
        ++(multi_param->counter);
    }
    --(param->pending_count);
    // if user not leave:
    //  1. result enough should wakeup user
    //  2. last callback should wakeup user
    if (param->user_leave == 0) {
        if ((multi_param->counter >= multi_param->limit) || (param->pending_count == 1)) {
            param->cond.Signal();
        }
    } else {
        // if user leave, last callback should free param
        if (param->pending_count == 0) {
            delete param;

            // last index searcher free multi_param
            multi_param->ref--;
            if (multi_param->ref == 0) {
                multi_param->mutex.Unlock();
                delete multi_param;
                return;
            }
        }
    }
    multi_param->mutex.Unlock();
}

bool FilterIndexBreak(Status s, ResultStream* result, void* callback_param,
                      const std::string& data,
                      const std::string& primary_key) {
    FilterIndexParam* param = (FilterIndexParam*)callback_param;
    MultiIndexParam* multi_param = param->multi_param;
    MutexLock l(&multi_param->mutex);
    if (multi_param->counter >= multi_param->limit) {
        return true;
    }

    // enqueue data in lock
    if (s.ok() && (data.size() > 0)) {
        result->result_data_list.push_back(data);
    }
    if (s.ok() && (primary_key.size() > 0)) {
        result->primary_key = primary_key;
    }
    return false;
}

void TableImpl::GetByFilterIndex(tera::Table* index_table,
                                 tera::ScanDescriptor* scan_desc,
                                 MultiIndexParam* multi_param,
                                 const std::vector<IndexConditionExtend>* index_cond_list,
                                 std::map<std::string, ResultStream>* results) {
    tera::ErrorCode err;
    tera::ResultStream* stream = index_table->Scan(*scan_desc, &err);
    if (stream == NULL) {
        VLOG(5) << "new scan stream error\n";

        multi_param->mutex.Lock();
        // last index searcher free multi_param
        multi_param->ref--;
        if (multi_param->ref == 0) {
            multi_param->mutex.Unlock();
            delete multi_param;
            return;
        }
        multi_param->mutex.Unlock();
        return;
    }

    // filter by timestamp
    VLOG(10) << "begin scan index: " << index_table->GetName();
    FilterIndexParam* param = new FilterIndexParam(multi_param);
    param->pending_count = 1;
    while (!stream->Done(&err)) {
        // skip 8 bytes's ts
        std::string primary_key(stream->Qualifier(), 8, std::string::npos);
        {
            MutexLock l(&multi_param->mutex);
            /*
            if (FLAGS_enable_scan_control) {
                // :(,  compile optimistic may cause error
                if (pending_count >= limit) {
                    // if has limit getrow flying, wait for finish
                    VLOG(12) << "wait flying GetSingleRow finish, pending count " << pending_count
                        << ", limit " << limit;
                    while (pending_count > 0) {cond.Wait();}
                }
            }
            */
            if (multi_param->finish || multi_param->counter >= multi_param->limit) {
                break;
            }
            if (results->find(primary_key) != results->end()) {
                stream->Next();
                continue;
            }
            VLOG(12) << "select op, primary key: " << DebugString(primary_key);
            (*results)[primary_key].primary_key = ""; // mark as invalid
            param->pending_count++;
        }

        GetSingleRow(primary_key, &(*results)[primary_key],
                     0, (uint64_t)timer::get_micros(),
                     index_cond_list,
                     FilterIndexCallback, param,
                     FilterIndexBreak);

        stream->Next();
    }
    // batch scan must wait other rpc callback
    //delete stream;
    cleaner_thread_->AddTask(boost::bind(&TableImpl::CleanerThread, this, stream));

    // stop other index table scan streams
    {
        MutexLock l(&multi_param->mutex);
	if (multi_param->finish == false) {
       	    VLOG(10) << "finish scan index: " << index_table->GetName()
                 << ", notify other index table scan streams to stop";
	    multi_param->finish = true;
	}
    }

    // wait for background read
    VLOG(10) << "finish scan index: " << index_table->GetName()
             << ", wait for background read completion";
    {
        multi_param->mutex.Lock();
        // :(,  compile optimistic may cause error
	while (!((param->pending_count == 1) ||
               (multi_param->counter >= multi_param->limit))) {
            param->cond.Wait();
        }
    	VLOG(10) << "finish scan index: " << index_table->GetName()
                << ", all done, get data_num " << multi_param->counter;
        param->user_leave++;
        param->pending_count--;
        // if user leave, last callback should free param
        if (param->pending_count == 0) {
            delete param;

            // last index searcher free multi_param
            multi_param->ref--;
            if (multi_param->ref == 0) {
                multi_param->mutex.Unlock();
                delete multi_param;
                return;
            }
        }
        multi_param->mutex.Unlock();
    }
}

bool TableImpl::ScanMultiIndexTables(tera::Table** index_table_list,
                                     tera::ScanDescriptor** scan_desc_list,
                                     tera::ResultStream** scan_stream_list,
                                     std::vector<std::string>* primary_key_vec_list,
                                     uint32_t size, int32_t limit) {
    //CHECK_GT(size, 0U);
    if (size == 0) {
        VLOG(12) << "nr of index table " << size;
        return false;
    }

    VLOG(10) << "ScanMultiIndexTables index: " << index_table_list[0]->GetName() << ", size " << size;
    if (size == 1) {
        primary_key_vec_list[0].clear();
    }

    if (primary_key_vec_list[0].empty()) {
        scan_stream_list[0] = ScanIndexTable(index_table_list[0], scan_desc_list[0], scan_stream_list[0],
                                             limit, &primary_key_vec_list[0]);
        if (primary_key_vec_list[0].size() == 0) {
            return false;
        }
    }

    while (size > 1 && !ScanMultiIndexTables(index_table_list + 1, scan_desc_list + 1,
                                             scan_stream_list + 1, primary_key_vec_list + 1,
                                             size - 1, limit)) {
        CHECK_EQ(scan_stream_list[1], (void*)NULL);
        CHECK_EQ(primary_key_vec_list[1].size(), 0U);

        primary_key_vec_list[0].clear();
        scan_stream_list[0] = ScanIndexTable(index_table_list[0], scan_desc_list[0], scan_stream_list[0],
                                             limit, &primary_key_vec_list[0]);
        if (primary_key_vec_list[0].size() <= 0) {
            return false;
        }
    }

    return true;
}

tera::ResultStream* TableImpl::ScanIndexTable(tera::Table* index_table,
                                              tera::ScanDescriptor* scan_desc,
                                              tera::ResultStream* result, int32_t limit,
                                              std::vector<std::string>* primary_key_list) {
    tera::ErrorCode err;
    CHECK_EQ(primary_key_list->size(), 0U);
    VLOG(10) << "begin scan index: " << index_table->GetName();
    if (result == NULL) {
        result = index_table->Scan(*scan_desc, &err);
    }
    CHECK_NOTNULL(result);
    while (!result->Done(&err) && (int32_t)primary_key_list->size() < limit) {
        std::string primary_key(result->Qualifier(), 8, std::string::npos);
        primary_key_list->push_back(primary_key);
        VLOG(12) << "select op, primary key: " << primary_key;
        result->Next();
    }
    if (primary_key_list->size() == 0) {
        delete result;
        result = NULL;
    }
    VLOG(10) << "finish scan index: " << index_table->GetName() << ", get primary key num: "
             << primary_key_list->size();
    return result;
}

///////   fast merge sort algorithm   ////////
Status TableImpl::PrimaryKeyMergeSort(std::vector<std::vector<std::string> >& pri_vec,
                                      std::vector<std::string>* primary_key_list) {
    uint32_t nr_stream = pri_vec.size();
    if (nr_stream <= 0) {
        return Status::OK();
    }

    // collect iterator, min_key
    std::string min_key;
    std::vector<std::vector<std::string>::iterator > iter_vec(nr_stream);
    for (uint32_t i = 0; i < nr_stream; i++) {
        if (pri_vec[i].size() == 0) {
            LOG(INFO) << "no result in stream " << i;
            return Status::OK();
        }

        // sort primary_key stream, erase unique primary key
        std::string unique_key;
        std::sort(pri_vec[i].begin(), pri_vec[i].end());
        std::vector<std::string>::iterator unique_it = pri_vec[i].begin();
        unique_key = *unique_it;
        ++unique_it;
        while (unique_it != pri_vec[i].end()) {
            if ((*unique_it).compare(unique_key) == 0) {
                unique_it = pri_vec[i].erase(unique_it);
            } else {
                unique_key = *unique_it;
                ++unique_it;
            }
        }

        // get min_key in primary_key stream
        std::vector<std::string>::iterator it = pri_vec[i].begin();
        iter_vec[i] = it;
        if ((min_key.size() == 0) ||
            (min_key.compare((pri_vec[i])[0]) > 0)) {
            min_key = (pri_vec[i])[0];
        }
    }

    while (1) {
        std::string max_key = min_key;
        // collect min && max key in all stream
        for (uint32_t i = 0; i < nr_stream; i++) {
            if (min_key.compare(*(iter_vec[i])) == 0) {
                ++(iter_vec[i]);
            } else {
                // binary search
                iter_vec[i] = std::lower_bound(iter_vec[i], pri_vec[i].end(), min_key);
                if (iter_vec[i] == pri_vec[i].end()) {
                    // this stream finish, so exit
                    return Status::OK();
                }
                if (max_key.compare(*(iter_vec[i])) < 0) max_key = *(iter_vec[i]);
            }
        }

        // collect result
        if (min_key.compare(max_key) == 0) {
            LOG(INFO) << "merge pri key " << max_key;
            primary_key_list->push_back(max_key);
            // get next min_key
            if (iter_vec[0] == pri_vec[0].end()) {
                return Status::OK();
            }
            min_key = *(iter_vec[0]);
        } else {
            // drop invalid result, update min_key, use max_key enhance merge sort
            min_key = max_key;
        }
    }
    LOG(WARNING) << "merge sort error";
    return Status::OK();
}

// read data from primary table
struct GetRowParam {
    Status* status;
    Mutex* mutex;
    uint32_t* pending_count;
    CondVar* cond;
};

void GetRowCallback(Status s, ResultStream* result, void* callback_param) {
    GetRowParam* param = (GetRowParam*)callback_param;
    (*param->status) = s;
    MutexLock l(param->mutex);
    --(*param->pending_count);
    if ((*param->pending_count) == 0) {
        param->cond->Signal();
    }
    delete param;
}

int32_t TableImpl::GetRows(const std::vector<std::string>& primary_key_list, int32_t limit,
                           std::vector<ResultStream>* row_list) {
    std::vector<Status> status_list(primary_key_list.size());
    std::vector<ResultStream> tmp_row_list(primary_key_list.size());
    uint32_t pending_count = primary_key_list.size();
    Mutex mutex;
    CondVar cond(&mutex);

    for (uint32_t i = 0; i < primary_key_list.size(); i++) {
        GetRowParam* param = new GetRowParam;
        param->status = &status_list[i];
        param->pending_count = &pending_count;
        param->mutex = &mutex;
        param->cond = &cond;

        GetSingleRow(primary_key_list[i], &tmp_row_list[i],
                     0, (uint64_t)timer::get_micros(),
                     NULL, GetRowCallback, param, NULL);
    }

    MutexLock l(&mutex);
    while (pending_count > 0) {
        cond.Wait();
    }

    int32_t got_count = 0;
    for (uint32_t i = 0; got_count < limit && i < primary_key_list.size(); i++) {
        Status s = status_list[i];
        if (s.ok()) {
            VLOG(5) << "get primary data: " << primary_key_list[i];
            row_list->push_back(tmp_row_list[i]);
            got_count++;
        } else {
            LOG(WARNING) << "fail to get primary data: " << primary_key_list[i]
                << ", status: " << s.ToString();
        }
    }
    return got_count;
}

struct ReadPrimaryTableContext {
    TableImpl* table;
    std::vector<IndexConditionExtend> index_cond_list;
    void* user_callback;
    void* user_break_func; // break read primary table
    void* user_param;
    ResultStream* result; // user context

    // useful if user_callback == NULL
    Mutex* mutex;
    CondVar* cond;
    bool* finish;
};

void ReleaseReadPrimaryTableContext(ReadPrimaryTableContext* param, ResultStream* result, Status s) {
    if (param->user_callback != NULL) {
        ((GetSingleRowCallback*)param->user_callback)(s, result, param->user_param);
    } else {
        MutexLock l(param->mutex);
        (*param->finish) = true;
        param->cond->Signal();
    }
    delete param;
}

bool BreakOrPushData(ReadPrimaryTableContext* param, ResultStream* result, Status s,
                           const std::string& data, const std::string& primary_key) {
    bool should_break = false;
    if (param->user_break_func != NULL) {
        should_break = ((GetSingleRowBreak*)param->user_break_func)(s, result, param->user_param,
                                            data, primary_key);
    } else {
        // enqueue data in lock
        if (s.ok() && (data.size() > 0)) {
            result->result_data_list.push_back(data);
        }
        if (s.ok() && (primary_key.size() > 0)) {
            result->primary_key = primary_key;
        }
    }
    return should_break;
}

void TableImpl::ReadPrimaryTableCallback(tera::RowReader* reader) {
    ReadPrimaryTableContext* param = (ReadPrimaryTableContext*)reader->GetContext();
    const std::string& primary_key = reader->RowName();
    VLOG(12) << "finish read primary table: " <<  param->table->table_desc_.table_name
             << ", primary key: " << DebugString(primary_key);
    param->table->thread_pool_->AddTask(boost::bind(&TableImpl::ReadData, param->table, reader));
}

struct AsyncReadParam {
    ReadPrimaryTableContext* param;
    FileLocation* location;
    Mutex* lock;
    CondVar* cond;
    int* nr_record;
    Counter* nr_reader;
    std::string primary_key;
};
void TableImpl::ReadData(tera::RowReader* reader) {
    ReadPrimaryTableContext* param = (ReadPrimaryTableContext*)reader->GetContext();
    const std::string& primary_key = reader->RowName();
    bool should_break = false;

    // check break
    should_break = BreakOrPushData(param, param->result, Status::OK(), "", "");

    std::vector<std::pair<FileLocation, std::string> > value_locations;
    std::multimap<std::string, std::string> indexes;
    while (!reader->Done()) {
        // check break
        should_break = BreakOrPushData(param, param->result, Status::OK(), "", "");
        if (should_break) { break; }
        std::pair<FileLocation, std::string> item;

        const std::string& location_buffer = reader->Qualifier();
        FileLocation& location = item.first;
        location.ParseFromString(location_buffer);

        const std::string& index_buffer = reader->Value();
        item.second.clear();
        ParseIndexesFromString(index_buffer, &indexes, &(item.second));

        value_locations.push_back(item);

        reader->Next();
    }

    int nr_record = 0;
    VLOG(12) << "test indexes of primary key: " << primary_key;
    if (!should_break && (TestIndexCondition(param->index_cond_list, indexes))) {
        VLOG(12) << "read data of primary key: " << primary_key;
        // sync filesystem read
        if (!FLAGS_enable_async_read) {
            for (size_t i = 0; i < value_locations.size(); ++i) {
            	std::pair<FileLocation, std::string>& item = value_locations[i];
            	FileLocation& location = item.first;
                VLOG(12) << "begin to read data from " << location;
            	std::string data = item.second;
		// data in tera, get it, else get from filesystem
		if (data.size()) {
		    VLOG(12) << "finish read data from tera";
		    // check break
		    should_break = BreakOrPushData(param, param->result, Status::OK(), data, "");
		    if (should_break) { break; }
		    nr_record++;
		    continue;
		}

		// get data from filesystem
		Status s = param->table->ReadDataFromFile(location, &data);
                if (s.ok()) {
                    VLOG(12) << "finish read data from " << location;
                    // check break
                    ::leveldb::BlockContents block_contents;
                    ::leveldb::ReadOptions options;
                    options.verify_checksums = true;
                    ::leveldb::Status b_status = IndexBlock::ConstructBlockContents(::leveldb::Slice(data), &block_contents, options);
                    if (b_status.ok()) {
                        // parse compress index from primary table's value
                        IndexBlock data_block(block_contents, leveldb_options_);
                        ::leveldb::Iterator* iter = data_block.NewIterator();
                        iter->Seek(primary_key);
                        //std::string encoded_key;
                        //IndexBlock::EncodeInternalKey(primary_key, &encoded_key);
                        //while (iter->Valid() && (leveldb_options_.comparator->Compare(iter->key(), encoded_key) == 0)) {
                        while (iter->Valid() && (IndexBlock::UserComparator()->Compare(iter->key(), primary_key) == 0)) {
                            ::leveldb::Slice value = iter->value();
                            should_break = BreakOrPushData(param, param->result, Status::OK(), value.ToString(), "");
                            if (should_break) break;
                            nr_record++;
                            iter->Next();
                        }
                        delete iter;
                        if (should_break) break;
                    } else {
                        should_break = BreakOrPushData(param, param->result, Status::OK(), data, "");
                        if (should_break) { break; }
                        nr_record++;
                    }
                } else {
                    LOG(WARNING) << "fail to read data from " << location << " error: " << s.ToString();
                }
            }
        } else {
            // use async filesystem read
            Mutex lock;
            CondVar cond(&lock);
            Counter nr_reader;
            nr_reader.Set((uint64_t)(value_locations.size()) + 1);

            for (size_t i = 0; i < value_locations.size(); ++i) {
            	std::pair<FileLocation, std::string>& item = value_locations[i];
            	FileLocation& location = item.first;
            	std::string data = item.second;
		// data in tera, get it, else get from filesystem
		if (data.size()) {
		    VLOG(12) << "finish read data from tera";
		    // check break
		    should_break = BreakOrPushData(param, param->result, Status::OK(), data, "");
		    if (should_break) { break; }
		    nr_record++;
		    nr_reader.Dec();
		    continue;
		}

		// get data from filesystem
                AsyncReadParam* async_read_param = new AsyncReadParam;
                async_read_param->param = param;
                async_read_param->lock = &lock;
                async_read_param->cond = &cond;
                async_read_param->nr_record = &nr_record;
                async_read_param->nr_reader = &nr_reader;
                async_read_param->location = &location;
                async_read_param->primary_key = primary_key;
                async_read_thread_->AddTask(boost::bind(&TableImpl::AsyncRead, this, async_read_param));
            }
            {
                MutexLock l(&lock);
		nr_reader.Dec();
                VLOG(30) << "async read, wait file read finish";
                while (nr_reader.Get()) {
                    cond.Wait();
                }
            }
        }
    }

    Status s;
    if (should_break) {
        s = Status::NotFound("break from read data");
    } else if (reader->GetError().GetType() != tera::ErrorCode::kOK) {
        std::string reason = strerr(reader->GetError());
	VLOG(30) << "tera row reader error, primary key " << primary_key << ", errcode " << reason;
        s = Status::IOError("tera error");
    } else if (nr_record > 0) {
        BreakOrPushData(param, param->result, Status::OK(), "", primary_key);
        s = Status::OK();
    } else {
	LOG(WARNING) << "row not found, priamry key " << primary_key;
        s = Status::NotFound("row not found");
    }
    delete reader;

    // trigger user callback
    ReleaseReadPrimaryTableContext(param, param->result, s);
}

void TableImpl::AsyncRead(void* read_param) {
    AsyncReadParam* async_read_param = (AsyncReadParam*)read_param;
    ReadPrimaryTableContext* param = async_read_param->param;
    FileLocation* location = async_read_param->location;
    Mutex* lock = async_read_param->lock;
    CondVar* cond = async_read_param->cond;
    int* nr_record = async_read_param->nr_record;
    Counter* nr_reader = async_read_param->nr_reader;
    std::string primary_key = async_read_param->primary_key;
    delete async_read_param;

    // read from file system
    VLOG(12) << "begin to async read data from " << *location;
    std::string data;
    Status s = param->table->ReadDataFromFile(*location, &data);
    if (s.ok()) {
        VLOG(12) << "finish async read data from " << *location;

        ::leveldb::BlockContents block_contents;
        ::leveldb::ReadOptions options;
        options.verify_checksums = true;
        ::leveldb::Status b_status = IndexBlock::ConstructBlockContents(::leveldb::Slice(data), &block_contents, options);
        bool should_break = false;
        if (b_status.ok()) {
            // parse compress index from primary table's value
            IndexBlock data_block(block_contents, leveldb_options_);
            ::leveldb::Iterator* iter = data_block.NewIterator();
            iter->Seek(primary_key);
            //std::string encoded_key;
            //IndexBlock::EncodeInternalKey(primary_key, &encoded_key);
            //while (iter->Valid() && (leveldb_options_.comparator->Compare(iter->key(), encoded_key) == 0)) {
            while (iter->Valid() && (IndexBlock::UserComparator()->Compare(iter->key(), primary_key) == 0)) {
                ::leveldb::Slice value = iter->value();
                // check break
                MutexLock l(lock);
                should_break = BreakOrPushData(param, param->result, Status::OK(), value.ToString(), "");
                if (should_break) break;

                (*nr_record)++;
                iter->Next();
            }
            delete iter;
        } else {
            MutexLock l(lock);
            should_break = BreakOrPushData(param, param->result, Status::OK(), data, "");
            if (!should_break) {
                (*nr_record)++;
            }
        }
    } else {
        LOG(WARNING) << "fail to async read data from " << *location << " error: " << s.ToString();
    }
    {
        MutexLock l(lock);
        if (nr_reader->Dec() == 0) {
            cond->Signal();
        }
    }
}

void TableImpl::ParseIndexesFromString(const std::string& index_buffer,
                                       std::multimap<std::string, std::string>* indexes,
                                       std::string* value) {
    ::leveldb::BlockContents block_contents;
    ::leveldb::ReadOptions options;
    options.verify_checksums = true;
    ::leveldb::Status s = IndexBlock::ConstructBlockContents(::leveldb::Slice(index_buffer), &block_contents, options);
    if (s.ok()) {
        // parse compress index from primary table's value
        IndexBlock iblock(block_contents, leveldb_options_);
        ::leveldb::Iterator* iter = iblock.NewIterator();
        iter->SeekToFirst();
        while (iter->Valid()) {
            ::leveldb::Slice key = iter->key();
            ::leveldb::Slice value = iter->value();
            indexes->insert(std::pair<std::string, std::string>(key.ToString(), value.ToString()));
            iter->Next();
        }
        delete iter;
        return;
    }

    const char* buf = index_buffer.data();
    uint32_t left = index_buffer.size();
    while (left > sizeof(uint32_t)) {
        uint32_t index_len = *(uint32_t*)buf;
        buf += sizeof(uint32_t);
        left -= sizeof(uint32_t);
        if (index_len == 0 || index_len > left) {
            break;
        }

        const char* delim = buf;
        for (; delim < buf + index_len && *delim != ':'; delim++) { }
        if (delim <= buf || delim >= buf + index_len - 1) {
            break;
        }

        // NOTE: tera value must in the last item
        std::string index_name(buf, delim - buf);
        std::string index_key(delim + 1, buf + index_len - delim - 1);
        // try get data from tera
        if (index_name == kTeraValue) {
            *value = index_key;
        } else {
            indexes->insert(std::pair<std::string, std::string>(index_name, index_key));
        }
        buf += index_len;
        left -= index_len;
    }
}

bool TableImpl::TestIndexCondition(const std::vector<IndexConditionExtend>& index_cond_list,
                                   const std::multimap<std::string, std::string>& index_list) {
    for (size_t i = 0; i < index_cond_list.size(); i++) {
        const IndexConditionExtend& index_cond = index_cond_list[i];
        const std::string& index_cond_name = index_cond.index_name;

        bool match = false;
        std::multimap<std::string, std::string>::const_iterator it = index_list.begin();
        for (; it != index_list.end(); ++it) {
            const std::string& index_name = it->first;
            const std::string& index_key = it->second;

            if (index_cond_name == index_name) {
                switch ((int)index_cond.comparator) {
                case kEqualTo:
                    if (index_key == index_cond.compare_value1) {
                        match = true;
                    }
                    break;
                case kNotEqualTo:
                    LOG(WARNING) << "Scan not support !=";
                    break;
                case kLess:
                    if (index_key < index_cond.compare_value1) {
                        match = true;
                    }
                    break;
                case kLessEqual:
                    if (index_key <= index_cond.compare_value1) {
                        match = true;
                    }
                    break;
                case kGreater:
                    if (index_key > index_cond.compare_value1) {
                        match = true;
                    }
                    break;
                case kGreaterEqual:
                    if (index_key >= index_cond.compare_value1) {
                        match = true;
                    }
                    break;
                case kBetween:
                    if (((index_cond.flag1 && index_key >= index_cond.compare_value1)
                            || (!index_cond.flag1 && index_key > index_cond.compare_value1))
                        && ((index_cond.flag2 && index_key <= index_cond.compare_value2)
                            || (!index_cond.flag2 && index_key < index_cond.compare_value2))) {
                        match = true;
                    }
                    break;
                default:
                    LOG(WARNING) << "Scan: unknown cmp";
                    break;
                }

                if (match) {
                    break;
                }
            }
        }

        if (!match) {
            return false;
        }
    }
    return true;
}

Status TableImpl::GetSingleRow(const std::string& primary_key, ResultStream* result,
                               int64_t start_timestamp, int64_t end_timestamp,
                               const std::vector<IndexConditionExtend>* index_cond_list,
                               GetSingleRowCallback user_callback, void* user_param,
                               GetSingleRowBreak user_break_func) {
    Mutex mu;
    CondVar cond(&mu);
    bool finish = false;
    Status s;

    // init RowReader callback param
    ReadPrimaryTableContext* param = new ReadPrimaryTableContext;
    param->table = this;
    param->result = result;
    if (index_cond_list) {
        param->index_cond_list = *index_cond_list;
    }
    param->user_callback = (void*)user_callback;
    param->user_break_func = (void*)user_break_func;
    param->user_param = user_param;
    if (user_callback == NULL) {
        param->mutex = &mu;
        param->cond = &cond;
        param->finish = &finish;
    }

    tera::Table* primary_table = GetPrimaryTable(table_desc_.table_name);
    tera::RowReader* reader = primary_table->NewRowReader(primary_key);
    reader->AddColumnFamily(kPrimaryTableColumnFamily);
    // [default] not use timestamp filter to get  span in same row
    if (FLAGS_enable_multi_version_read)
        reader->SetTimeRange(start_timestamp, end_timestamp);
    reader->SetCallBack(ReadPrimaryTableCallback);
    reader->SetContext(param);

    VLOG(12) << "begin to read primary table: " <<  table_desc_.table_name
        << ", primary key: " << DebugString(primary_key);
    primary_table->Get(reader);

    if (user_callback == NULL) {
        MutexLock l(&mu);
        while (!finish) {
            cond.Wait();
        }
        if (result->result_data_list.size() == 0) {
            return Status::NotFound("row not found");
        }
    }
    return Status::OK();
}

Status TableImpl::ReadDataFromFile(const FileLocation& location, std::string* data) {
    RandomAccessFile* file = OpenFileForRead(location.fname_);
    if (file == NULL) {
        return Status::NotFound("file not found");
    }
    char* scratch = new char[location.size_];
    Slice result;
    Status s = file->Read(location.offset_, location.size_, &result, scratch);
    if (s.ok()) {
        data->assign(result.data(), result.size());
    }
    ReleaseDataReader(location.fname_);
    delete[] scratch;
    return s;
}

tera::Table* TableImpl::GetPrimaryTable(const std::string& table_name) {
    std::string index_table_name = tera_.table_prefix_ + "#pri#" + table_name;
    VLOG(12) << "get primary table: " << index_table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

tera::Table* TableImpl::GetIndexTable(const std::string& index_name) {
    std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + index_name;
    VLOG(12) << "get index table: " << index_table_name;
    std::map<std::string, tera::Table*>::iterator it = tera_.tera_table_map_.find(index_table_name);
    if (it != tera_.tera_table_map_.end()) {
        return it->second;
    } else {
        VLOG(12) << "not index table " << index_table_name;
        return NULL;
    }
}

tera::Table* TableImpl::GetTimestampTable() {
    MutexLock mu(&write_mutex_);
    cur_timestamp_table_id_ = (cur_timestamp_table_id_ + 1) % nr_timestamp_table_;
    char ts_name[32];
    snprintf(ts_name, sizeof(ts_name), "timestamp#%d", cur_timestamp_table_id_);
    std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + ts_name;
    VLOG(12) << "get index table: " << index_table_name;
    tera::Table* table = tera_.tera_table_map_[index_table_name];
    return table;
}

void TableImpl::GetAllTimestampTables(std::vector<tera::Table*>* table_list) {
    for (int i = 0; i < nr_timestamp_table_; i++) {
        char ts_name[32];
        snprintf(ts_name, sizeof(ts_name), "timestamp#%d", i);
        std::string index_table_name = tera_.table_prefix_ + "#" + table_desc_.table_name + "#" + ts_name;
        tera::Table* table = tera_.tera_table_map_[index_table_name];
        table_list->push_back(table);
    }
}

std::string TableImpl::TimeToString(struct timeval* filetime) {
#ifdef OS_LINUX
    pid_t tid = syscall(SYS_gettid);
#else
    pthread_t tid = pthread_self();
#endif
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));

    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[34];
    char* p = buf;
    p += snprintf(p, 34,
            "%04d-%02d-%02d-%02d:%02d:%02d.%06d.%06lu",
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec),
            (unsigned long)thread_id);
    std::string time_buf(buf, 33);
    *filetime = now_tv;
    return time_buf;
}

// cache file handler
RandomAccessFile* TableImpl::OpenFileForRead(const std::string& filename) {
    MutexLock l(&file_mutex_);

    // get file from cache
    std::map<std::string, DataReader>::iterator it = file_map_.find(filename);
    if (it != file_map_.end()) {
        DataReader& reader = it->second;
        // update lru
        std::map<uint64_t, std::string>::iterator lru_it = file_lru_.find(reader.seq_);
        if (lru_it != file_lru_.end()) {
            file_lru_.erase(lru_it);
        }
        uint64_t seq_no = seq_cnt_++;
        file_lru_[seq_no] = filename;

        reader.seq_ = seq_no;
        reader.ref_.Inc();
        VLOG(5) << "find file in cache: " << filename;
        return reader.file_;
    }

    // open file
    file_mutex_.Unlock();
    RandomAccessFile* file = NULL;
    Status s = fs_.env_->NewRandomAccessFile(filename, &file);
    file_mutex_.Lock();
    if (!s.ok()) {
        LOG(WARNING) << "fail to open file: " << filename << " error: " << s.ToString();
        return NULL;
    }

    // insert into cache
    VLOG(5) << "open file: " << filename;
    it = file_map_.find(filename);
    if (file_map_.find(filename) == file_map_.end()) {
        uint64_t seq_no = seq_cnt_++;
        DataReader reader;
        reader.seq_ = seq_no;
        reader.file_ = file;
        reader.ref_.Set(1); // init to be 1
        reader.ref_.Inc();

        file_map_[filename] = reader;
        // insert into lru list
        std::map<uint64_t, std::string>::iterator lru_it = file_lru_.find(reader.seq_);
        if (lru_it != file_lru_.end()) {
            file_lru_.erase(lru_it);
        }
        file_lru_[seq_no] = filename;

        // try to evict cache if the num of cache item > 1M
        if (file_lru_.size() > 1000000) {
            std::map<uint64_t, std::string>::iterator erase_it = file_lru_.begin();
            // delete top 1000 oldest cache item
            for (; erase_it != file_lru_.end(); ++ erase_it) {
                std::map<std::string, DataReader>::iterator file_map_tmp_it = file_map_.find(erase_it->second);
                if (file_map_tmp_it == file_map_.end()) {
                    LOG(ERROR) << "file " << erase_it->second << ", seq " << erase_it->first
                        << ", in lru, not in file map";
                    file_lru_.erase(erase_it);
                    break;
                }

                DataReader& erase_reader = file_map_tmp_it->second;
                if ((erase_reader.ref_.Get() == 0) || (erase_reader.ref_.Dec() == 0)) {
                    // do something cleanup
                    file_lru_.erase(erase_it);
                    delete erase_reader.file_;
                    file_map_.erase(file_map_tmp_it);
                    break;
                }
            }
        }

    } else {
        delete file;
        DataReader& reader = it->second;

        // update lru
        std::map<uint64_t, std::string>::iterator lru_it = file_lru_.find(reader.seq_);
        if (lru_it != file_lru_.end()) {
            file_lru_.erase(lru_it);
        }
        uint64_t seq_no = seq_cnt_++;
        file_lru_[seq_no] = filename;

        reader.seq_ = seq_no;
        reader.ref_.Inc();
        file = reader.file_;
    }
    return file;
}

void TableImpl::ReleaseDataReader(const std::string& filename) {
    MutexLock l(&file_mutex_);
    std::map<std::string, DataReader>::iterator it = file_map_.find(filename);
    if (it != file_map_.end()) {
        DataReader& reader = it->second;

        if ((reader.ref_.Get() == 0) && (reader.ref_.Dec() == 0)) {
            // do something cleanup
            std::map<uint64_t, std::string>::iterator lru_it = file_lru_.find(reader.seq_);
            if (lru_it != file_lru_.end()) {
                file_lru_.erase(lru_it);
            }
            delete reader.file_;
            file_map_.erase(it);
        }
    }
}

// gc impl
void* TableImpl::GarbageCleanThreadWrapper(void* arg) {
    reinterpret_cast<TableImpl*>(arg)->GarbageClean();
    return NULL;
}

void TableImpl::GarbageClean() {
    struct timeval dummyfiletime;
    std::string dummyfname = fs_.root_path_ + "/" + TimeToString(&dummyfiletime) + ".data";

    // random sleep
    struct timeval randtime;
    gettimeofday(&randtime, NULL);
    uint64_t sleep_duration = (randtime.tv_usec % 3600 + 1) * 1000000;
    usleep(sleep_duration);

    // enable gc
    while (1) {
        // handle gc per hour
        if (gc_stop_ == true) {
            break;
        }
        usleep(FLAGS_gc_interval);
        if (ttl_ == 0) {
            continue;
        }
        VLOG(50) << "path " << fs_.root_path_ << ", table ttl " << ttl_;
        std::vector<std::string> result;
        fs_.env_->GetChildren(fs_.root_path_, &result, NULL);

        for (uint64_t i = 0; i < result.size(); i++) {
            std::string filename = fs_.root_path_ + "/" + result[i];
            if (filename.size() == dummyfname.size()) {
                // ttl check , ttl + 1hour be should index data has been invalid
                struct timeval now_tv;
                gettimeofday(&now_tv, NULL);
                if ((uint64_t)now_tv.tv_sec < (uint64_t)(ttl_ + 3600)) {
                    continue;
                }

                const time_t seconds = now_tv.tv_sec - ttl_ - 3600;
                struct tm t;
                localtime_r(&seconds, &t);
                char buf[34];
                char* p = buf;
                p += snprintf(p, 34,
                        "%04d-%02d-%02d-%02d:%02d:%02d.%06d.%06lu",
                        t.tm_year + 1900,
                        t.tm_mon + 1,
                        t.tm_mday,
                        t.tm_hour,
                        t.tm_min,
                        t.tm_sec,
                        static_cast<int>(0),
                        (unsigned long)(0));
                std::string time_buf(buf, 33);
                std::string delete_file = fs_.root_path_ + "/" + time_buf + ".data";
                if (delete_file.size() != dummyfname.size()) {
                    LOG(INFO) << "Garbage Clean, error, max delete file " << delete_file << ", unkown";
                    continue;
                }

                if (filename < delete_file) {
                    VLOG(30) << "Garbage Clean, ttl " << ttl_ << ", file " << filename << ", delete " << delete_file;
                    fs_.env_->DeleteFile(filename);
                } else {
                    VLOG(50) << "Garbage Clean, not timeout, delete file " << delete_file << ", current file " << filename;
                }

            } else {
                LOG(INFO) << "Garbage Clean, unknow file " << filename;
                if (FLAGS_delete_unknow_file) {
                    fs_.env_->DeleteFile(filename);
                }
            }
        }
    }
}

// DataWriter Impl
DataWriter* TableImpl::GetDataWriter(WriteHandle* write_handle) {
    DataWriter* writer = NULL;
    struct timeval filetime;
    if (write_handle->writer_ && write_handle->writer_->SwitchDataFile()) {
        LOG(INFO) << "data file too large, switch";
        delete write_handle->writer_;
        write_handle->writer_ = NULL;
    }
    if (write_handle->writer_ == NULL) {
        Status s;
        std::string fname = fs_.root_path_ + "/" + TimeToString(&filetime) + ".data";
        WritableFile* file;
        s = fs_.env_->NewWritableFile(fname, &file);
        write_handle->writer_ = new DataWriter(fname, file);
        write_handle->writer_->SetFileTime(filetime);
	LOG(INFO) << "nowts " << timer::get_micros() << ", filetime " << write_handle->writer_->GetFileTime()
            << s.ToString() << "file null " << (file == NULL) << ", file " << fname;
    }
    writer = write_handle->writer_;
    return writer;
}

void TableImpl::ReleaseDataWriter(WriteHandle* write_handle) {
    // TODO
    if (write_handle->writer_) {
        delete write_handle->writer_;
    }
    write_handle->writer_ = NULL;
}

TableImpl::WriteHandle* TableImpl::GetWriteHandle() {
    MutexLock mu(&write_mutex_);

    if (cur_write_handle_seq_++ >= FLAGS_max_write_handle_seq) {
        cur_write_handle_seq_ = 0;
        cur_write_handle_id_ = (cur_write_handle_id_ + 1) % nr_write_handle_;
    }

    WriteHandle* write_handle = (WriteHandle*)&(write_handle_list_[cur_write_handle_id_]);
    LOG(INFO) << "id " << cur_write_handle_id_ << ", seq " << cur_write_handle_seq_
            << ", write_handle addr " << (uint64_t)write_handle;
    return write_handle;
}

// fail tolerant filesystem error, and small span write tera
int DataWriter::AddRecord(const std::string& data, FileLocation* location) {
    Status s;
    s = file_->Append(data);
    if (!s.ok()) {
        LOG(WARNING) << "fs not available, file " << fname_ << ", offset " << offset_;
        return -1;
    }
    location->size_ = data.size();
    location->offset_ = offset_;
    offset_ += location->size_;
    location->fname_ = fname_;
    // per 256KB, trigger sync
    assert(offset_ >= cur_sync_offset_);
    if ((offset_ - cur_sync_offset_) > (int32_t)FLAGS_data_size_per_sync) {
        file_->Sync();
        cur_sync_offset_ = offset_;
    }
    LOG(INFO) << "add record, offset " << location->offset_
        << ", size " << location->size_;
    return 0;
}

// version 2, write with compression
int DataWriter::AddCompressRecord(::leveldb::Slice data, FileLocation* location) {
    std::string data_batch;
    data_batch.append(data.ToString());
    data_batch.append(kVersion2);

    Status s;
    s = file_->Append(data_batch);
    if (!s.ok()) {
        return -1;
    }
    location->offset_ = offset_;
    location->size_ = data.size();
    offset_ += data_batch.size();
    location->fname_ = fname_;
    // per 256KB, trigger sync
    assert(offset_ >= cur_sync_offset_);
    if ((offset_ - cur_sync_offset_) > (int32_t)FLAGS_data_size_per_sync) {
        file_->Sync();
        cur_sync_offset_ = offset_;
    }
    LOG(INFO) << "add record, offset " << location->offset_
        << ", size " << location->size_;
    return 0;
}

// file > 1G or has been create 1 hour, create an new file
bool DataWriter::SwitchDataFile() {
    bool shouldswitch = offset_ > 1000000000;
    if (!shouldswitch) {
        int64_t nowts = timer::get_micros();
        int64_t filets = filetime_.tv_sec * 1000000 + filetime_.tv_usec;
        if (nowts > filets + FLAGS_switch_file_time) {
	    VLOG(30) << "time switch file, nowts " << nowts << ", filetime " << filets;
            shouldswitch = true;
        }
    }

    return shouldswitch;
}

// WriteBatch format
//      body := value(non fixed len) + red_zone(8 Bytes magic code)
int WriteBatch::Append(WriteContext* context) {
    context->offset_ = rep_.size();
    rep_.append(context->req_->data);
    rep_.append(kVersion1);

    context_list_.push_back(context);
    return 0;
}

} // namespace mdt
