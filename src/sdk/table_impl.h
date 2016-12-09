// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_TABLE_IMPL_H_
#define  MDT_SDK_TABLE_IMPL_H_

#include <deque>

#include <boost/asio.hpp>

#include "proto/kv.pb.h"
#include "utils/coding.h"
#include "utils/counter.h"
#include "utils/env.h"
#include "utils/mutex.h"
#include "utils/thread_pool.h"
#include "utils/sort_block.h"
#include "sdk/option.h"
#include "sdk/sdk.h"
#include "sdk/table.h"
#include <tera.h>


#include "leveldb/slice.h"

namespace mdt {

const std::string kPrimaryTableColumnFamily = "Location";
const std::string kIndexTableColumnFamily = "PrimaryKey";
const std::string kTeraValue = "kTeraValue";
const std::string kVersion1 = "aaccbbdd";
const std::string kVersion2 = "zzxxyyaa";

///////////////////////////////
//      TableImpl class      //
///////////////////////////////
// table in memory control structure
struct FileLocation {
    std::string fname_;
    uint32_t offset_;
    int32_t size_;

public:
    std::string SerializeToString() {
        char buf[8];
        char* p = buf;
        EncodeBigEndian32(p, offset_);
        EncodeBigEndian32(p + 4, size_);
        std::string offset_size(buf, 8);
        std::string s = fname_ + offset_size;
        return s;
    }
    void ParseFromString(const std::string& str) {
        fname_.assign(str.data(), str.size() - 8);
        offset_ = DecodeBigEndain32(str.data() + str.size() - 8);
        size_ = DecodeBigEndain32(str.data() + str.size() - 4);
    }
    friend std::ostream& operator << (std::ostream& o, const FileLocation& file_location);
};

struct TeraAdapter {
    std::string table_prefix_; // db_name
    TeraOptions opt_;
    std::map<std::string, tera::Table*> tera_table_map_; // <table_name, table desc>
};

struct WriteContext {
    // write param field
    const StoreRequest* req_;
    StoreResponse* resp_;
    StoreCallback callback_;
    void* callback_param_;

    // control field
    bool sync_;
    bool is_wait_;
    bool done_;
    CondVar cv_;

    // result field, WriteBatch response for it
    Status status_;
    uint32_t offset_; // offset in file

public:
    explicit WriteContext(Mutex* mu) : cv_(mu), offset_(0) {}
};

struct WriteBatch {
    // add red_zone magic code in the end of value
    int Append(WriteContext* context);
    //const std::string* GetData() { return &rep_;}

    std::vector<WriteContext*> context_list_;
    std::string rep_;
};

class DataWriter {
public:
    DataWriter(const std::string& fname, WritableFile* file)
        : fname_(fname), file_(file), offset_(0), cur_sync_offset_(0) {}
    ~DataWriter() {
        if(file_) {
            delete file_;
        }
    }
    void SetFileTime(struct timeval val) { filetime_ = val;}
    int64_t GetFileTime() { return (int64_t)(filetime_.tv_sec * 1000000 + filetime_.tv_usec);}
    bool SwitchDataFile();
    int AddRecord(const std::string& data, FileLocation* location);
    int AddCompressRecord(::leveldb::Slice data, FileLocation* location);

private:
    // write data to filesystem
    struct timeval filetime_;
    std::string fname_;
    WritableFile* file_;
    int32_t offset_; // TODO: no more than 4G per file
    int32_t cur_sync_offset_; // offset of data has been sync to datanode, default Sync per 256KB
};

struct FilesystemAdapter {
    std::string root_path_; // data file dir
    Env* env_;

};

enum COMPARATOR_EXTEND {
    kBetween = 100
};

struct IndexConditionExtend {
    std::string index_name;
    enum COMPARATOR comparator;
    std::string compare_value1;
    std::string compare_value2;
    bool flag1;
    bool flag2;
};

typedef void GetSingleRowCallback(Status s, ResultStream* result, void* callback_param);
typedef bool GetSingleRowBreak(Status s, ResultStream* result, void* callback_param,
                               const std::string& data, const std::string& primary_key);
struct MultiIndexParam {
    Mutex mutex;
    int32_t limit;
    int32_t counter;
    bool finish;
    int32_t ref;
};

class BatchIndexContext;
class TableImpl : public Table {
public:
    TableImpl(const TableDescription& table_desc,
              const TeraAdapter& tera_adapter,
              const FilesystemAdapter& fs_adapter);
    ~TableImpl();
    /*
    virtual int BatchWrite(std::vector<StoreRequest*> request, std::vector<StoreResponse*> response,
                           BatchWriteCallback callback = NULL, void* callback_param = NULL);
    */
    int BatchWrite(BatchWriteContext* ctx);
    virtual int Put(const StoreRequest* request, StoreResponse* response,
                    StoreCallback callback = NULL, void* callback_param = NULL);
    virtual Status Get(const SearchRequest* request, SearchResponse* response,
                       SearchCallback callback = NULL, void* callback_param = NULL);

    virtual const std::string& TableName() {return table_desc_.table_name;}
    virtual void Profile(TableProfile* profile);

    static Status OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                         const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                         Table** table_ptr);

    tera::Table* GetTimestampTable();

private:
    void BGInfoCollector();
    void FreeTeraTable();
    Status Init();
    void CleanerThread(tera::ResultStream* stream);

    // filesystem async reader
    void AsyncRead(void* async_read_param);

    // write op
    void AsyncWriteBatchIndexTable(std::string primary_key, uint64_t timestamp,
                                   BatchIndexContext* context,
                                   BlockBuilder* index_builder,
                                   FileLocation location);
    int InternalCompressBatchWrite(WriteContext* context, std::vector<WriteContext*>& ctx_queue);
    /*
    int WriteBatchIndexTable(const std::string& primary_key, uint64_t timestamp,
                             std::vector<WriteContext*>& vec_ctx,
                             BlockBuilder* index_builder,
                             FileLocation& location);
    */
    int WriteBatchIndexTable(const std::string& primary_key, uint64_t timestamp,
                             BatchIndexContext* context,
                             BlockBuilder* index_builder,
                             FileLocation& location);

    int InternalBatchWrite(WriteContext* context, std::vector<WriteContext*>& ctx_queue);
    static void* TimerThreadWrapper(void* arg);
    void QueueTimerFunc();
    void GetAllRequest(WriteContext** context_ptr, std::vector<WriteContext*>* local_queue);
    bool SubmitRequest(WriteContext* context, std::vector<WriteContext*>* local_queue);
    Status GetByPrimaryKey(const std::string& primary_key,
                           int64_t start_timestamp, int64_t end_timestamp,
                           std::vector<ResultStream>* result_list);

    Status GetByIndex(const std::vector<IndexCondition>& index_condition_list,
                      int64_t start_timestamp, int64_t end_timestamp, int32_t limit,
                      std::vector<ResultStream>* result_list);

    Status GetByTimestamp(int64_t start_timestamp, int64_t end_timestamp,
                          int32_t limit, std::vector<ResultStream>* result_list);

    Status ExtendIndexCondition(const std::vector<IndexCondition>& index_condition_list,
                                std::vector<IndexConditionExtend>* index_condition_ex_list);

    Status GetByExtendIndex(const std::vector<IndexConditionExtend>& index_condition_ex_list,
                            int64_t start_timestamp, int64_t end_timestamp,
                            int32_t limit, std::vector<ResultStream>* result_list);

    void GetByFilterIndex(tera::Table* index_table,
                          tera::ScanDescriptor* scan_desc,
                          MultiIndexParam* multi_param,
                          const std::vector<IndexConditionExtend>* index_cond_list,
                          std::map<std::string, ResultStream>* results);

    bool ScanMultiIndexTables(tera::Table** index_table_list,
                              tera::ScanDescriptor** scan_desc_list,
                              tera::ResultStream** scan_stream_list,
                              std::vector<std::string>* primary_key_vec_list,
                              uint32_t size, int32_t limit);

    tera::ResultStream* ScanIndexTable(tera::Table* index_table,
                                       tera::ScanDescriptor* scan_desc,
                                       tera::ResultStream* scan_stream, int32_t limit,
                                       std::vector<std::string>* primary_key_list);

    int32_t GetRows(const std::vector<std::string>& primary_key_list, int32_t limit,
                    std::vector<ResultStream>* row_list);

    static void ReadPrimaryTableCallback(tera::RowReader* reader);

    void ReadData(tera::RowReader* reader);

    Status GetSingleRow(const std::string& primary_key, ResultStream* result,
                        int64_t start_timestamp = 0, int64_t end_timestamp = 0,
                        const std::vector<IndexConditionExtend>* index_cond_list = NULL,
                        GetSingleRowCallback callback = NULL, void* callback_param = NULL,
                        GetSingleRowBreak break_func = NULL);

    Status ReadDataFromFile(const FileLocation& location, std::string* data);

    RandomAccessFile* OpenFileForRead(const std::string& filename) ;

    int WriteIndexTable(const StoreRequest* req, StoreResponse* resp,
                        StoreCallback callback, void* callback_param,
                        FileLocation& location);

    Status PrimaryKeyMergeSort(std::vector<std::vector<std::string> >& pri_vec,
                               std::vector<std::string>* primary_key_list);
    tera::Table* GetPrimaryTable(const std::string& table_name);
    tera::Table* GetIndexTable(const std::string& index_name);
    void GetAllTimestampTables(std::vector<tera::Table*>* table_list);
    std::string TimeToString(struct timeval* filetime);
    void ParseIndexesFromString(const std::string& index_buffer,
                                std::multimap<std::string, std::string>* indexes,
                                std::string* value);
    bool TestIndexCondition(const std::vector<IndexConditionExtend>& index_cond_list,
                            const std::multimap<std::string, std::string>& index_list);

    Status StringToTypeString(const std::string& index_table,
                              const std::string& key,
                              std::string* type_key);

    Status TypeStringToString(const std::string& index_table,
                              const std::string& type_key,
                              std::string* key);

    // gc impl
    static void* GarbageCleanThreadWrapper(void* arg);
    void GarbageClean();

    bool GetIndexShuttle(const std::string& index);

private:
    // NOTE： WriteHandle can not operator in race condition
    struct WriteHandle {
        std::deque<WriteContext*> write_queue_;
        DataWriter* writer_;
    };
    WriteHandle* GetWriteHandle();
    DataWriter* GetDataWriter(WriteHandle* write_handle);
    void ReleaseDataWriter(WriteHandle* write_handle);

    struct DataReader {
        RandomAccessFile* file_;
        uint64_t seq_;
        Counter ref_;
    };
    void ReleaseDataReader(const std::string& filename);

private:
    const ::leveldb::InternalKeyComparator internal_comparator_;
    ::leveldb::Options leveldb_options_;

    TableDescription table_desc_;
    TeraAdapter tera_;
    FilesystemAdapter fs_;
    ThreadPool* thread_pool_;
    // async cleaner
    ThreadPool* cleaner_thread_;
    ThreadPool* async_read_thread_;

    // file handle cache relative
    mutable Mutex file_mutex_;
    std::map<std::string, DataReader> file_map_;
    std::map<uint64_t, std::string> file_lru_; // cache file handle for read. <seq_, filename>
    uint64_t seq_cnt_; // seq number generator


    // use for put
    mutable Mutex write_mutex_;
    std::vector<WriteHandle> write_handle_list_;
    int nr_write_handle_;
    int cur_write_handle_id_; // current selected write_handle
    int cur_write_handle_seq_; // num of request schedule to current write_handle
    int nr_timestamp_table_; // const
    int cur_timestamp_table_id_;
    // support batch write
    std::vector<WriteContext*> batch_queue_;
    mutable Mutex queue_mutex_;
    // queue timer
    bool queue_timer_stop_;
    pthread_t timer_tid_;
    mutable Mutex queue_timer_mu_; // mutex must declare before cv
    CondVar queue_timer_cv_;


    // garbage clean, delete nfs file with ttl
    pthread_t gc_tid_;
    volatile bool gc_stop_;
    uint64_t ttl_;

    // async tera write
    ThreadPool* async_tera_writer_;

    // statictis info dump
    ThreadPool* info_collector_thread_;
};

class BatchIndexContext {
public:
    BatchIndexContext() {}
    ~BatchIndexContext() {}
    int64_t SetReference(uint64_t ref) {
        return ref_.Set(ref);
    }
    void SetTable(TableImpl* table_impl) {
        table = table_impl;
    }
    void PushBack(const StoreRequest* req, StoreResponse* resp,
                  StoreCallback callback, void* callback_param);
    void Release();

private:
    int64_t DecReference() {
        return ref_.Dec();
    }

public:
    Mutex mu;
    std::map<uint64_t, uint64_t> row_table_map;
    TableImpl* table;

private:
    Counter ref_;
    std::vector<const StoreRequest*> req_vec;
    std::vector<StoreResponse*> resp_vec;
    std::vector<StoreCallback> callback_vec;
    std::vector<void*> param_vec;
};

struct PutContext {
    TableImpl* table_;
    const StoreRequest* req_;
    StoreResponse* resp_;
    StoreCallback callback_;
    void* callback_param_;
    Counter counter_; // atomic counter

    PutContext(TableImpl* table,
               const StoreRequest* request,
               StoreResponse* response,
               StoreCallback callback = NULL,
               void* callback_param = NULL)
        : table_(table), req_(request),
        resp_(response), callback_(callback),
        callback_param_(callback_param) {}
};

struct GetContext {
    TableImpl* table_;
    const SearchRequest* req_;
    SearchResponse* resp_;
    SearchCallback callback_;
    Counter counter_; // atomic counter

    GetContext(TableImpl* table,
               const SearchRequest* request,
               SearchResponse* response,
               SearchCallback callback)
        : table_(table), req_(request),
        resp_(response), callback_(callback) {}
};

} // namespace mdt

#endif  // MDT_SDK_TABLE_IMPL_H_
