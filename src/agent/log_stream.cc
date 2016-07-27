// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "agent/log_stream.h"

#include <dirent.h>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <uuid/uuid.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/foreach.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/regex.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "agent/options.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "proto/query.pb.h"
#include "proto/scheduler.pb.h"
#include "utils/coding.h"

DECLARE_string(agent_service_port);
DECLARE_string(scheduler_addr);
DECLARE_int32(file_stream_max_pending_request);
DECLARE_string(db_name);
DECLARE_string(table_name);
DECLARE_string(primary_key);
DECLARE_string(user_time);
DECLARE_int32(time_type);
// support line filter
DECLARE_string(string_line_filter_list);
// split string by substring
DECLARE_string(string_delims);
// split string by char
DECLARE_string(line_delims);
DECLARE_string(kv_delims);
DECLARE_bool(enable_index_filter);
DECLARE_string(index_list);
DECLARE_string(alias_index_list);

DECLARE_bool(use_fixed_index_list);
DECLARE_string(fixed_index_list);

DECLARE_int64(delay_retry_time);
DECLARE_int64(agent_max_fd_num);

DECLARE_bool(use_regex_index_pattern);
DECLARE_bool(enable_reclaim_ino);

namespace mdt {
namespace agent {

/*
 *      leveldb's data: MagicYoYo1989, CurrentOffset, checkpoint
 *      MagicYoYo1989=db+ino, path
 *      CurrentOffset=db+ino, offset
 *      CheckPoint=db+ino+offset, size
 */

static int64_t kLastLogWarningTime;
static uint64_t kReadBlockSize = 1048576;

void* LogStreamWrapper(void* arg) {
    LogStream* stream = (LogStream*)arg;
    stream->Run();
    return NULL;
}

LogStream::LogStream(std::string module_name, LogOptions log_options,
                     RpcClient* rpc_client, pthread_spinlock_t* server_addr_lock,
                     AgentInfo* info)
    : module_name_(module_name),
    log_options_(log_options),
    rpc_client_(rpc_client),
    server_addr_lock_(server_addr_lock),
    info_(info),
    //server_addr_(server_addr),
    stop_(false),
    fail_delay_thread_(1) {
    total_stream_offset_ = 0;
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&monitor_lock_, PTHREAD_PROCESS_PRIVATE);
    db_name_ = FLAGS_db_name;
    table_name_ = FLAGS_table_name;

    kLastLogWarningTime = timer::get_micros();

    char hostname[255];
    if (0 != gethostname(hostname, 256)) {
        LOG(FATAL) << "fail to report message";
    }
    std::string hostname_str = hostname;
    hostname_ = hostname_str + ":" + FLAGS_agent_service_port;

    // support line filter
    if (FLAGS_string_line_filter_list.size() > 0) {
        boost::split(string_filter_, FLAGS_string_line_filter_list, boost::is_any_of(","));
    }

    // split alias index
    //std::map<std::string, std::string> alias_index_map;
    if (FLAGS_alias_index_list.size() != 0) {
        std::vector<std::string> alias_index_vec;
        boost::split(alias_index_vec, FLAGS_alias_index_list, boost::is_any_of(";"));
        VLOG(30) << "DEBUG: split alias index tablet\n";
        for (int i = 0; i < (int)alias_index_vec.size(); i++) {
            std::vector<std::string> alias_vec;
            boost::split(alias_vec, alias_index_vec[i], boost::is_any_of(":"));
            if ((alias_vec.size() >= 2) && alias_vec[1].size()) {
                std::vector<std::string> alias;
                boost::split(alias, alias_vec[1], boost::is_any_of(","));
                alias_index_map_.insert(std::pair<std::string, std::string>(alias_vec[0], alias_vec[0]));
                VLOG(30) << "=====> index: " << alias_vec[0] << std::endl;
                for (int j = 0; j < (int)alias.size(); j++) {
                    alias_index_map_.insert(std::pair<std::string, std::string>(alias[j], alias_vec[0]));
                    VLOG(30) << "parse alias list: " << alias[j] << std::endl;
                }
            }
        }
    }

    // split string delims
    std::vector<std::string> string_delims;
    if (FLAGS_string_delims.size() != 0) {
        boost::split(string_delims, FLAGS_string_delims, boost::is_any_of(","));
        VLOG(30) << "DEBUG: get string delims";
        for (int i = 0; i < (int)string_delims.size(); i++) {
            string_delims_.push_back(string_delims[i]);
        }
    }

    // split fixed index list
    use_fixed_index_list_ = FLAGS_use_fixed_index_list;
    std::vector<std::string> fixed_index_list;
    if (FLAGS_fixed_index_list.size() != 0) {
        // --fixed_index_list=url:5,time:2
        boost::split(fixed_index_list, FLAGS_fixed_index_list, boost::is_any_of(","));
        VLOG(30) << "DEBUG: split fixed index table";
        for (int i = 0 ; i < (int)fixed_index_list.size(); i++) {
            std::vector<std::string> idx_pair;
            boost::split(idx_pair, fixed_index_list[i], boost::is_any_of(":"));
            if ((idx_pair.size() == 2) &&
                (idx_pair[0].size() > 0) &&
                (idx_pair[1].size() > 0)) {
                int idx_num = atoi(idx_pair[1].c_str());
                fixed_index_list_.insert(std::pair<std::string, int>(idx_pair[0], idx_num));
            }
        }
    }

    line_delims_ = FLAGS_line_delims;
    kv_delims_ = FLAGS_kv_delims;
    enable_index_filter_ = FLAGS_enable_index_filter;
    // split index
    std::vector<std::string> log_columns;
    if (FLAGS_index_list.size() != 0) {
        boost::split(log_columns, FLAGS_index_list, boost::is_any_of(","));
        VLOG(30) << "DEBUG: split index table";
        for (int i = 0 ; i < (int)log_columns.size(); i++) {
            alias_index_map_.insert(std::pair<std::string, std::string>(log_columns[i], log_columns[i]));
            index_list_.insert(log_columns[i]);
        }
    }

    primary_key_ = FLAGS_primary_key;
    user_time_ = FLAGS_user_time;
    time_type_ = FLAGS_time_type;

    last_ino_check_ts_ = mdt::timer::get_micros();
    last_update_time_ = mdt::timer::get_micros();
    pthread_create(&tid_, NULL, LogStreamWrapper, this);

    // recovery write event
    std::vector<std::pair<std::string, uint64_t> > event_vec;
    RecoverWriteEvent(&event_vec);
    for (uint32_t vec_i = 0; vec_i < event_vec.size(); vec_i++) {
        AddWriteEvent(event_vec[vec_i].first, event_vec[vec_i].second);
    }
}

LogStream::~LogStream() {
    stop_ = true;
    thread_event_.Set();
    pthread_join(tid_, NULL);
    //pthread_spin_destroy(&lock_);
}

void LogStream::GetTableName(std::string file_name, std::string* table_name) {
    uint64_t max_len = 0;
    std::set<std::string>::iterator it = log_name_prefix_.begin();
    for (; it != log_name_prefix_.end(); ++it) {
        const std::string& log_name = *it;
        if ((file_name.find(log_name) != std::string::npos) &&
            (max_len < log_name.size())) {
            *table_name = log_name;
            max_len = log_name.size();
        }
    }
    if (max_len == 0) {
        *table_name = "trash";
    }
    return;
}

void LogStream::EncodeUint64BigEndian(uint64_t value, std::string* str) {
    char offset_buf[8];
    EncodeBigEndian(offset_buf, value);
    std::string offset_str(offset_buf, 8);
    *str = offset_str;
}
// key=MagicYoYo1989'\0'dbname'\0'ino, value=tablename
void LogStream::DumpWriteEvent(const std::string& filename, uint64_t ino) {
    std::string key, value;
    key = "MagicYoYo1989";
    key.push_back('\0');
    key+= module_name_;
    key.push_back('\0');

    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    key += ino_str;

    value = filename;

    std::string value1;
    leveldb::Status s;
    s = log_options_.db->Get(leveldb::ReadOptions(), key, &value1);
    if (!s.ok()) {
        VLOG(30) << "dump write event, key " << key << ", value " << value << ", ino " << ino;
        s = log_options_.db->Put(leveldb::WriteOptions(), key, value);
    } else if (value != value1) {
        VLOG(30) << "dump write event, key " << key << ", value " << value << ", ino " << ino;
        s = log_options_.db->Put(leveldb::WriteOptions(), key, value);
    }
    if (!s.ok()) {
        // reopen leveldb, flush error
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        s = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!s.ok()) {
            LOG(WARNING) << "DumpWriteEvent, re-open error";
        }
    }
    return;
}
// vec[(fname, ino)]
void LogStream::RecoverWriteEvent(std::vector<std::pair<std::string, uint64_t> >* event_vec) {
    std::string key, value;
    key = "MagicYoYo1989";
    key.push_back('\0');
    key+= module_name_;
    key.push_back('\0');

    leveldb::Iterator* db_it = log_options_.db->NewIterator(leveldb::ReadOptions());

    std::string startkey, endkey;
    startkey = key;
    std::string start_ino_str;
    EncodeUint64BigEndian((uint64_t)0UL, &start_ino_str);
    startkey += start_ino_str;

    endkey = key;
    std::string end_ino_str;
    EncodeUint64BigEndian(0xffffffffffffffff, &end_ino_str);
    endkey += end_ino_str;

    for (db_it->Seek(startkey);
         db_it->Valid() && db_it->key().ToString() < endkey;
         db_it->Next()) {
        leveldb::Slice lkey = db_it->key();
        leveldb::Slice lvalue = db_it->value();
        uint64_t tmp_ino;
        const std::string& fname = lvalue.ToString();

        leveldb::Slice ino_str = leveldb::Slice(lkey.data() + key.size(), 8);
        tmp_ino = DecodeBigEndain(ino_str.data());
        if (tmp_ino != 0 || tmp_ino != 0xffffffffffffffff) {
            event_vec->push_back(std::pair<std::string, uint64_t>(fname, tmp_ino));
        }
    }
    delete db_it;
}
void LogStream::EraseWriteEvent(const std::string& filename, uint64_t ino, std::string* key) {
    *key = "MagicYoYo1989";
    key->push_back('\0');
    *key += module_name_;
    key->push_back('\0');

    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    //value = filename;
#if 0
    leveldb::Status s;
    VLOG(30) << "Magic Table delete: ino " << ino << ", filename " << filename;
    s = log_options_.db->Delete(leveldb::WriteOptions(), key);
    if (!s.ok()) {
        // reopen leveldb, flush error
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        s = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!s.ok()) {
            LOG(WARNING) << "EraseWriteEvent, re-open error";
        }
    }
#endif
    return;
}

template<class T>
void ToString(std::string* str, const T& val) {
    std::ostringstream ss;
    ss << val;
    *str = ss.str();
}

void LogStream::Run() {
    while (1) {
        bool has_event = false;
        std::map<uint64_t, std::string> local_write_event;
        std::map<uint64_t, std::string> local_delete_event;
        std::queue<DBKey*> local_key_queue;
        std::queue<DBKey*> local_failed_key_queue;
        std::map<uint64_t, uint64_t> local_ctrl_event;

        pthread_spin_lock(&lock_);
        VLOG(30) << "event: write " << write_event_.size() << ", delete " << delete_event_.size()
            << ", success " << key_queue_.size() << ", fail " << failed_key_queue_.size();
        if (write_event_.size()) {
            swap(local_write_event,write_event_);
            has_event = true;
        }
        if (delete_event_.size()) {
            swap(local_delete_event, delete_event_);
            has_event = true;
        }
        if (key_queue_.size()) {
            swap(key_queue_, local_key_queue);
            has_event = true;
        }
        if (failed_key_queue_.size()) {
            swap(failed_key_queue_, local_failed_key_queue);
            has_event = true;
        }
        if (ctrl_event_.size()) {
            swap(ctrl_event_, local_ctrl_event);
            has_event = true;
        }
        pthread_spin_unlock(&lock_);

        if (!has_event) {
            thread_event_.Wait();
        }
        if (stop_ == true) {
            break;
        }

        int64_t start_ts = mdt::timer::get_micros();
        // handle push callback event
        while (!local_key_queue.empty()) {
            DBKey* key = local_key_queue.front();
            std::map<uint64_t, FileStream*>::iterator succ_file_it = file_streams_.find(key->ino);
            FileStream* file_stream = NULL;
            if (succ_file_it != file_streams_.end()) {
                file_stream = succ_file_it->second;

                file_stream->kreq_success.Inc();
            }
            local_key_queue.pop();

            // last one delete and free space
            if (key->ref.Dec() == 0) {
                if (file_stream) {
                    file_stream->DeleteCheckoutPoint(key);
                }
                VLOG(30) << "delete key, file " << key->filename << ", key " << (uint64_t)key << ", offset " << key->offset
                    << ", ino " << key->ino;
                delete key;
            }
        }
        int64_t handle_suc_ts = mdt::timer::get_micros();
        if (handle_suc_ts > start_ts + 10000) {
            VLOG(30) << "handle succ timestamp " << handle_suc_ts - start_ts;
        }

        // handle fail push callback
        while (!local_failed_key_queue.empty()) {
            DBKey* key = local_failed_key_queue.front();
            std::map<uint64_t, FileStream*>::iterator fail_file_it = file_streams_.find(key->ino);
            FileStream* file_stream = NULL;
            if (fail_file_it != file_streams_.end()) {
                file_stream = fail_file_it->second;

                file_stream->kreq_fail.Inc();
            }
            local_failed_key_queue.pop();

            if (key->ref.Dec() == 0) {
                if (file_stream) {
                    if (file_stream->HanleFailKey(key)) {
                        // re-send data
                        uint64_t offset, size;
                        file_stream->GetCheckpoint(key, &offset, &size);

                        if (size) {
                            VLOG(30) << "file " << key->filename << ", async push error, re-send";
                            std::vector<std::string> line_vec;
                            DBKey* rkey = NULL;
                            if (file_stream->CheckPointRead(&line_vec, &rkey, offset, size) >= 0) {
                                std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
                                std::string table_name;
                                std::vector<std::string> monitor_vec;
                                GetTableName(file_stream->GetFileName(), &table_name);
                                ParseMdtRequest(table_name, line_vec, &req_vec, &monitor_vec);
                                AsyncPush(req_vec, rkey);
                                AsyncPushMonitor(table_name, monitor_vec);
                            } else {
                                file_stream->ReSetFileStreamCheckPoint();
                            }
                        }
                    }
                }
                VLOG(30) << "delete key, file " << key->filename << ", key " << (uint64_t)key << ", offset " << key->offset;
                delete key;
            }
        }
        int64_t handle_fail_ts = mdt::timer::get_micros();
        if (handle_fail_ts > handle_suc_ts + 10000) {
            VLOG(30) << "handle fail timestamp " << handle_fail_ts - handle_suc_ts;
        }

        // handle write event
        std::map<uint64_t, std::string>::iterator write_it = local_write_event.begin();
        for(; write_it != local_write_event.end(); ++write_it) {
            uint64_t ino = write_it->first;
            const std::string& filename = write_it->second;
            VLOG(30) << ">>>>> Handle write: ino " << ino << ", filename " << filename;
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.find(ino);
            FileStream* file_stream;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;
                if (filename != file_stream->GetFileName()) {
                    // file has been rename
                    VLOG(30) << "file " << file_stream->GetFileName() << " has been rename, new name " << filename
                        << ", ino " << ino;
                    file_stream->SetFileName(filename);
                    DumpWriteEvent(file_stream->GetFileName(), ino);
                }
                file_stream->OpenFile();
            } else {
                // dump write event into leveldb
                DumpWriteEvent(filename, ino);

                // first log file add, check max fd wether is overflow
                if (file_streams_.size() > FLAGS_agent_max_fd_num) {
                    // close other file stream without checkpoint
                    bool reclaim_succ = false;
                    uint32_t stream_offset = (total_stream_offset_++) % (file_streams_.size());
                    std::map<uint64_t, FileStream*>::iterator stream_it = file_streams_.begin();
                    for (uint32_t tmp_i = 0; stream_it != file_streams_.end(); tmp_i++, ++stream_it) {
                        if (tmp_i >= stream_offset) {
                            FileStream* tmp_file_stream = stream_it->second;
                            std::string tmp_filename = tmp_file_stream->GetFileName();
                            uint64_t tmp_ino = stream_it->first;
                            if (tmp_file_stream->MarkDelete() >= 0) {
                                VLOG(35) << "file stream evict, " << tmp_file_stream->GetFileName();
                                file_streams_.erase(stream_it);
                                delete tmp_file_stream;

                                // delay resched it
                                ThreadPool::Task task =
                                    boost::bind(&LogStream::AddWriteEvent, this, tmp_filename, tmp_ino);
                                fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);

                                reclaim_succ = true;
                                break;
                            }
                        }
                    }

                    if (!reclaim_succ) {
                        pthread_spin_lock(server_addr_lock_);
                        // fd overflow, ignore write event
                        info_->history_fd_overflow_count++;
                        pthread_spin_unlock(server_addr_lock_);

                        // delay retry
                        ThreadPool::Task task =
                            boost::bind(&LogStream::AddWriteEvent, this, filename, ino);
                        fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
                        continue; // goto next round
                    }
                }

                // new file stream
                int success;
                file_stream = new FileStream(module_name_, log_options_, filename, ino, &success);
                if (success < 0) {
                    // TODO: log has been delete before it can be collector, :(-
                    kfile_miss_num.Inc();
                    LOG(WARNING) << "new stream, filename " << file_stream->GetFileName() << ", faile, ino " << ino;
                }
                file_streams_[ino] = file_stream;
            }

            // if configure file not push intime, no readable
            if (!CheckReadable(file_stream)) {
                VLOG(35) << "no configure, filename " << file_stream->GetFileName() << ", ino " << ino;
                ThreadPool::Task task =
                    boost::bind(&LogStream::AddWriteEvent, this, file_stream->GetFileName(), ino);
                fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
                continue;
            }
            // may take a little long
            ApplyRedoList(file_stream);

            DBKey* key = NULL;
            std::vector<std::string> line_vec;
            int file_read_res = 0;
            if ((file_read_res = file_stream->Read(&line_vec, &key)) >= 0) {
                if (line_vec.size() == 0) { // key will be null
                    // read file end, and no ref, evict from cache
                    if (file_stream->MarkDelete() >= 0) {
                        VLOG(35) << "evict from cache, ino " << ino << ", filename " << file_stream->GetFileName();
                        std::map<uint64_t, FileStream*>::iterator tmp_file_it = file_streams_.find(ino);
                        file_streams_.erase(tmp_file_it);
                        delete file_stream;
                    }
                    continue;
                }

                std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
                std::string table_name;
                std::vector<std::string> monitor_vec;
                GetTableName(file_stream->GetFileName(), &table_name);
                ParseMdtRequest(table_name, line_vec, &req_vec, &monitor_vec);
                AsyncPush(req_vec, key);
                AsyncPushMonitor(table_name, monitor_vec);

                AddWriteEvent(file_stream->GetFileName(), ino);
            } else if (file_read_res == -1) {
                // file error, give it
                LOG(WARNING) << "Missfile=" << file_stream->GetFileName() << " MissInode=" << ino;
                kfile_miss_num.Inc();
                DeleteWatchEvent(file_stream->GetFileName(), ino, false);
            } else if (file_read_res == -2) {
                // delay retry
                VLOG(30) << "delay schedule read, " << file_stream->GetFileName() << ", ino " << ino;
                ThreadPool::Task task =
                    boost::bind(&LogStream::AddWriteEvent, this, file_stream->GetFileName(), ino);
                fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
            } else {
                VLOG(30) << "has write event, but read nothing";
                //DeleteWatchEvent(file_stream->GetFileName(), ino, false);
            }
        }

        // handle delete event
        std::map<uint64_t, std::string>::iterator delete_it = local_delete_event.begin();
        for(; delete_it != local_delete_event.end(); ++delete_it) {
            uint64_t ino = delete_it->first;
            const std::string& filename = delete_it->second;
            std::map<uint64_t, FileStream*>::iterator del_file_it = file_streams_.find(ino);
            if (del_file_it != file_streams_.end()) {
                FileStream* file_stream = del_file_it->second;
                // if no one refer this file stream, then delete it
                if (file_stream->MarkDelete() >= 0) {
                    // TODO: need delete file_stream ??
                    file_streams_.erase(del_file_it);
                    delete file_stream;

                    // delete magic,current table, :)   :(-
                    std::string newname;
                    struct stat stat_buf;
                    uint64_t fino;
                    if (lstat(filename.c_str(), &stat_buf) < 0) {
                        // filename invalid
                        if (!StreamInodeToFileName(ino, filename, &newname)) {
                            DeleteMagicAndOffset(ino, filename);
                        }
                    } else {
                        fino = (uint64_t)stat_buf.st_ino;
                        if (fino != ino) {
                            // file rename
                            if (!StreamInodeToFileName(ino, filename, &newname)) {
                                DeleteMagicAndOffset(ino, filename);
                            }
                        }
                    }
                } else {
                    // add into delete queue without wakeup thread
                    ThreadPool::Task task =
                        boost::bind(&LogStream::DeleteWatchEvent, this, filename, ino, false);
                    fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
                    //DeleteWatchEvent(filename, ino, false);
                }
            } else {
                // handle file delete and no file stream(evict)
                // delete magic,current table, :)   :(-
                std::string newname;
                struct stat stat_buf;
                uint64_t fino;
                if (lstat(filename.c_str(), &stat_buf) < 0) {
                    // filename invalid
                    if (!StreamInodeToFileName(ino, filename, &newname)) {
                        DeleteMagicAndOffset(ino, filename);
                    }
                } else {
                    fino = (uint64_t)stat_buf.st_ino;
                    if (fino != ino) {
                        // file rename
                        if (!StreamInodeToFileName(ino, filename, &newname)) {
                            DeleteMagicAndOffset(ino, filename);
                        }
                    }
                }
            }
        }

/*
 *  (handle delete evet miss)
 *      LEVELDB data: MagicYoYo1989, CurrentOffset, checkpoint
 *      MagicYoYo1989=db+ino, path
 *      CurrentOffset=db+ino, offset
 *      Content=db+ino, offset+size+1k raw data
 *      CheckPoint=db+ino+offset, size
 */
#if 0
        int64_t curr_check_ts = mdt::timer::get_micros();
        if (curr_check_ts - last_ino_check_ts_ > 600000000) { // checkout per 10 min
            last_ino_check_ts_ = curr_check_ts;

            if (FLAGS_enable_reclaim_ino) {
                //ReclaimOrphanInode(module_name_);
            }
        } else {
            bool sched_need = true;
            pthread_spin_lock(&lock_);
            if (ctrl_event_.size() > 0) {
                sched_need = false;
            }
            pthread_spin_unlock(&lock_);
            if (sched_need) {
                //ThreadPool::Task task1 =
                //    boost::bind(&LogStream::AddCtrlEvent, this, 0);
                //fail_delay_thread_.DelayTask(60000, task1);
            }
        }
#endif

        // try to dump some information
        int64_t end_ts = mdt::timer::get_micros();
        VLOG(30) << "logstream run duration, " << end_ts - start_ts << ", end ts " << end_ts << ", start ts " << last_update_time_;
        int64_t curr_pending_req = 0;
        if (end_ts - last_update_time_ > 1000000) {
            last_update_time_ = end_ts;
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.begin();
            while (file_it != file_streams_.end()) {
                FileStream* file_stream = file_it->second;
                FileStreamProfile profile;
                file_stream->Profile(&profile);
                curr_pending_req += profile.nr_pending;
                VLOG(30) << "ino " << profile.ino << ", filename " << profile.filename
                    << ", nr_pending " << profile.nr_pending << ", current_offset " << profile.current_offset;

                // dump info into mem db
                std::string table_name;
                GetTableName(file_stream->GetFileName(), &table_name);
                std::string mkey;

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kreq_success";
                log_options_.counter_map->Add(mkey, file_stream->kreq_success.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kreq_fail";
                log_options_.counter_map->Add(mkey, file_stream->kreq_fail.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kfile_read_success";
                log_options_.counter_map->Add(mkey, file_stream->kfile_read_success.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kfile_read_fail";
                log_options_.counter_map->Add(mkey, file_stream->kfile_read_fail.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kleveldb_put_success";
                log_options_.counter_map->Add(mkey, file_stream->kleveldb_put_success.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kleveldb_put_fail";
                log_options_.counter_map->Add(mkey, file_stream->kleveldb_put_fail.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kleveldb_reopen_fail";
                log_options_.counter_map->Add(mkey, file_stream->kleveldb_reopen_fail.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kleveldb_delete_success";
                log_options_.counter_map->Add(mkey, file_stream->kleveldb_delete_success.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kleveldb_delete_fail";
                log_options_.counter_map->Add(mkey, file_stream->kleveldb_delete_fail.Clear());

                mkey = module_name_ + "." + table_name + "." + hostname_ + "." + "kcheckpoint_read_num";
                log_options_.counter_map->Add(mkey, file_stream->kcheckpoint_read_num.Clear());

                ++file_it;
            }

            std::string key1;
            key1 = module_name_ + "." + hostname_ + "." + "kseq_send_num";
            log_options_.counter_map->Add(key1, kseq_send_num.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kseq_nonsend_num";
            log_options_.counter_map->Add(key1, kseq_nonsend_num.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kseq_send_success";
            log_options_.counter_map->Add(key1, kseq_send_success.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kseq_send_fail";
            log_options_.counter_map->Add(key1, kseq_send_fail.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kindex_filter_num";
            log_options_.counter_map->Add(key1, kindex_filter_num.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kkeyword_filter_num";
            log_options_.counter_map->Add(key1, kkeyword_filter_num.Clear());

            key1 = module_name_ + "." + hostname_ + "." + "kfile_miss_num";
            log_options_.counter_map->Add(key1, kfile_miss_num.Clear());
        }

        pthread_spin_lock(server_addr_lock_);
        info_->nr_file_streams = (int64_t)file_streams_.size();
        if (curr_pending_req > 0) {
            info_->curr_pending_req = curr_pending_req;
        }
        pthread_spin_unlock(server_addr_lock_);
    }
}

void LogStream::StreamMakeContextKey(const std::string& db_name, uint64_t ino, std::string* key,
                                     uint64_t offset, uint64_t size, char* buf, std::string* val) {
    *key = "ContentKey";
    key->push_back('\0');
    *key += db_name;
    key->push_back('\0');
    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    if (val) {
        std::string offset_str;
        EncodeUint64BigEndian(offset, &offset_str);
        std::string size_str;
        EncodeUint64BigEndian(size, &size_str);
        std::string buf_str(buf, 0, size);
        *val = offset_str + size_str + buf_str;
    }
}
void LogStream::DeleteMagicAndOffset(uint64_t ino, const std::string& filename) {
    leveldb::WriteBatch batch;

    std::string magic_key;
    EraseWriteEvent(filename, ino, &magic_key);
    batch.Delete(magic_key);

    std::string offset_key;
    StreamMakeCurrentOffsetKey(module_name_, filename, ino, 0, &offset_key, NULL);
    batch.Delete(offset_key);

    std::string ckey;
    StreamMakeContextKey(module_name_, ino, &ckey, 0, 0, NULL, NULL);
    batch.Delete(ckey);

    VLOG(30) << "Magic,Offset,Content Table delete: ino " << ino << ", filename " << filename;

    // delete leveldb stat
    leveldb::Status ds = log_options_.db->Write(leveldb::WriteOptions(), &batch);
    if (!ds.ok()) {
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        ds = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!ds.ok()) {
            LOG(WARNING) << "leveldb reopen errno " << ds.ToString();
        }
    }
}

// key=CurrentOffset'\0'dbname'\0'ino;
// value=offset
void LogStream::StreamMakeCurrentOffsetKey(const std::string& module_name,
                                     const std::string& filename,
                                     uint64_t ino,
                                     uint64_t offset,
                                     std::string* key,
                                     std::string* value) {
    *key = "CurrentOffset";
    key->push_back('\0');
    *key += module_name;
    key->push_back('\0');

    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    if (value) {
        std::string offset_str;
        EncodeUint64BigEndian(offset, &offset_str);
        *value = offset_str;
    }
}

// key=dbname'\0'ino offset, value=size
void LogStream::StreamMakeKeyValue(const std::string& module_name,
                             const std::string& filename,
                             uint64_t ino,
                             uint64_t offset,
                             std::string* key,
                             uint64_t size,
                             std::string* value) {
    *key = "CheckPoint";
    key->push_back('\0');
    *key += module_name;
    key->push_back('\0');
    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    std::string offset_str;
    EncodeUint64BigEndian(offset, &offset_str);
    *key += offset_str;

    if (value) {
        EncodeUint64BigEndian(size, value);
    }
    return;
}

void LogStream::StreamParseKeyValue(const leveldb::Slice& key,
                              const leveldb::Slice& value,
                              uint64_t* ino,
                              uint64_t* offset, uint64_t* size) {
    int prefixlen = strlen(key.data());
    leveldb::Slice db_slice(key.data() + prefixlen + 1, key.size() - prefixlen - 1);

    int mlen = strlen(db_slice.data());
    leveldb::Slice ino_str = leveldb::Slice(db_slice.data() + mlen + 1, 8);
    *ino = DecodeBigEndain(ino_str.data());

    leveldb::Slice offset_str = leveldb::Slice(db_slice.data() + mlen + 1 + 8, 8);
    *offset = DecodeBigEndain(offset_str.data());

    *size = DecodeBigEndain(value.data());
}

void LogStream::ReclaimOrphanInode(const std::string& db_name) {
    // fname + ino
    std::vector<std::pair<std::string, uint64_t> > event_vec;
    RecoverWriteEvent(&event_vec);

    for (uint32_t i = 0; i < event_vec.size(); i++) {
        std::string newname;
        if (!StreamInodeToFileName(event_vec[i].second, event_vec[i].first, &newname)) {
            // file has been delete, check cp
            leveldb::Iterator* db_it = log_options_.db->NewIterator(leveldb::ReadOptions());
            std::string startkey, endkey;
            StreamMakeKeyValue(module_name_, event_vec[i].first, event_vec[i].second, 0, &startkey, 0, NULL);
            StreamMakeKeyValue(module_name_, event_vec[i].first, event_vec[i].second, 0xffffffffffffffff, &endkey, 0, NULL);
            bool has_cp = false;
            for (db_it->Seek(startkey);
                    db_it->Valid() && db_it->key().ToString() < endkey;
                    db_it->Next()) {
                has_cp = true;
                break;
            }
            delete db_it;
            if (has_cp) {
                continue;
            }

            VLOG(35) << "reclaim magic, offset table, ino " << event_vec[i].second
                << ", filename " << event_vec[i].first;
            DeleteMagicAndOffset(event_vec[i].second, event_vec[i].first);
#if 0
            // no check point, TODO: may use write batch
            leveldb::WriteBatch batch;
            std::string magic_key;
            EraseWriteEvent(event_vec[i].first, event_vec[i].second, &magic_key);
            batch.Delete(magic_key);

            std::string offset_key;
            StreamMakeCurrentOffsetKey(db_name, event_vec[i].first, event_vec[i].second, 0, &offset_key, NULL);
            batch.Delete(offset_key);

            // delete leveldb stat
            leveldb::Status ds = log_options_.db->Write(leveldb::WriteOptions(), &batch);
            if (!ds.ok()) {
                delete log_options_.db;
                log_options_.db = NULL;
                leveldb::Options options;
                ds = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
                if (!ds.ok()) {
                    LOG(WARNING) << "leveldb reopen errno " << ds.ToString();
                }
            }
#endif
        }
    }
}

bool LogStream::StreamInodeToFileName(uint64_t ino, const std::string& filename, std::string* newname) {
    // get parent dir
    newname->clear();
    std::string dir;
    std::string delim("/");
    std::size_t pos = filename.rfind(delim);
    if (pos != std::string::npos) {
        dir = std::string(filename, 0, pos + 1);

        // list dir
        DIR* dirptr = NULL;
        struct dirent* entry = NULL;
        if ((dirptr = opendir(dir.c_str())) != NULL) {
            while ((entry = readdir(dirptr)) != NULL) {
                std::string fname(entry->d_name);
                if (fname == "." || fname == "..") {
                    continue;
                }
                // find filename match ino
                uint64_t tmp_ino = (uint64_t)entry->d_ino;
                if (tmp_ino == ino) {
                    *newname = dir + fname;
                    closedir(dirptr);
                    return true;
                }

                // subdir, search into it
                if (entry->d_type == DT_DIR) {
                    std::string subdir = dir + fname + "/";
                    if (StreamFindLostInode(ino, subdir, newname)) {
                        closedir(dirptr);
                        return true;
                    }
                }
            }
            closedir(dirptr);
        }
    }
    return false;
}
bool LogStream::StreamFindLostInode(uint64_t ino, const std::string& dir, std::string* newname) {
    newname->clear();

    // list dir
    DIR* dirptr = NULL;
    struct dirent* entry = NULL;
    if ((dirptr = opendir(dir.c_str())) != NULL) {
        while ((entry = readdir(dirptr)) != NULL) {
            // find filename match ino
            uint64_t tmp_ino = (uint64_t)entry->d_ino;
            if (tmp_ino == ino) {
                std::string fname(entry->d_name);
                *newname = dir + fname;
                closedir(dirptr);
                return true;
            }
        }
        closedir(dirptr);
    }
    return false;
}

// split string by substring
struct LogRecord {
    std::vector<std::string> columns;

    void Print() {
        std::cout << "LogRecord\n";
        for (int i = 0 ; i < (int)columns.size(); i++) {
            std::cout << "\t" << columns[i] << std::endl;
        }
    }

    int SplitLogItem(const std::string& str, const std::vector<std::string>& dim_vec) {
        if (dim_vec.size() == 0) {
            columns.push_back(str);
            return 0;
        }
        std::size_t pos = 0, prev = 0;
        while (1) {
            std::size_t min_pos = std::string::npos;
            pos = min_pos;
            int min_idx = (int)(1 << 20);
            for (int i = 0; i < (int)dim_vec.size(); i++) {
                const std::string& dim = dim_vec[i];
                min_pos = str.find(dim, prev);
                if ((pos == std::string::npos) || (min_pos != std::string::npos && pos > min_pos)) {
                    pos = min_pos;
                    min_idx = i;
                }
            }
            if (pos > prev) {
                columns.push_back(str.substr(prev, pos - prev));
            }
            if ((pos == std::string::npos) || (min_idx == (int)(1 << 20))) {
                break;
            }
            prev = pos + dim_vec[min_idx].size();
        }
        return 0;
    }
};

// kv parser
// split string by char
struct LogTailerSpan {
    std::map<std::string, std::string> kv_annotation;

    uint32_t ParseKVpairs(const std::string& line, const std::string& linedelims,
                          const std::string& kvdelims,
                          const std::set<std::string>& index_list) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::vector<std::string> linevec;
        boost::split(linevec, line, boost::is_any_of("\n"));
        if (linevec.size() == 0 || linevec[0].size() == 0) return 0;
        //if (linevec[0].at(linevec.size() - 1) != '\n') return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, linevec[0], boost::is_any_of(linedelims));
        for (uint32_t i = 0; i < kvpairs.size(); i++) {
            const std::string& kvpair = kvpairs[i];
            std::vector<std::string> kv;
            boost::split(kv, kvpair, boost::is_any_of(kvdelims));
            if (kv.size() == 2 && kv[0].size() > 0 && kv[1].size() > 0) {
                if (index_list.find(kv[0]) == index_list.end()) {
                    logkv.insert(std::pair<std::string, std::string>(kv[0], kv[1]));
                }
            }
        }
        return linevec[0].size() + 1;
    }

    uint32_t ParseKVpairs(const std::string& line, const std::string& linedelims,
                          const std::string& kvdelims,
                          const std::map<std::string, std::string>& alias_index_map) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::vector<std::string> linevec;
        boost::split(linevec, line, boost::is_any_of("\n"));
        if (linevec.size() == 0 || linevec[0].size() == 0) return 0;
        //if (linevec[0].at(linevec.size() - 1) != '\n') return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, linevec[0], boost::is_any_of(linedelims));
        for (uint32_t i = 0; i < kvpairs.size(); i++) {
            const std::string& kvpair = kvpairs[i];
            std::vector<std::string> kv;
            boost::split(kv, kvpair, boost::is_any_of(kvdelims));
            if (kv.size() == 2 && kv[0].size() > 0 && kv[1].size() > 0) {
                std::map<std::string, std::string>::const_iterator it = alias_index_map.find(kv[0]);
                if (it != alias_index_map.end()) {
                    logkv.insert(std::pair<std::string, std::string>(it->second, kv[1]));
                }
            } else if (kv.size() > 2 && kv[kv.size() - 2].size() > 0 && kv[kv.size() - 1].size() > 0) {
                std::map<std::string, std::string>::const_iterator it = alias_index_map.find(kv[kv.size() - 2]);
                if (it != alias_index_map.end()) {
                    logkv.insert(std::pair<std::string, std::string>(it->second, kv[kv.size() - 1]));
                }
            }

        }
        return linevec[0].size() + 1;
    }
    uint32_t ParseFixedKvPairs(const std::string& line, const std::string& linedelims,
                               const std::map<std::string, int>& fixed_index_list) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, line, boost::is_any_of(linedelims));
        std::map<std::string, int>::const_iterator it = fixed_index_list.begin();
        for (; it != fixed_index_list.end(); ++it) {
            if ((uint32_t)it->second < kvpairs.size()) {
                logkv[it->first] = kvpairs[it->second];
            }
        }
        return 0;
    }

    void PrintKVpairs() {
        const std::map<std::string, std::string>& logkv = kv_annotation;
        std::cout << "LogSpan kv: ";
        std::map<std::string, std::string>::const_iterator it = logkv.begin();
        for (; it != logkv.end(); ++it) {
            std::cout << "[" << it->first << ":" << it->second << "]  ";
        }
        std::cout << std::endl;
    }
};

// only use in agent, primary key may cause seq write in tera
std::string LogStream::TimeToStringWithTid(struct timeval* filetime) {
#ifdef OS_LINUX
    pid_t tid = syscall(SYS_gettid);
#else
    pthread_t tid = pthread_self();
#endif
    uint64_t thread_id = 0;
    memcpy(&thread_id, &tid, std::min(sizeof(thread_id), sizeof(tid)));
    //thread_id %= 1000000;

    struct timeval now_tv;
    gettimeofday(&now_tv, NULL);
    const time_t seconds = now_tv.tv_sec;
    struct tm t;
    localtime_r(&seconds, &t);
    char buf[34];
    char* p = buf;
#if 0
    p += snprintf(p, 34,
            "%06lu:%04d-%02d-%02d-%02d:%02d:%02d.%06d",
            (unsigned long)thread_id,
            t.tm_year + 1900,
            t.tm_mon + 1,
            t.tm_mday,
            t.tm_hour,
            t.tm_min,
            t.tm_sec,
            static_cast<int>(now_tv.tv_usec));
#endif
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

std::string LogStream::GetUUID() {
    char buf[36];
    uuid_t uid;
    uuid_generate(uid);
    uuid_unparse(uid, buf);
    std::string s = buf;
    return s;
#if 0
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    std::stringstream ss;
    ss << uuid;
    std::string s = ss.str();
    return s;
#endif
}

// type 1: sec + micro sec
uint64_t LogStream::ParseTime(const std::string& time_str) {
    if (time_type_ == 1) {
        return (uint64_t)atol(time_str.c_str());
    }
    return 0;
}

bool LogStream::CheckReadable(FileStream* file_stream) {
    std::string table_name;
    GetTableName(file_stream->GetFileName(), &table_name);
    pthread_spin_lock(&monitor_lock_);
    if (index_set_.find(table_name) != index_set_.end()) {
        pthread_spin_unlock(&monitor_lock_);
        return true;
    }
    pthread_spin_unlock(&monitor_lock_);

    if (!FLAGS_use_regex_index_pattern) {
        return true;
    }
    return false;
}
// NOTICE: parse log line, add monitor logic
// case 1: key1=001,key2=002,key3=003||key4=004,key005=005,key006=006
// case 2: 001 002 003 004 005 006
// case 3: key1 001, key2 002, key3 003, key4 004, key5 005, key6 006
int LogStream::ParseMdtRequest(const std::string table_name,
                               std::vector<std::string>& line_vec,
                               std::vector<mdt::SearchEngine::RpcStoreRequest* >* req_vec,
                               std::vector<std::string>* monitor_vec) {
    int64_t start_ts, end_ts;
    start_ts = mdt::timer::get_micros();

    if (table_name == "" || table_name == "trash") {
        return 0;
    }
    for (uint32_t i = 0; i < line_vec.size(); i++) {
        int res = 0;
        std::string& line  = line_vec[i];
        if (line.size() == 0) {
            continue;
        }

        end_ts = mdt::timer::get_micros();
        VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
        start_ts = end_ts;
        // support line filter
        if (string_filter_.size() > 0) {
            bool found = false;
            for (uint32_t filter_idx = 0; filter_idx < string_filter_.size(); filter_idx++) {
                if (line.find(string_filter_[filter_idx]) != std::string::npos) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                kkeyword_filter_num.Inc();
                continue;
            }
        }

        end_ts = mdt::timer::get_micros();
        VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
        start_ts = end_ts;
        // support monitor
        if (MonitorHasEvent(table_name, line)) {
            monitor_vec->push_back(line);
        }

        end_ts = mdt::timer::get_micros();
        VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
        start_ts = end_ts;
        // support regex index
        mdt::SearchEngine::RpcStoreRequest* regex_req = new mdt::SearchEngine::RpcStoreRequest();
        if (SearchIndex(line, table_name, regex_req) >= 0) {
            req_vec->push_back(regex_req);
        } else {
            delete regex_req;
            kindex_filter_num.Inc();
        }

        // old index parser
        if (FLAGS_use_regex_index_pattern) {
            continue;
        }
        end_ts = mdt::timer::get_micros();
        VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
        start_ts = end_ts;
        mdt::SearchEngine::RpcStoreRequest* req = NULL;
        LogRecord log;
        if (log.SplitLogItem(line, string_delims_) < 0) {
            res = -1;
            VLOG(30) << "parse mdt request split by string fail: " << line;
        } else {
            end_ts = mdt::timer::get_micros();
            VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
            start_ts = end_ts;
            LogTailerSpan kv;
            for (int col_idx = 0; col_idx < (int)log.columns.size(); col_idx++) {
                if (use_fixed_index_list_) {
                    kv.ParseFixedKvPairs(log.columns[col_idx], line_delims_, fixed_index_list_);
                } else {
                    //kv.ParseKVpairs(log.columns[col_idx], line_delims_, kv_delims_, index_list_);
                    kv.ParseKVpairs(log.columns[col_idx], line_delims_, kv_delims_, alias_index_map_);
                }
            }

            end_ts = mdt::timer::get_micros();
            VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
            start_ts = end_ts;
            req = new mdt::SearchEngine::RpcStoreRequest();
            req->set_db_name(module_name_);
            req->set_table_name(table_name);
            // set index
            std::map<std::string, std::string>::iterator it = kv.kv_annotation.begin();
            for (; it != kv.kv_annotation.end(); ++it) {
                if (it->first == primary_key_) {
                    req->set_primary_key(it->second);
                } else {
                    mdt::SearchEngine::RpcStoreIndex* idx = req->add_index_list();
                    idx->set_index_table(it->first);
                    idx->set_key(it->second);
                }
            }

            end_ts = mdt::timer::get_micros();
            VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
            start_ts = end_ts;
            // if primary key not set, use time
            if (req->primary_key() == "") {
                struct timeval dummy_time;
                //req->set_primary_key(TimeToStringWithTid(&dummy_time));
                req->set_primary_key(GetUUID());
            }
            end_ts = mdt::timer::get_micros();
            VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
            start_ts = end_ts;
            req->set_data(line);

            end_ts = mdt::timer::get_micros();
            VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
            start_ts = end_ts;
            // user has time item in log
            it = kv.kv_annotation.find(user_time_);
            if (user_time_.size() && it != kv.kv_annotation.end()) {
                uint64_t ts = ParseTime(it->second);
                if (ts > 0) {
                    req->set_timestamp(ts);
                } else {
                    req->set_timestamp(mdt::timer::get_micros());
                }
            } else {
                req->set_timestamp(mdt::timer::get_micros());
            }
        }

        end_ts = mdt::timer::get_micros();
        VLOG(45) << "parse index " << i << ", table " << table_name << ", " << end_ts - start_ts;
        start_ts = end_ts;
        if (res >= 0) {
            req_vec->push_back(req);
        } else if (req) {
            delete req;
        }
    }
    return req_vec->size();
}

void LogStream::ApplyRedoList(FileStream* file_stream) {
    // redo check point
    std::map<uint64_t, uint64_t> redo_list;
    file_stream->GetRedoList(&redo_list);
    std::map<uint64_t, uint64_t>::iterator redo_it = redo_list.begin();
    bool file_rename = false;
    for (; redo_it != redo_list.end(); ++redo_it) {
        DBKey* key = NULL;
        std::vector<std::string> line_vec;
        std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;

        // agent restart, check log file rename
        if (file_stream->CheckPointRead(&line_vec, &key, redo_it->first, redo_it->second) >= 0) {
            std::string table_name;
            std::vector<std::string> monitor_vec;
            GetTableName(file_stream->GetFileName(), &table_name);
            ParseMdtRequest(table_name, line_vec, &req_vec, &monitor_vec);
            AsyncPush(req_vec, key);
            AsyncPushMonitor(table_name, monitor_vec);
        } else {
            file_rename = true;
        }
    }
    if (file_rename) {
        file_stream->ReSetFileStreamCheckPoint();
    }
}

int LogStream::AsyncPush(std::vector<mdt::SearchEngine::RpcStoreRequest*>& req_vec, DBKey* key) {
    if (!key) {
        return 0;
    }

    pthread_spin_lock(server_addr_lock_);
    std::string server_addr = info_->collector_addr;
    info_->qps_use += req_vec.size();
    pthread_spin_unlock(server_addr_lock_);
    VLOG(30) << "async send data to " << server_addr << ", nr req " << req_vec.size()
        << ", offset " << key->offset << ", ino " << key->ino << ", filename " << key->filename;

    mdt::SearchEngine::SearchEngineService_Stub* service;
    if (req_vec.size() == 0) {
        // assume async send success.
        kseq_nonsend_num.Inc();

        key->ref.Set(1);
        mdt::SearchEngine::RpcStoreRequest* req = new mdt::SearchEngine::RpcStoreRequest;
        mdt::SearchEngine::RpcStoreResponse* resp = new mdt::SearchEngine::RpcStoreResponse;
        rpc_client_->GetMethodList(server_addr, &service);
        VLOG(40) << "assume async send success, file " << key->filename << ", offset " << key->offset
            << ", ino " << key->ino;
        AsyncPushCallback(req, resp, 0, 0, service, key);
        return 0;
    }

    key->ref.Set((uint64_t)(req_vec.size()));
    for (uint32_t i = 0; i < req_vec.size(); i++) {
        rpc_client_->GetMethodList(server_addr, &service);
        mdt::SearchEngine::RpcStoreRequest* req = req_vec[i];
        mdt::SearchEngine::RpcStoreResponse* resp = new mdt::SearchEngine::RpcStoreResponse;
        VLOG(40) << "\n =====> async send data to " << server_addr << ", req " << (uint64_t)req
            << ", resp " << (uint64_t)resp << ", key " << (uint64_t)key << "\n " << req->DebugString();

        kseq_send_num.Inc();

        pthread_spin_lock(server_addr_lock_);
        info_->average_packet_size += (int64_t)req->data().size();
        if (info_->max_packet_size < (int64_t)(req->data().size())) {
            info_->max_packet_size = req->data().size();
        }
        if (info_->min_packet_size > (int64_t)(req->data().size())) {
            info_->min_packet_size = req->data().size();
        }
        pthread_spin_unlock(server_addr_lock_);

        boost::function<void (const mdt::SearchEngine::RpcStoreRequest*,
                              mdt::SearchEngine::RpcStoreResponse*,
                              bool, int)> callback =
            boost::bind(&LogStream::AsyncPushCallback,
                        this, _1, _2, _3, _4, service, key);
        rpc_client_->AsyncCall(service,
                              &mdt::SearchEngine::SearchEngineService_Stub::Store,
                              req, resp, callback);
    }
    return 0;
}

void LogStream::HandleDelayFailTask(DBKey* key) {
    pthread_spin_lock(&lock_);
    failed_key_queue_.push(key);
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
}

void LogStream::AsyncPushCallback(const mdt::SearchEngine::RpcStoreRequest* req,
                                  mdt::SearchEngine::RpcStoreResponse* resp,
                                  bool failed, int error,
                                  mdt::SearchEngine::SearchEngineService_Stub* service,
                                  DBKey* key) {
    // handle data push error
    if (failed || (resp->status() != mdt::SearchEngine::RpcOK)) {
        if (kLastLogWarningTime + 60000000 < timer::get_micros()) {
            kLastLogWarningTime = timer::get_micros();
            LOG(WARNING) << "async write error " << error << ", key " << (uint64_t)key
                << ", ino " << key->ino << ", file " << key->filename << " add to failed event queue"
                << ", req " << (uint64_t)req << ", resp " << (uint64_t)resp << ", key.ref " << key->ref.Get() << ", offset " << key->offset;
        }
        pthread_spin_lock(server_addr_lock_);
        info_->error_nr++;
        pthread_spin_unlock(server_addr_lock_);

        kseq_send_fail.Inc();

        ThreadPool::Task task =
            boost::bind(&LogStream::HandleDelayFailTask, this, key);
        fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
    } else {
        kseq_send_success.Inc();

        VLOG(40) << "ino " << key->ino << ", file " << key->filename << " add to success event queue, req "
            << (uint64_t)req << ", resp " << (uint64_t)resp << ", key " << (uint64_t)key
            << ", key.ref " << key->ref.Get() << ", offset " << key->offset;
        pthread_spin_lock(&lock_);
        key_queue_.push(key);
        pthread_spin_unlock(&lock_);
        thread_event_.Set();
    }
    delete req;
    delete service;
    delete resp;
}

void LogStream::AsyncPushMonitorCallback(const mdt::LogSchedulerService::RpcMonitorStreamRequest* req,
                                         mdt::LogSchedulerService::RpcMonitorStreamResponse* resp,
                                         bool failed, int error,
                                         mdt::LogSchedulerService::LogSchedulerService_Stub* service) {
    delete req;
    delete service;
    delete resp;
}

int LogStream::AsyncPushMonitor(const std::string& table_name, const std::vector<std::string>& monitor_vec) {
    if (monitor_vec.size() == 0) {
        return 0;
    }
    // push monitor event to scheduler
    char hostname[256];
    gethostname(hostname, 256);
    std::string hname = hostname;

    mdt::LogSchedulerService::LogSchedulerService_Stub* service;
    rpc_client_->GetMethodList(FLAGS_scheduler_addr, &service);
    mdt::LogSchedulerService::RpcMonitorStreamRequest* req = new mdt::LogSchedulerService::RpcMonitorStreamRequest();
    mdt::LogSchedulerService::RpcMonitorStreamResponse* resp = new mdt::LogSchedulerService::RpcMonitorStreamResponse();
    req->set_db_name(module_name_);
    req->set_table_name(table_name);
    req->set_hostname(hname);
    for (uint32_t i = 0; i < monitor_vec.size(); i++) {
        std::string* tmp_record = req->add_log_record();
        *tmp_record = monitor_vec[i];
    }
    boost::function<void (const mdt::LogSchedulerService::RpcMonitorStreamRequest*,
                          mdt::LogSchedulerService::RpcMonitorStreamResponse*,
                          bool, int)> callback =
            boost::bind(&LogStream::AsyncPushMonitorCallback,
                        this, _1, _2, _3, _4, service);
    rpc_client_->AsyncCall(service,
                           &mdt::LogSchedulerService::LogSchedulerService_Stub::RpcMonitorStream,
                           req, resp, callback);
    return 0;
}

int LogStream::AddCtrlEvent(uint64_t event_id) {
    pthread_spin_lock(&lock_);
    ctrl_event_.insert(std::pair<uint64_t, uint64_t>(event_id, 0));
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

int LogStream::AddWriteEvent(std::string filename) {
    struct stat stat_buf;
    if (lstat(filename.c_str(), &stat_buf) < 0) {
        return 0;
    }
    uint64_t ino = (uint64_t)stat_buf.st_ino;
    VLOG(40) << "ino " << ino << ", file " << filename << " add to write event queue";

    pthread_spin_lock(&lock_);
    //write_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    write_event_[ino] = filename;
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

int LogStream::AddWriteEvent(std::string filename, uint64_t ino) {
    std::string newname;
    struct stat stat_buf;
    uint64_t fino;
    if (lstat(filename.c_str(), &stat_buf) < 0) {
        // filename invalid
        StreamInodeToFileName(ino, filename, &newname);
    } else {
        fino = (uint64_t)stat_buf.st_ino;
        if (fino != ino) {
            // file rename
            StreamInodeToFileName(ino, filename, &newname);
        }
    }

    VLOG(40) << "ino " << ino << ", file " << (newname.size() ? newname: filename) << " add to write event queue";

    pthread_spin_lock(&lock_);
    if (newname.size()) {
        write_event_[ino] = newname;
    } else {
        write_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    }
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

int LogStream::DeleteWatchEvent(std::string filename, bool need_wakeup) {
    struct stat stat_buf;
    if (lstat(filename.c_str(), &stat_buf) == -1) {
        return 0;
    }
    uint64_t ino = (uint64_t)stat_buf.st_ino;
    VLOG(40) << "ino " << ino << ", file " << filename << ", fsize " << stat_buf.st_size
        << ", add to delete event queue, wakup " << need_wakeup;

    pthread_spin_lock(&lock_);
    //delete_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    delete_event_[ino] = filename;
    pthread_spin_unlock(&lock_);
    if (need_wakeup) {
        thread_event_.Set();
    }
    return 0;
}

int LogStream::DeleteWatchEvent(std::string filename, uint64_t ino, bool need_wakeup) {
    std::string newname;
    struct stat stat_buf;
    uint64_t fino;
    if (lstat(filename.c_str(), &stat_buf) < 0) {
        // filename invalid
        StreamInodeToFileName(ino, filename, &newname);
    } else {
        fino = (uint64_t)stat_buf.st_ino;
        if (fino != ino) {
            // file rename
            StreamInodeToFileName(ino, filename, &newname);
        }
    }

    VLOG(40) << "ino " << ino << ", file " << (newname.size() ? newname: filename) << ", fsize " << stat_buf.st_size
        << " add to delete event queue, wakup " << need_wakeup;

    pthread_spin_lock(&lock_);
    delete_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    //delete_event_[ino] = filename;
    pthread_spin_unlock(&lock_);
    if (need_wakeup) {
        thread_event_.Set();
    }
    return 0;
}

int LogStream::AddTableName(const std::string& log_name) {
    log_name_prefix_.insert(log_name);
    return 0;
}

/////////////////////////////////////////
//  support index
/////////////////////////////////////////
int LogStream::UpdateIndex(const mdt::LogAgentService::RpcUpdateIndexRequest* request) {
    pthread_spin_lock(&monitor_lock_);
    mdt::LogAgentService::RpcUpdateIndexRequest& index = index_set_[request->table_name()];
    index.CopyFrom(*request);
    pthread_spin_unlock(&monitor_lock_);
    VLOG(10) << "Add index: " << request->DebugString();
    return 0;
}

int LogStream::InternalSearchIndex(const std::string& line,
                                   const mdt::LogAgentService::Rule& rule,
                                   std::map<std::string, std::string>* kv) {
    const mdt::LogAgentService::Expression& expr = rule.expr();

    // only support regex
    if (expr.type() == "regex") {
        try {
            std::string::const_iterator start, end;
            start = line.begin();
            end = line.end();

            boost::regex expression(expr.expr());
            boost::match_results<std::string::const_iterator> watch;
            while (boost::regex_search(start, end, watch, expression)) {
                if (watch.size() >= (rule.record_vec_size() + 1)) {
                    VLOG(50) << line << ", watch " << watch[0];
                    for (uint32_t result_idx = 1; (result_idx < watch.size()) && (result_idx <= rule.record_vec_size()); result_idx++) {
                        kv->insert(std::pair<std::string, std::string>(rule.record_vec(result_idx - 1).key_name(), watch[result_idx]));
                        VLOG(50) << "key " << rule.record_vec(result_idx - 1).key_name() << ", value " << watch[result_idx];
                    }
                }
                start = watch[0].second;
                break;
            }
        } catch (const boost::bad_expression& e) {}
    } else if (expr.type() == "fixed") {
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, line, boost::is_any_of(expr.column_delim()));
        if ((kvpairs.size() > expr.column_idx()) && (rule.record_vec_size() > 0)) {
            kv->insert(std::pair<std::string, std::string>(rule.record_vec(0).key_name(), kvpairs[expr.column_idx()]));
            VLOG(50) << "fixed key " << rule.record_vec(0).key_name() << ", value " << kvpairs[expr.column_idx()];
        }
    }
    return 0;
}

bool LogStream::CheckTimeStampValid(const std::string& time_str) {
    int64_t ts = timer::get_micros();
    int64_t log_ts = (int64_t)(atol(time_str.c_str()));
    if (((ts - 3600 * 24 * 30) < log_ts) && (log_ts < (ts + 3600 * 24 * 30))) {
        return true;
    }
    return false;
}

int LogStream::CollectorMeta(const mdt::LogAgentService::LogMeta& meta,
                             std::map<std::string, std::string>* kv) {
    int res = 0;

    if (meta.meta_name() == "ip") {
        char hostname[256];
        gethostname(hostname, 256);
        std::string hname = hostname;
        kv->insert(std::pair<std::string, std::string>(meta.meta_name(), hname));
    }

    return res;
}

int LogStream::SearchIndex(const std::string& line, const std::string& table_name,
                           mdt::SearchEngine::RpcStoreRequest* req) {
    int res = -1;
    pthread_spin_lock(&monitor_lock_);
    if (index_set_.find(table_name) != index_set_.end()) {
        const mdt::LogAgentService::RpcUpdateIndexRequest& index = index_set_[table_name];
        std::string tmp_line;

        req->set_db_name(index.db_name());
        req->set_table_name(index.table_name());
        req->set_timestamp(0);

        // parse index from line
        std::map<std::string, std::string> kv;
        for (uint32_t idx = 0; idx < index.rule_list_size(); idx++) {
            const mdt::LogAgentService::Rule& rule = index.rule_list(idx);
            InternalSearchIndex(line, rule, &kv);
        }

        // parse log meta
        for (uint32_t idx = 0; idx < index.meta_size(); idx++) {
            const mdt::LogAgentService::LogMeta& meta = index.meta(idx);
            CollectorMeta(meta, &kv);
        }

        std::map<std::string, std::string>::iterator it = kv.begin();
        for (; it != kv.end(); ++it) {
            if (it->first == index.primary_key()) {
                req->set_primary_key(it->second);
            } else if ((it->first == index.timestamp()) && (CheckTimeStampValid(it->second))) {
                req->set_timestamp((uint64_t)atol((it->second).c_str()));
            } else {
                mdt::SearchEngine::RpcStoreIndex* idx_tmp = req->add_index_list();
                idx_tmp->set_index_table(it->first);
                idx_tmp->set_key(it->second);

                tmp_line += "{" + it->first + ":" + it->second + "}";
            }
        }
        tmp_line += ":";
        if (req->primary_key() == "") {
            struct timeval dummy_time;
            //req->set_primary_key(TimeToStringWithTid(&dummy_time));
            req->set_primary_key(GetUUID());
        }
        if (req->timestamp() == 0) {
            req->set_timestamp(mdt::timer::get_micros());
        }
        tmp_line += line;
        req->set_data(tmp_line);
        res = 0;
    }
    pthread_spin_unlock(&monitor_lock_);

    return res;
}

/////////////////////////////////////////
//  support monitor
/////////////////////////////////////////
int LogStream::AddMonitor(const mdt::LogAgentService::RpcMonitorRequest* request) {
    pthread_spin_lock(&monitor_lock_);
    mdt::LogAgentService::RpcMonitorRequest& monitor = monitor_handler_set_[request->table_name()];
    monitor.CopyFrom(*request);
    pthread_spin_unlock(&monitor_lock_);
    VLOG(10) << "Add Monitor: " << request->DebugString();
    return 0;
}

// only support type: string, int64
bool LogStream::CheckRecord(const std::string& key, const mdt::LogAgentService::Record& record) {
    bool is_match = false;

    if (record.op() == "==") {
        if (record.type() == "string") {
            is_match = (key == record.key());
        } else if (record.type() == "int64") {
            is_match = (atol(key.c_str()) == atol(record.key().c_str()));
        }
    } else if (record.op() == ">=") {
        if (record.type() == "string") {
            is_match = (key >= record.key());
        } else if (record.type() == "int64") {
            is_match = (atol(key.c_str()) >= atol(record.key().c_str()));
        }
    } else if (record.op() == ">") {
        if (record.type() == "string") {
            is_match = (key > record.key());
        } else if (record.type() == "int64") {
            is_match = (atol(key.c_str()) > atol(record.key().c_str()));
        }
    } else if (record.op() == "<=") {
        if (record.type() == "string") {
            is_match = (key <= record.key());
        } else if (record.type() == "int64") {
            is_match = (atol(key.c_str()) <= atol(record.key().c_str()));
        }
    } else if (record.op() == "<") {
        if (record.type() == "string") {
            is_match = (key < record.key());
        } else if (record.type() == "int64") {
            is_match = (atol(key.c_str()) < atol(record.key().c_str()));
        }
    }
    return is_match;
}

bool LogStream::CheckRegex(const std::string& line, const mdt::LogAgentService::Rule& rule) {
    bool is_match = false;
    const mdt::LogAgentService::Expression& expr = rule.expr();

    // only support regex
    if (expr.type() == "regex") {
        try {
            std::string::const_iterator start, end;
            start = line.begin();
            end = line.end();

            boost::regex expression(expr.expr());
            boost::match_results<std::string::const_iterator> watch;
            while (boost::regex_search(start, end, watch, expression)) {
                // check log.y1, log.y2 {==, >=, >, <=, <} rule.x1, rule.x2
                if (watch.size() >= (rule.record_vec_size() + 1)) {
                    VLOG(50) << line << ", watch " << watch[0];
                    for (uint32_t result_idx = 1; (result_idx < watch.size()) && (result_idx <= rule.record_vec_size()); result_idx++) {
                        if (!CheckRecord(watch[result_idx], rule.record_vec(result_idx - 1))) {
                            is_match = false;
                            break;
                        }
                        is_match = true;
                        VLOG(50) << "log.key " << watch[result_idx] << ", expect.key " << rule.record_vec(result_idx - 1).key();
                    }
                }
                start = watch[0].second;
                if (is_match == true) {
                    break;
                }
            }
        } catch (const boost::bad_expression& e) {}
    }
    return is_match;
}

// not support nest json parse
bool LogStream::CheckJson(const std::string& line, const mdt::LogAgentService::Rule& rule) {
    bool is_match = false;
    const mdt::LogAgentService::Expression& expr = rule.expr();

    VLOG(50) << line << ", rule " << rule.DebugString();
    if (expr.type() == "json") {
        LogRecord log;
        std::vector<std::string> str_delim_vec;
        str_delim_vec.push_back(expr.column_delim());
        if (log.SplitLogItem(line, str_delim_vec) >= 0 && log.columns.size() > expr.column_idx()) {
            VLOG(50) << line << ", json " << log.columns[expr.column_idx()];
            std::stringstream ss(log.columns[expr.column_idx()]);
            try {
                boost::property_tree::ptree ptree;
                boost::property_tree::json_parser::read_json(ss, ptree);
                for (uint32_t idx = 0; idx < rule.record_vec_size(); idx++) {
                    std::string item = ptree.get<std::string>(rule.record_vec(idx).key_name());
                    if (!CheckRecord(item, rule.record_vec(idx))) {
                        is_match = false;
                        break;
                    }
                    is_match = true;
                    VLOG(50) << "json.key " << item << ", expect.key " << rule.record_vec(idx).key();
                }
            } catch (boost::property_tree::ptree_error& e) {}
        }
    }
    return is_match;
}

bool LogStream::MonitorHasEvent(const std::string& table_name, const std::string& line) {
    bool is_match = false;

    pthread_spin_lock(&monitor_lock_);
    if (monitor_handler_set_.find(table_name) != monitor_handler_set_.end()) {
        mdt::LogAgentService::RpcMonitorRequest& monitor = monitor_handler_set_[table_name];
        if (monitor.has_rule_set()) {
            const mdt::LogAgentService::RuleInfo& rule_info = monitor.rule_set();

            // 1. parse and check rule
            for (uint32_t idx = 0; idx < rule_info.rule_list_size(); idx++) {
                const mdt::LogAgentService::Rule& rule = rule_info.rule_list(idx);
                if (CheckRegex(line, rule)) {
                    is_match = true;
                    break;
                }
            }

            // 2. parse and check result
            const mdt::LogAgentService::Rule& result = rule_info.result();
            VLOG(50) << "is_match " << is_match;
            is_match = is_match && !CheckJson(line, result);
        }
    }
    pthread_spin_unlock(&monitor_lock_);

    return is_match;
}

//////////////////////////////////////////
//      FileStream implementation       //
//////////////////////////////////////////
FileStream::FileStream(std::string module_name, LogOptions log_options,
                       std::string filename,
                       uint64_t ino,
                       int* success)
    : module_name_(module_name),
    filename_(filename),
    ino_(ino),
    log_options_(log_options) {

    current_offset_ = 0;
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);

    *success = -1;
    int64_t fsize = -1;
    while (1) {
        struct stat stat_buf1;
        uint64_t ino1 = 0;
        if (lstat(filename_.c_str(), &stat_buf1) >= 0) {
            ino1 = (uint64_t)stat_buf1.st_ino;
        }

        fd_ = -1;
        fd_ = open(filename_.c_str(), O_RDONLY);
        fsize = (int64_t)stat_buf1.st_size;

        struct stat stat_buf2;
        uint64_t ino2 = 0;
        if (lstat(filename_.c_str(), &stat_buf2) >= 0) {
            ino2 = (uint64_t)stat_buf2.st_ino;
        }

        if ((ino1 == 0) || (ino1 != ino2)) {
            // rename or delete occur during open
            if (fd_ >= 0) {
                close(fd_);
                fd_ = -1;
                fsize = -1;
            }
            std::string newname;
            if (!InodeToFileName(ino_, filename_, &newname)) {
                // file no exit
                LOG(WARNING) << "file " << filename << "(" << ino << ") has been delete during new filestream";
                return;
            }
            LOG(WARNING) << "rename happen, " << ino_ << ", ino1 " << ino1 << ", ino2 " << ino2
                << ", old file " << filename_ << ", new file " << newname;
            filename_ = newname;
            continue;
        } else {
            // rename safety, check ino
            std::string newname;
            if (ino1 != ino_) { // ino and filename not match
                if (fd_ >= 0) {
                    close(fd_);
                    fd_ = -1;
                    fsize = -1;
                }
                if (!InodeToFileName(ino_, filename_, &newname)) {
                    // file no exit
                    LOG(WARNING) << "file " << filename << "(" << ino << ") has been delete during new filestream";
                    return;
                }
                LOG(WARNING) << "rename safe, but ino not match, " << ino_ << ", ino1 " << ino1 << ", ino2 " << ino2
                    << ", old file " << filename_ << ", new file " << newname;
                filename_ = newname;
                continue;
            }

            // no rename during open, and filename match with ino, open success
            break;
        }
    }
    VLOG(35) << "FileStream new, filename " << filename_ << ", ino " << ino_ << ", fsize " << fsize;

    //fd_ = open(filename.c_str(), O_RDONLY);
    //if (fd_ < 0) {
    //    return;
    //}

    // recovery restart point
    *success = RecoveryCheckPoint(fsize);
    if (*success < 0) {
        close(fd_);
    }
}

// dir, dir/log.bak
bool FileStream::InodeToFileName(uint64_t ino, const std::string& filename, std::string* newname) {
    // get parent dir
    newname->clear();
    std::string dir;
    std::string delim("/");
    std::size_t pos = filename.rfind(delim);
    if (pos != std::string::npos) {
        dir = std::string(filename, 0, pos + 1);

        // list dir
        DIR* dirptr = NULL;
        struct dirent* entry = NULL;
        if ((dirptr = opendir(dir.c_str())) != NULL) {
            while ((entry = readdir(dirptr)) != NULL) {
                std::string fname(entry->d_name);
                if (fname == "." || fname == "..") {
                    continue;
                }
                // find filename match ino
                uint64_t tmp_ino = (uint64_t)entry->d_ino;
                if (tmp_ino == ino) {
                    *newname = dir + fname;
                    closedir(dirptr);
                    return true;
                }

                // subdir, search into it
                if (entry->d_type == DT_DIR) {
                    std::string subdir = dir + fname + "/";
                    if (FindLostInode(ino, subdir, newname)) {
                        closedir(dirptr);
                        return true;
                    }
                }
            }
            closedir(dirptr);
        }
    }
    return false;
}
bool FileStream::FindLostInode(uint64_t ino, const std::string& dir, std::string* newname) {
    newname->clear();

    // list dir
    DIR* dirptr = NULL;
    struct dirent* entry = NULL;
    if ((dirptr = opendir(dir.c_str())) != NULL) {
        while ((entry = readdir(dirptr)) != NULL) {
            // find filename match ino
            uint64_t tmp_ino = (uint64_t)entry->d_ino;
            if (tmp_ino == ino) {
                std::string fname(entry->d_name);
                *newname = dir + fname;
                closedir(dirptr);
                return true;
            }
        }
        closedir(dirptr);
    }
    return false;
}

FileStream::~FileStream() {

}

void FileStream::GetRedoList(std::map<uint64_t, uint64_t>* redo_list) {
    pthread_spin_lock(&lock_);
    swap(*redo_list, redo_list_);
    pthread_spin_unlock(&lock_);
}

// big endian
void FileStream::EncodeUint64BigEndian(uint64_t value, std::string* str) {
    char offset_buf[8];
    EncodeBigEndian(offset_buf, value);
    std::string offset_str(offset_buf, 8);
    *str = offset_str;
}

// key=CurrentOffset'\0'dbname'\0'ino;
// value=offset
void FileStream::MakeCurrentOffsetKey(const std::string& module_name,
                                      const std::string& filename,
                                      uint64_t ino,
                                      uint64_t offset,
                                      std::string* key,
                                      std::string* value) {
    *key = "CurrentOffset";
    key->push_back('\0');
    *key += module_name;
    key->push_back('\0');

    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    if (value) {
        std::string offset_str;
        EncodeUint64BigEndian(offset, &offset_str);
        *value = offset_str;
    }
}

// key=dbname'\0'ino offset, value=size
void FileStream::MakeKeyValue(const std::string& module_name,
                              const std::string& filename,
                              uint64_t ino,
                              uint64_t offset,
                              std::string* key,
                              uint64_t size,
                              std::string* value) {
    *key = "CheckPoint";
    key->push_back('\0');
    *key += module_name;
    key->push_back('\0');
    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    std::string offset_str;
    EncodeUint64BigEndian(offset, &offset_str);
    *key += offset_str;

    if (value) {
        EncodeUint64BigEndian(size, value);
    }
    return;
}

void FileStream::ParseKeyValue(const leveldb::Slice& key,
                               const leveldb::Slice& value,
                               uint64_t* ino,
                               uint64_t* offset, uint64_t* size) {
    int prefixlen = strlen(key.data());
    leveldb::Slice db_slice(key.data() + prefixlen + 1, key.size() - prefixlen - 1);

    int mlen = strlen(db_slice.data());
    leveldb::Slice ino_str = leveldb::Slice(db_slice.data() + mlen + 1, 8);
    *ino = DecodeBigEndain(ino_str.data());

    leveldb::Slice offset_str = leveldb::Slice(db_slice.data() + mlen + 1 + 8, 8);
    *offset = DecodeBigEndain(offset_str.data());

    *size = DecodeBigEndain(value.data());
}

// use leveldb recovery mem cp list
int FileStream::RecoveryCheckPoint(int64_t fsize) {
    int64_t begin_ts = timer::get_micros();
    int64_t end_ts;
    leveldb::WriteBatch batch;
    uint64_t batch_size = 0;

    leveldb::Iterator* db_it = log_options_.db->NewIterator(leveldb::ReadOptions());
    std::string startkey, endkey;
    MakeKeyValue(module_name_, filename_, ino_, 0, &startkey, 0, NULL);
    MakeKeyValue(module_name_, filename_, ino_, 0xffffffffffffffff, &endkey, 0, NULL);
    for (db_it->Seek(startkey);
         db_it->Valid() && db_it->key().ToString() < endkey;
         db_it->Next()) {
        leveldb::Slice key = db_it->key();
        leveldb::Slice value = db_it->value();
        uint64_t offset, size, ino;
        ParseKeyValue(key, value, &ino, &offset, &size);
        VLOG(30) << "recovery cp, offset " << offset << ", size " << size << ", ino " << ino;

        // insert [offset, size] into mem cp list
        pthread_spin_lock(&lock_);
        std::map<uint64_t, uint64_t>::iterator cp_it =  mem_checkpoint_list_.find(offset);
        if (cp_it != mem_checkpoint_list_.end()) {
            uint64_t tmp_size = cp_it->second;
            if (size > tmp_size) {
                cp_it->second = size;
                redo_list_[offset] = size;
            }
        } else {
            mem_checkpoint_list_[offset] = size;
            redo_list_[offset] = size;
        }
        pthread_spin_unlock(&lock_);

        batch.Delete(key);
        batch_size++;

        // update current_offset
        if (current_offset_ < offset + size) {
            current_offset_ = offset + size;
        }
    }
    delete db_it;

    // update current_offset
    std::string offset_key, offset_val;
    MakeCurrentOffsetKey(module_name_, filename_, ino_, 0, &offset_key, NULL);
    leveldb::Status s = log_options_.db->Get(leveldb::ReadOptions(), offset_key, &offset_val);
    if (s.ok()) {
        uint64_t tmp_offset = DecodeBigEndain(offset_val.c_str());
        if (tmp_offset > current_offset_) {
            current_offset_ = tmp_offset;
        }

        batch.Delete(offset_key);
        batch_size++;
    }

    // ******* check ino reuse ********
    bool file_change = true;
    if ((fsize >= 0) && ((uint64_t)fsize >= current_offset_)) {
        // check content key
        std::string ckey, cval;
        MakeContextKey(module_name_, ino_, &ckey, 0, 0, NULL, NULL);
        leveldb::Status cs = log_options_.db->Get(leveldb::ReadOptions(), ckey, &cval);
        if (cs.ok()) {
            uint64_t coffset, csize;
            std::string c2;
            if (ParseContextKey(NULL, NULL, NULL, &coffset, &csize, &c2, &cval)) {
                char* buf = new char[csize];
                ssize_t res = pread(fd_, buf, csize, coffset);
                if ((res >= 0) && ((uint64_t)res == csize)) {
                    std::string c1(buf, csize);
                    VLOG(35) << "file change check, c1 " << c1 << ", c2 " << c2;
                    if (c1 == c2) {
                        file_change = false;
                    }
                }
                delete [] buf;
            } else {
                VLOG(35) << "content key parse error, ino " << ino_ << ", filename " << filename_
                    << ", offset " << current_offset_;
            }

            batch.Delete(ckey);
            batch_size++;
        }
    }

    // file change before restart, clear old file stat
    if (file_change) {
        VLOG(30) << "ino reuse, filename " << filename_ << ", ino " << ino_
            << ", offset " << current_offset_ << ", fsize " << fsize;
        pthread_spin_lock(&lock_);
        mem_checkpoint_list_.clear();
        redo_list_.clear();
        current_offset_ = 0;
        pthread_spin_unlock(&lock_);

        // delete leveldb stat
        if (batch_size) {
            leveldb::Status ds = log_options_.db->Write(leveldb::WriteOptions(), &batch);
            if (!ds.ok()) {
                delete log_options_.db;
                log_options_.db = NULL;
                leveldb::Options options;
                ds = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
                if (!ds.ok()) {
                    LOG(WARNING) << "leveldb reopen errno " << ds.ToString();
                    kleveldb_reopen_fail.Inc();
                }
            }
        }
    }

    end_ts = timer::get_micros();
    VLOG(35) << "file stream recovery, file " << filename_ << ", ino " << ino_
        << ", current_offset " << current_offset_ << ", cost time " << end_ts - begin_ts;
    return 0;
}

void FileStream::ReSetFileStreamCheckPoint() {
    leveldb::Iterator* db_it;
    std::string startkey, endkey;

    VLOG(30) << "log file rename after agent down, file " << filename_;
    pthread_spin_lock(&lock_);
    mem_checkpoint_list_.clear();
    redo_list_.clear();
    current_offset_ = 0;
    pthread_spin_unlock(&lock_);

    // delete cp in leveldb
    db_it = log_options_.db->NewIterator(leveldb::ReadOptions());
    MakeKeyValue(module_name_, filename_, ino_, 0, &startkey, 0, NULL);
    MakeKeyValue(module_name_, filename_, ino_, 0xffffffffffffffff, &endkey, 0, NULL);
    leveldb::Status s;
    for (db_it->Seek(startkey);
            db_it->Valid() && db_it->key().ToString() < endkey;
            db_it->Next()) {
        leveldb::Slice key = db_it->key();
        leveldb::Slice value = db_it->value();
        uint64_t offset, size, ino;
        ParseKeyValue(key, value, &ino, &offset, &size);
        s = log_options_.db->Delete(leveldb::WriteOptions(), key);
        if (!s.ok()) {
            kleveldb_delete_fail.Inc();
            LOG(WARNING) << "delete db checkpoint error, " << filename_ << ", offset " << offset
                << ", size " << size;
            break;
        }
    }
    delete db_it;

    if (!s.ok()) {
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        s = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!s.ok()) {
            LOG(WARNING) << "leveldb reopen errno " << s.ToString();
            kleveldb_reopen_fail.Inc();
        }
    }
    return;
}

void FileStream::GetCheckpoint(DBKey* key, uint64_t* offset, uint64_t* size) {
    *size = 0;
    pthread_spin_lock(&lock_);
    std::map<uint64_t, uint64_t>::iterator it = mem_checkpoint_list_.find(key->offset);
    if (it != mem_checkpoint_list_.end()) {
        *offset = it->first;
        *size = it->second;
    }
    pthread_spin_unlock(&lock_);
}

// if not half line, return size, else return sum of non-half line
ssize_t FileStream::ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec, bool read_half_line) {
    if (size <= 0) {
        return -1;
    }
    ssize_t res = 0;
    int nr_lines = 0;
    std::string str(buf, size);
    boost::split((*line_vec), str, boost::is_any_of("\n"));
    nr_lines = line_vec->size();
    VLOG(30) << "parse line, nr of line " << nr_lines;
    bool half_line = false;
    //if ((buf[size -1] != '\n') || ((*line_vec)[nr_lines - 1].size() == 0)) {
    //    line_vec->pop_back();
    //}
    if ((*line_vec)[nr_lines - 1].size() == 0) {
        // full line case : aaaaaaaaaaaa\nbbbbbbbbbb\nccccccccccccccccc\n
        line_vec->pop_back();
    } else if (buf[size -1] != '\n') {
        // half line case : aaaaaaaaaaaa\nbbbbbbbbbb\nccccccccccccccccc
        half_line = true;
        if (!read_half_line) {
            line_vec->pop_back();
        }
    }
    for (uint32_t i = 0; i < line_vec->size(); i++) {
        if (!half_line || (i != (line_vec->size() - 1))) {
            res += (*line_vec)[i].size() + 1;
        }
        VLOG(70) << "line: " << (*line_vec)[i] << ", res " << res << ", size " << size;
    }
    return res;
}

// key = ContentKey + db + ino, value = offset + size +content
void FileStream::MakeContextKey(const std::string& db_name, uint64_t ino, std::string* key,
                                uint64_t offset, uint64_t size, char* buf, std::string* val) {
    *key = "ContentKey";
    key->push_back('\0');
    *key += db_name;
    key->push_back('\0');
    std::string ino_str;
    EncodeUint64BigEndian(ino, &ino_str);
    *key += ino_str;

    if (val) {
        std::string offset_str;
        EncodeUint64BigEndian(offset, &offset_str);
        std::string size_str;
        EncodeUint64BigEndian(size, &size_str);
        std::string buf_str(buf, 0, size);
        *val = offset_str + size_str + buf_str;
    }
}
bool FileStream::ParseContextKey(std::string* db_name, uint64_t* ino, std::string* key,
                                 uint64_t* offset, uint64_t* size, std::string* buf, std::string* val) {
    if (val) {
        if (val->size() <= (2 * sizeof(uint64_t))) {
            return false;
        }
        *offset = DecodeBigEndain(val->c_str());
        *size = DecodeBigEndain(val->c_str() + sizeof(uint64_t));
        if (*offset != 0 || *size > 1048576) {
            return false;
        }
        leveldb::Slice sli(val->c_str() + 2 * sizeof(uint64_t), val->size() - 2 * sizeof(uint64_t));
        *buf = sli.data();
    }
    return true;
}

// each read granularity is 64KB
// ret: 0, no data
//      -1, file error
//      -2, flow control
int FileStream::Read(std::vector<std::string>* line_vec, DBKey** key) {
    int ret = 0;
    *key = NULL;
    if (fd_ >= 0) {
        // check mem cp pending request
        pthread_spin_lock(&lock_);
        if (mem_checkpoint_list_.size() > (uint32_t)FLAGS_file_stream_max_pending_request) {
            VLOG(30) << "pending overflow, max queue size " << FLAGS_file_stream_max_pending_request << ", cp list size " << mem_checkpoint_list_.size();
            pthread_spin_unlock(&lock_);
            return -2; // need delay retry
        }
        pthread_spin_unlock(&lock_);

        uint64_t size = kReadBlockSize;
        uint64_t offset = current_offset_;
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
        if (res < 0) {
            if (kLastLogWarningTime + 60000000 < timer::get_micros()) {
                kLastLogWarningTime = timer::get_micros();
                LOG(WARNING) << "redo cp, read file error " << filename_ << ", ino " << ino_ << ", offset " << offset
                    << ", size " << size << ", res " << res << ", errno " << errno;
            }
            kfile_read_fail.Inc();
            ret = -1;
        } else if (res == 0) {
            struct stat stat_buf;
            int64_t fsize = -1;
            int64_t fino = -1;
            if (lstat(filename_.c_str(), &stat_buf) >= 0) {
                fsize = (int64_t)(stat_buf.st_size);
                fino = (int64_t)(stat_buf.st_ino);
            }
            VLOG(30) << "read file end: ino " << ino_ << ", offset " << offset << ",file " << filename_
                << ", st ino " << fino << ", st size " << fsize;
            delete [] buf;
            return 0;

        } else {
            uint64_t tmp_res = res;
            res = ParseLine(buf, tmp_res, line_vec, tmp_res < size);
            if ((tmp_res == size) && (res <= 0)) {
                res = -2; // delay retry
            }
        }

        if (res >= 0) {
            // Log content into db, use it after restart to check ino reuse, valid current_offset
            // key:= db+ino, value:=offset+size+content
            if (offset == 0 && res > 0) {
                std::string ckey, cval;
                MakeContextKey(module_name_, ino_, &ckey, offset, res > 1024 ? 1024: res, buf, &cval);
                leveldb::Status cs = log_options_.db->Put(leveldb::WriteOptions(), ckey, cval);
                if (!cs.ok()) {
                    delete log_options_.db;
                    log_options_.db = NULL;
                    leveldb::Options options;
                    cs = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
                    if (!cs.ok()) {
                        LOG(WARNING) << "leveldb reopen errno " << cs.ToString();
                    }
                }
            }

            // Log CheckPoint
            kfile_read_success.Inc();

            *key = new DBKey;
            (*key)->filename = filename_;
            (*key)->ino = ino_;
            (*key)->offset = offset;
            (*key)->ref.Set(0);

            ret = LogCheckPoint(offset, res);
            if (ret < 0) {
                delete (*key);
                line_vec->clear();
            }
        }
        delete [] buf;
    }

    if (fd_ < 0) {
        if (kLastLogWarningTime + 60000000 < timer::get_micros()) {
            kLastLogWarningTime = timer::get_micros();
            LOG(WARNING) << "file error, " << filename_ << ", ino " << ino_;
        }
        kfile_read_fail.Inc();
        ret = -1;
        return ret;

        // file error, try to re-open it
        struct stat stat_buf;
        if (lstat(filename_.c_str(), &stat_buf) >= 0) {
            uint64_t ino = (uint64_t)stat_buf.st_ino;
            if (ino == ino_) {
                OpenFile();
                ret = -2; // delay it
            } else {
                // readdir and find it, ino to file
                ret = -1;
            }
        } else {
            ret = -1; // give up it
        }
    }
    return ret;
}

int FileStream::LogCheckPoint(uint64_t offset, uint64_t size) {
    int ret = 0;
    uint32_t nr_pending;
    pthread_spin_lock(&lock_);
    mem_checkpoint_list_[offset] = size;
    nr_pending = mem_checkpoint_list_.size();
    pthread_spin_unlock(&lock_);

    std::string key, value;
    MakeKeyValue(module_name_, filename_, ino_, offset, &key, size, &value);
    leveldb::Status s = log_options_.db->Put(leveldb::WriteOptions(), key, value);
    if (!s.ok()) {
        // reopen leveldb, flush error
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        s = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!s.ok()) {
            LOG(WARNING) << "leveldb reopen errno " << s.ToString();
            kleveldb_reopen_fail.Inc();
        }

        pthread_spin_lock(&lock_);
        std::map<uint64_t, uint64_t>::iterator it = mem_checkpoint_list_.find(offset);
        if (it != mem_checkpoint_list_.end()) {
            mem_checkpoint_list_.erase(it);
        }
        pthread_spin_unlock(&lock_);

        kleveldb_put_fail.Inc();
        LOG(WARNING) << "log cp into leveldb error, file " << filename_ << ", offset " << offset << ", size " << size << ", err " << s.ToString();
        ret = -1;
    } else {
        kleveldb_put_success.Inc();
        // write db success
        current_offset_ = offset + size;
        ret = size;
        VLOG(30) << "log cp, write leveldb succes, ino " << ino_ << ", file " << filename_ << ", offset "
            << offset << ", size " << size << ", current_offset " << current_offset_
            << ", nr pending " << nr_pending << ", max queue size " << FLAGS_file_stream_max_pending_request;
    }
    return ret;
}

int FileStream::DeleteCheckoutPoint(DBKey* key) {
    // delete mem checkpoint
    bool should_dump_offset = false;
    pthread_spin_lock(&lock_);
    std::map<uint64_t, uint64_t>::iterator it =  mem_checkpoint_list_.find(key->offset);
    if (it != mem_checkpoint_list_.end()) {
        mem_checkpoint_list_.erase(it);
    }
    // dump current offset into db
    if (mem_checkpoint_list_.size() == 0) {
        should_dump_offset = true;
    }
    pthread_spin_unlock(&lock_);

    if (should_dump_offset) {
        // no write occur, TODO: check file end
        std::string offset_key, offset_val;
        MakeCurrentOffsetKey(module_name_, filename_, key->ino, current_offset_, &offset_key, &offset_val);
        VLOG(30) << "Offset Table dump: ino " << key->ino << ", filename " << filename_ << ", offset " << current_offset_;
        leveldb::Status s1 = log_options_.db->Put(leveldb::WriteOptions(), offset_key, offset_val);
        if (!s1.ok()) {
            delete log_options_.db;
            log_options_.db = NULL;
            leveldb::Options options;
            s1 = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
            if (!s1.ok()) {
                LOG(WARNING) << "leveldb reopen errno " << s1.ToString();
                kleveldb_reopen_fail.Inc();
            }
        }
    }

    // delete log checkpoint
    std::string key_str;
    MakeKeyValue(module_name_, filename_, key->ino, key->offset, &key_str, 0, NULL);
    leveldb::Status s = log_options_.db->Delete(leveldb::WriteOptions(), key_str);
    if (!s.ok()) {
        delete log_options_.db;
        log_options_.db = NULL;
        leveldb::Options options;
        s = leveldb::DB::Open(options, log_options_.db_dir.c_str(), &log_options_.db);
        if (!s.ok()) {
            LOG(WARNING) << "leveldb reopen errno " << s.ToString();
            kleveldb_reopen_fail.Inc();
        }

        kleveldb_delete_fail.Inc();
        LOG(WARNING) << "delete db checkpoint error, " << filename_ << ", offset " << key->offset;
    } else {
        kleveldb_delete_success.Inc();
    }
    VLOG(30) << "delete cp, file " << key->filename << ", cp offset " << key->offset << ", ino " << key->ino;
    return 0;
}

int FileStream::CheckPointRead(std::vector<std::string>* line_vec, DBKey** key,
                               uint64_t offset, uint64_t size) {
    int ret = 0;
    *key = NULL;
    struct stat stat_buf1;
    uint64_t st_size = 0;
    if (lstat(filename_.c_str(), &stat_buf1) >= 0) {
        st_size = (uint64_t)stat_buf1.st_size;
    }
    if (size == 0) {
        if (st_size > current_offset_) {
            size = st_size - current_offset_;
            if (size > kReadBlockSize) {
                size = kReadBlockSize;
            }
        } else {
            return ret;
        }
    }
    if (fd_ > 0) {
        kcheckpoint_read_num.Inc();

        VLOG(30) << "file " << filename_ << " read from cp, offset " << offset << ", size " << size;
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
#if 0
        if (res < (int64_t)size) {
            LOG(WARNING) << "redo cp, read file error " << offset << ", size " << size << ", res " << res;
            ret = -1;
        }
#endif
        // file error or read file end
        if (res <= 0) {
            LOG(WARNING) << "redo cp, read file error " << offset << ", size " << size << ", res " << res;
            delete [] buf;
            return 0;
        }
        uint64_t tmp_res = res;
        res = ParseLine(buf, tmp_res, line_vec, tmp_res < size);
        if ((tmp_res == size) && (res <= 0)) {
            LOG(WARNING) << "redo cp, parse buf, size not match, offset " << offset << ", size " << size << ", res " << res;
            ret = -1;
        } else {
            *key = new DBKey;
            (*key)->filename = filename_;
            (*key)->ino = ino_;
            (*key)->offset = offset;
            (*key)->ref.Set(0);
        }
        delete [] buf;
    } else {
        ret = -1;
    }
    return ret;
}

int FileStream::HanleFailKey(DBKey* key) {
    // TODO: mark fail, retry and change channel
    return 1;
}

// two case:
//  1) evict from fd cache
//  2) delete file event
int FileStream::MarkDelete() {
    pthread_spin_lock(&lock_);
    uint32_t nr_pending = mem_checkpoint_list_.size();
    pthread_spin_unlock(&lock_);

    if (nr_pending == 0) {
        VLOG(30) << "delete file stream: ino " << ino_ << ", filename " <<  filename_ << ", offset " << current_offset_;
        close(fd_);
        fd_ = -1;
        return 1;
    }
    return -1;
}

int FileStream::OpenFile() {
    if (fd_ < 0) {
        fd_ = open(filename_.c_str(), O_RDONLY);
        if (fd_ < 0) {
            return -1;
        }
    }
    return 0;
}

void FileStream::Profile(FileStreamProfile* profile) {
    profile->ino = ino_;
    profile->filename = filename_;
    pthread_spin_lock(&lock_);
    profile->nr_pending = mem_checkpoint_list_.size();
    pthread_spin_unlock(&lock_);
    profile->current_offset = current_offset_;
}

}
}

