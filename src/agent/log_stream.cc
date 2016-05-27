#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/regex.hpp>

#include "proto/scheduler.pb.h"
#include "agent/log_stream.h"
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <pthread.h>
#include "agent/log_stream.h"
#include "agent/options.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "utils/coding.h"
#include <sys/time.h>

#include "leveldb/slice.h"
#include "leveldb/db.h"
#include "leveldb/status.h"
#include "proto/query.pb.h"

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

namespace mdt {
namespace agent {

static int64_t kLastLogWarningTime;

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

    last_update_time_ = mdt::timer::get_micros();
    pthread_create(&tid_, NULL, LogStreamWrapper, this);
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
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.find(key->ino);
            FileStream* file_stream = NULL;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;

                file_stream->kreq_success.Inc();
            }
            local_key_queue.pop();

            // last one delete and free space
            if (key->ref.Dec() == 0) {
                if (file_stream) {
                    file_stream->DeleteCheckoutPoint(key);
                }
                VLOG(30) << "delete key, file " << key->filename << ", key " << (uint64_t)key << ", offset " << key->offset;
                delete key;
            }
        }

        // handle fail push callback
        while (!local_failed_key_queue.empty()) {
            DBKey* key = local_failed_key_queue.front();
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.find(key->ino);
            FileStream* file_stream = NULL;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;

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

        // handle write event
        std::map<uint64_t, std::string>::iterator write_it = local_write_event.begin();
        for(; write_it != local_write_event.end(); ++write_it) {
            uint64_t ino = write_it->first;
            const std::string& filename = write_it->second;
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.find(ino);
            FileStream* file_stream;
            if (file_it != file_streams_.end()) {
                file_stream = file_it->second;
                if (filename != file_stream->GetFileName()) {
                    // file has been rename
                    VLOG(30) << "file " << file_stream->GetFileName() << " has been rename, new name " << filename
                        << ", ino " << ino;
                    file_stream->SetFileName(filename);
                }
                file_stream->OpenFile();
            } else {
                // first log file add, check max fd wether is overflow
                pthread_spin_lock(server_addr_lock_);
                if (info_->nr_file_streams > FLAGS_agent_max_fd_num) {
                    // fd overflow, ignore write event
                    info_->history_fd_overflow_count++;
                    pthread_spin_unlock(server_addr_lock_);
                    continue;
                }
                pthread_spin_unlock(server_addr_lock_);

                // new file stream
                int success;
                file_stream = new FileStream(module_name_, log_options_, filename, ino, &success);
                if (success < 0) {
                    // TODO: do something
                    LOG(WARNING) << "new file stream " << filename << ", faile";
                }
                file_streams_[ino] = file_stream;
                // may take a little long
                ApplyRedoList(file_stream);
            }
            DBKey* key;
            std::vector<std::string> line_vec;
            if (file_stream->Read(&line_vec, &key) > 0) {
                std::vector<mdt::SearchEngine::RpcStoreRequest*> req_vec;
                std::string table_name;
                std::vector<std::string> monitor_vec;
                GetTableName(file_stream->GetFileName(), &table_name);
                ParseMdtRequest(table_name, line_vec, &req_vec, &monitor_vec);
                AsyncPush(req_vec, key);
                AsyncPushMonitor(table_name, monitor_vec);

                // read next offset
                AddWriteEvent(filename, ino);
            } else {
                VLOG(30) << "has write event, but read nothing";
            }
        }

        // handle delete event
        std::map<uint64_t, std::string>::iterator delete_it = local_delete_event.begin();
        for(; delete_it != local_delete_event.end(); ++delete_it) {
            uint64_t ino = delete_it->first;
            const std::string& filename = delete_it->second;
            std::map<uint64_t, FileStream*>::iterator file_it = file_streams_.find(ino);
            if (file_it != file_streams_.end()) {
                VLOG(30) << "delete filestream, " << filename;
                FileStream* file_stream = file_it->second;
                // if no one refer this file stream, then delete it
                if (file_stream->MarkDelete() >= 0) {
                    // TODO: need delete file_stream ??
                    file_streams_.erase(file_it);
                    delete file_stream;
                } else {
                    // add into delete queue without wakeup thread
                    ThreadPool::Task task =
                        boost::bind(&LogStream::DeleteWatchEvent, this, filename, ino, false);
                    fail_delay_thread_.DelayTask(FLAGS_delay_retry_time, task);
                    //DeleteWatchEvent(filename, ino, false);
                }
            }
        }

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
        }

        pthread_spin_lock(server_addr_lock_);
        info_->nr_file_streams = (int64_t)file_streams_.size();
        if (curr_pending_req > 0) {
            info_->curr_pending_req = curr_pending_req;
        }
        pthread_spin_unlock(server_addr_lock_);
    }
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

std::string LogStream::TimeToString(struct timeval* filetime) {
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

// type 1: sec + micro sec
uint64_t LogStream::ParseTime(const std::string& time_str) {
    if (time_type_ == 1) {
        return (uint64_t)atol(time_str.c_str());
    }
    return 0;
}

// NOTICE: parse log line, add monitor logic
// case 1: key1=001,key2=002,key3=003||key4=004,key005=005,key006=006
// case 2: 001 002 003 004 005 006
// case 3: key1 001, key2 002, key3 003, key4 004, key5 005, key6 006
int LogStream::ParseMdtRequest(const std::string table_name,
                               std::vector<std::string>& line_vec,
                               std::vector<mdt::SearchEngine::RpcStoreRequest* >* req_vec,
                               std::vector<std::string>* monitor_vec) {
    for (uint32_t i = 0; i < line_vec.size(); i++) {
        int res = 0;
        std::string& line  = line_vec[i];

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

        // support monitor
        if (MonitorHasEvent(table_name, line)) {
            monitor_vec->push_back(line);
        }

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
        mdt::SearchEngine::RpcStoreRequest* req = NULL;
        LogRecord log;
        if (log.SplitLogItem(line, string_delims_) < 0) {
            res = -1;
            VLOG(30) << "parse mdt request split by string fail: " << line;
        } else {
            LogTailerSpan kv;
            for (int col_idx = 0; col_idx < (int)log.columns.size(); col_idx++) {
                if (use_fixed_index_list_) {
                    kv.ParseFixedKvPairs(log.columns[col_idx], line_delims_, fixed_index_list_);
                } else {
                    //kv.ParseKVpairs(log.columns[col_idx], line_delims_, kv_delims_, index_list_);
                    kv.ParseKVpairs(log.columns[col_idx], line_delims_, kv_delims_, alias_index_map_);
                }
            }

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
            // if primary key not set, use time
            if (req->primary_key() == "") {
                struct timeval dummy_time;
                req->set_primary_key(TimeToString(&dummy_time));
            }
            req->set_data(line);
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
    VLOG(30) << "begin use redo list to re-send log data";
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
    pthread_spin_lock(server_addr_lock_);
    std::string server_addr = info_->collector_addr;
    info_->qps_use += req_vec.size();
    pthread_spin_unlock(server_addr_lock_);
    VLOG(30) << "async send data to " << server_addr << ", nr req " << req_vec.size() << ", offset " << key->offset;

    mdt::SearchEngine::SearchEngineService_Stub* service;
    if (req_vec.size() == 0) {
        // assume async send success.
        kseq_nonsend_num.Inc();

        key->ref.Set(1);
        mdt::SearchEngine::RpcStoreRequest* req = new mdt::SearchEngine::RpcStoreRequest;
        mdt::SearchEngine::RpcStoreResponse* resp = new mdt::SearchEngine::RpcStoreResponse;
        rpc_client_->GetMethodList(server_addr, &service);
        VLOG(30) << "assume async send success, file " << key->filename << ", offset " << key->offset
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
            LOG(WARNING) << "async write error " << error << ", key " << (uint64_t)key << ", file " << key->filename << " add to failed event queue"
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

        VLOG(30) << "file " << key->filename << " add to success event queue, req "
            << (uint64_t)req << ", resp " << (uint64_t)resp << ", key " << (uint64_t)key << ", key.ref " << key->ref.Get() << ", offset " << key->offset;
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

int LogStream::AddWriteEvent(std::string filename) {
    struct stat stat_buf;
    if (lstat(filename.c_str(), &stat_buf) == -1) {
        return 0;
    }
    uint64_t ino = (uint64_t)stat_buf.st_ino;
    VLOG(35) << "ino " << ino << ", file " << filename << " add to write event queue";

    pthread_spin_lock(&lock_);
    write_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    pthread_spin_unlock(&lock_);
    thread_event_.Set();
    return 0;
}

int LogStream::AddWriteEvent(std::string filename, uint64_t ino) {
    VLOG(35) << "ino " << ino << ", file " << filename << " add to write event queue";

    pthread_spin_lock(&lock_);
    write_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
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
    VLOG(30) << "file " << filename << " add to delete event queue, wakup " << need_wakeup;

    pthread_spin_lock(&lock_);
    delete_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
    pthread_spin_unlock(&lock_);
    if (need_wakeup) {
        thread_event_.Set();
    }
    return 0;
}

int LogStream::DeleteWatchEvent(std::string filename, uint64_t ino, bool need_wakeup) {
    VLOG(30) << "file " << filename << " add to delete event queue, wakup " << need_wakeup;

    pthread_spin_lock(&lock_);
    delete_event_.insert(std::pair<uint64_t, std::string>(ino, filename));
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
            }
        }
        if (req->primary_key() == "") {
            struct timeval dummy_time;
            req->set_primary_key(TimeToString(&dummy_time));
        }
        if (req->timestamp() == 0) {
            req->set_timestamp(mdt::timer::get_micros());
        }
        req->set_data(line);
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
    *success = -1;
    fd_ = open(filename.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return;
    }
    current_offset_ = 0;
    pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);

    // recovery restart point
    *success = RecoveryCheckPoint();
    if (*success < 0) {
        close(fd_);
    }
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

void FileStream::MakeKeyValue(const std::string& module_name,
                               const std::string& filename,
                               uint64_t offset,
                               std::string* key,
                               uint64_t size,
                               std::string* value) {
    *key = module_name;
    key->push_back('\0');
    *key += filename;
    key->push_back('\0');

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
                               uint64_t* offset, uint64_t* size) {
    int mlen = strlen(key.data());
    //leveldb::Slice module_name = leveldb::Slice(key.data(), mlen);
    int flen = strlen(key.data() + mlen + 1);
    //leveldb::Slice filename = leveldb::Slice(key.data() + mlen + 1, flen);
    leveldb::Slice offset_str = leveldb::Slice(key.data() + mlen + 1 + flen + 1, 8);
    *offset = DecodeBigEndain(offset_str.data());
    *size = DecodeBigEndain(value.data());
}

// use leveldb recovery mem cp list
int FileStream::RecoveryCheckPoint() {
    int64_t begin_ts = timer::get_micros();
    int64_t end_ts;

    struct stat stat_buf;
    if (lstat(filename_.c_str(), &stat_buf) == -1) {
        return -1;
    }

    leveldb::Iterator* db_it = log_options_.db->NewIterator(leveldb::ReadOptions());
    uint64_t file_size = stat_buf.st_size;
    bool file_rename = false;

    std::string startkey, endkey;
    MakeKeyValue(module_name_, filename_, 0, &startkey, 0, NULL);
    MakeKeyValue(module_name_, filename_, 0xffffffffffffffff, &endkey, 0, NULL);
    for (db_it->Seek(startkey);
         db_it->Valid() && db_it->key().ToString() < endkey;
         db_it->Next()) {
        leveldb::Slice key = db_it->key();
        leveldb::Slice value = db_it->value();
        uint64_t offset, size;
        ParseKeyValue(key, value, &offset, &size);
        VLOG(30) << "recovery cp, offset " << offset << ", size " << size;

        if (file_rename || (file_size < (offset + size))) {
            // file has been rename
            file_rename = true;
            break;
        }

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

        // update current_offset
        if (current_offset_ < offset + size) {
            current_offset_ = offset + size;
        }
    }
    delete db_it;

    // clear old file stat
    if (file_rename) {
        VLOG(30) << "log file rename after agent down, file " << filename_;
        pthread_spin_lock(&lock_);
        mem_checkpoint_list_.clear();
        redo_list_.clear();
        current_offset_ = 0;
        pthread_spin_unlock(&lock_);

        // delete cp in leveldb
        db_it = log_options_.db->NewIterator(leveldb::ReadOptions());
        MakeKeyValue(module_name_, filename_, 0, &startkey, 0, NULL);
        MakeKeyValue(module_name_, filename_, 0xffffffffffffffff, &endkey, 0, NULL);
        leveldb::Status s;
        for (db_it->Seek(startkey);
                db_it->Valid() && db_it->key().ToString() < endkey;
                db_it->Next()) {
            leveldb::Slice key = db_it->key();
            leveldb::Slice value = db_it->value();
            uint64_t offset, size;
            ParseKeyValue(key, value, &offset, &size);
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
    }

    end_ts = timer::get_micros();
    VLOG(50) << "recovery " << filename_ << ", cost time " << end_ts - begin_ts;
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
    MakeKeyValue(module_name_, filename_, 0, &startkey, 0, NULL);
    MakeKeyValue(module_name_, filename_, 0xffffffffffffffff, &endkey, 0, NULL);
    leveldb::Status s;
    for (db_it->Seek(startkey);
            db_it->Valid() && db_it->key().ToString() < endkey;
            db_it->Next()) {
        leveldb::Slice key = db_it->key();
        leveldb::Slice value = db_it->value();
        uint64_t offset, size;
        ParseKeyValue(key, value, &offset, &size);
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

ssize_t FileStream::ParseLine(char* buf, ssize_t size, std::vector<std::string>* line_vec) {
    if (size <= 0) {
        return -1;
    }
    ssize_t res = 0;
    int nr_lines = 0;
    std::string str(buf, size);
    boost::split((*line_vec), str, boost::is_any_of("\n"));
    nr_lines = line_vec->size();
    VLOG(30) << "parse line, nr of line " << nr_lines;
    if ((buf[size -1] != '\n') || ((*line_vec)[nr_lines - 1].size() == 0)) {
        line_vec->pop_back();
    }
    for (uint32_t i = 0; i < line_vec->size(); i++) {
        res += (*line_vec)[i].size() + 1;
        VLOG(70) << "line: " << (*line_vec)[i] << ", res " << res << ", size " << size;
    }
    return res;
}

// each read granularity is 64KB
int FileStream::Read(std::vector<std::string>* line_vec, DBKey** key) {
    int ret = 0;
    if (fd_ > 0) {
        // check mem cp pending request
        pthread_spin_lock(&lock_);
        if (mem_checkpoint_list_.size() > (uint32_t)FLAGS_file_stream_max_pending_request) {
            VLOG(30) << "pending overflow, max queue size " << FLAGS_file_stream_max_pending_request << ", cp list size " << mem_checkpoint_list_.size();
            pthread_spin_unlock(&lock_);
            return -1;
        }
        pthread_spin_unlock(&lock_);

        uint64_t size = 65536;
        uint64_t offset = current_offset_;
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
        if (res < 0) {
            if (kLastLogWarningTime + 60000000 < timer::get_micros()) {
                kLastLogWarningTime = timer::get_micros();
                LOG(WARNING) << "redo cp, read file error, offset " << offset << ", size " << size << ", res " << res;
            }
            kfile_read_fail.Inc();
            ret = -1;
        } else if (res == 0) {
           VLOG(30) << "file " << filename_ << ", read size 0";
        } else {
            res = ParseLine(buf, res, line_vec);
        }
        delete buf;

        if (res <= 0) {
            ret = -1;
        } else {
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
    MakeKeyValue(module_name_, filename_, offset, &key, size, &value);
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
        VLOG(30) << "log cp, write leveldb succes, file " << filename_ << ", offset "
            << offset << ", size " << size << ", current_offset " << current_offset_
            << ", nr pending " << nr_pending << ", max queue size " << FLAGS_file_stream_max_pending_request;
    }
    return ret;
}

int FileStream::DeleteCheckoutPoint(DBKey* key) {
    // delete mem checkpoint
    pthread_spin_lock(&lock_);
    std::map<uint64_t, uint64_t>::iterator it =  mem_checkpoint_list_.find(key->offset);
    if (it != mem_checkpoint_list_.end()) {
        mem_checkpoint_list_.erase(it);
    }
    pthread_spin_unlock(&lock_);

    // delete log checkpoint
    std::string key_str;
    MakeKeyValue(module_name_, filename_, key->offset, &key_str, 0, NULL);
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
    VLOG(30) << "delete cp, file " << key->filename << ", cp offset " << key->offset;
    return 0;
}

int FileStream::CheckPointRead(std::vector<std::string>* line_vec, DBKey** key,
                               uint64_t offset, uint64_t size) {
    int ret = 0;
    if (fd_ > 0) {
        kcheckpoint_read_num.Inc();

        VLOG(30) << "file " << filename_ << " read from cp, offset " << offset << ", size " << size;
        char* buf = new char[size];
        ssize_t res = pread(fd_, buf, size, offset);
        if (res < (int64_t)size) {
            LOG(WARNING) << "redo cp, read file error " << offset << ", size " << size << ", res " << res;
            ret = -1;
        }
        res = ParseLine(buf, res, line_vec);
        if (res != (int64_t)size) {
            LOG(WARNING) << "redo cp, parse buf, size not match, offset " << offset << ", size " << size << ", res " << res;
            ret = -1;
        } else {
            *key = new DBKey;
            (*key)->filename = filename_;
            (*key)->ino = ino_;
            (*key)->offset = offset;
            (*key)->ref.Set(0);
        }
        delete buf;
    } else {
        ret = -1;
    }
    return ret;
}

int FileStream::HanleFailKey(DBKey* key) {
    // TODO: mark fail, retry and change channel
    return 1;
}

int FileStream::MarkDelete() {
    pthread_spin_lock(&lock_);
    uint32_t nr_pending = mem_checkpoint_list_.size();
    pthread_spin_unlock(&lock_);

    VLOG(30) << "delete file stream " << filename_ << ", nr_pending " << nr_pending;
    if (nr_pending == 0) {
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

