// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <gflags/gflags.h>

DEFINE_string(agent_service_port, "33331", "agent port");
DEFINE_int32(file_stream_max_pending_request, 500, "max pending write req");
//DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler server addr");
DEFINE_string(db_dir, "../leveldb_dir/", "leveldb dir for cp");
DEFINE_string(watch_log_dir, "../watch_log_dir/", "log dir");
DEFINE_string(module_name_list, "tabletnode.1.", "identify module name");

DEFINE_string(db_name, "mdttrace-debug", "production name");
DEFINE_string(table_name, "trace", "table name");
DEFINE_string(primary_key, "", "primary key name");
DEFINE_string(user_time, "", "user point out which field use as timestamp");
DEFINE_int32(time_type, 1, "use for parse user time from log");
DEFINE_int64(agent_max_fd_num, 30, "max fd can be hold in log agent");

// support line filter
DEFINE_string(string_line_filter_list, "", "support line filter, like: cn_garbage_main,diff_garbage_main");

// split string by substring
//DEFINE_string(string_delims, "||", "split string by substring");
DEFINE_string(string_delims, "", "split string by substring");

// split string by char
DEFINE_string(line_delims, " \t", "log tailer's line delim");
DEFINE_string(kv_delims, "=", "log tailer's kv delim");
DEFINE_bool(enable_index_filter, false, "do not filter log line by index list in agent");
DEFINE_string(index_list, "", "index table name list");
DEFINE_string(alias_index_list, "", "alias index table name list");

// split string by index number
DEFINE_bool(use_fixed_index_list, true, "use fixed index list");
DEFINE_string(fixed_index_list, "url:5,time:2", "use for fix index list match");

DEFINE_int64(delay_retry_time, 2000, "in ms, time period after async push fail to retry");

DEFINE_bool(use_regex_index_pattern, true, "new regex index search");

///////////////////////////////////////////
// scheduler flags
///////////////////////////////////////////
DEFINE_string(scheduler_service_port, "11111", "scheduler service port");
DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler service addr");
DEFINE_int32(agent_timeout, 8000000, "agent info will be delete after x second");
DEFINE_int32(agent_qps_quota, 10000, "max qps agent can be use per second");
DEFINE_int32(agent_bandwidth_quota, 20000000, "max bandwidth agent can be use per second");

DEFINE_int32(collector_timeout, 8000000, "collector info will be delete after x us");
DEFINE_int32(collector_max_error, 10000, "max error can occur in collector");

DEFINE_int64(scheduler_galaxy_app_trace_period, 300000, "in (ms) update task trace path info");
DEFINE_int64(scheduler_mail_max_queue_size, 30, "max mail queue size");
DEFINE_int64(scheduler_mail_delay, 5000, "delay (ms) time in send mail");

DEFINE_int64(cache_size, 104857600, "leveldb block cache size");
DEFINE_bool(scheduler_use_qps_schedule, true, "scheduler use qps for alloc collector");

///////////////////////////////////////////
// scheduler flags
///////////////////////////////////////////
DEFINE_int32(se_num_threads, 10, "num of thread handle search req");
DEFINE_string(se_service_port, "22221", "listen port for query service");
//DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler service addr");
DEFINE_bool(mdt_flagfile_set, false, "force user set mdt.flag");
DEFINE_string(mdt_flagfile, "../conf/trace.flag", "search service flagfile");

///////////////////////////////////////////
// collector flags
///////////////////////////////////////////
DEFINE_int64(collector_rpc_work_thread, 12, "nr or server rpc thread");
DEFINE_int64(collector_rpc_nr_io_server, 2, "nr of worker thread");
DEFINE_int64(collector_rpc_max_pending_size, 5, "MB, in server response buffer size");
DEFINE_int64(collector_store_max_pending, 10000, "store thread max pending");
DEFINE_int64(collector_tera_max_pending, 5000, "tera index thread max pending");
DEFINE_int64(collector_ktera_pending_count, 100000, "tera max pending write before callback is call");

