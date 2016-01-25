#include <sys/inotify.h>
#include "agent/options.h"
#include <gflags/gflags.h>

///////////////////////////////////////////
// agent flags
///////////////////////////////////////////
mdt::agent::EventMask event_masks[] = {
    {IN_ACCESS        , "IN_ACCESS"}        ,
    {IN_ATTRIB        , "IN_ATTRIB"}        ,
    {IN_CLOSE_WRITE   , "IN_CLOSE_WRITE"}   ,
    {IN_CLOSE_NOWRITE , "IN_CLOSE_NOWRITE"} ,
    {IN_CREATE        , "IN_CREATE"}        ,
    {IN_DELETE        , "IN_DELETE"}        ,
    {IN_DELETE_SELF   , "IN_DELETE_SELF"}   ,
    {IN_MODIFY        , "IN_MODIFY"}        ,
    {IN_MOVE_SELF     , "IN_MOVE_SELF"}     ,
    {IN_MOVED_FROM    , "IN_MOVED_FROM"}    ,
    {IN_MOVED_TO      , "IN_MOVED_TO"}      ,
    {IN_OPEN          , "IN_OPEN"}          ,
    {IN_DONT_FOLLOW   , "IN_DONT_FOLLOW"}   ,
    {IN_EXCL_UNLINK   , "IN_EXCL_UNLINK"}   ,
    {IN_MASK_ADD      , "IN_MASK_ADD"}      ,
    {IN_ONESHOT       , "IN_ONESHOT"}       ,
    {IN_ONLYDIR       , "IN_ONLYDIR"}       ,
    {IN_IGNORED       , "IN_IGNORED"}       ,
    {IN_ISDIR         , "IN_ISDIR"}         ,
    {IN_Q_OVERFLOW    , "IN_Q_OVERFLOW"}    ,
    {IN_UNMOUNT       , "IN_UNMOUNT"}       ,
};

DEFINE_string(agent_service_port, "33331", "agent port");
DEFINE_int32(file_stream_max_pending_request, 1000, "max pending write req");
//DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler server addr");
DEFINE_string(db_dir, "../leveldb_dir/", "leveldb dir for cp");
DEFINE_string(watch_log_dir, "../watch_log_dir/", "log dir");
DEFINE_string(module_name_list, "tabletnode.1.", "identify module name");

DEFINE_string(db_name, "mdttrace-debug", "production name");
DEFINE_string(table_name, "trace", "table name");
DEFINE_string(primary_key, "", "primary key name");
DEFINE_string(user_time, "", "user point out which field use as timestamp");
DEFINE_int32(time_type, 1, "use for parse user time from log");

DEFINE_int32(parser_type, 0, "parser type: 0 = kv self; 1 = fixed index");
// split string by substring
//DEFINE_string(string_delims, "||", "split string by substring");
DEFINE_string(string_delims, "", "split string by substring");

// split string by char
DEFINE_string(line_delims, " ", "log tailer's line delim");
DEFINE_string(kv_delims, "=", "log tailer's kv delim");
DEFINE_bool(enable_index_filter, false, "do not filter log line by index list in agent");
DEFINE_string(index_list, "", "index table name list");
DEFINE_string(alias_index_list, "", "alias index table name list");

// split string by index number
DEFINE_bool(use_fixed_index_list, true, "use fixed index list");
DEFINE_string(fixed_index_list, "url:5,time:2", "use for fix index list match");

DEFINE_int64(delay_retry_time, 1000000, "in second, time period after async push fail to retry");

///////////////////////////////////////////
// scheduler flags
///////////////////////////////////////////
DEFINE_string(scheduler_service_port, "11111", "scheduler service port");
DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler service addr");
DEFINE_int32(agent_timeout, 60, "agent info will be delete after x second");
DEFINE_int32(agent_qps_quota, 10000, "max qps agent can be use per second");
DEFINE_int32(agent_bandwidth_quota, 20000000, "max bandwidth agent can be use per second");

DEFINE_int32(collector_timeout, 60000000, "collector info will be delete after x us");
DEFINE_int32(collector_max_error, 10, "max error can occur in collector");

///////////////////////////////////////////
// scheduler flags
///////////////////////////////////////////
DEFINE_int32(se_num_threads, 10, "num of thread handle search req");
DEFINE_string(se_service_port, "22221", "listen port for query service");
//DEFINE_string(scheduler_addr, "0.0.0.0:11111", "scheduler service addr");
DEFINE_bool(mdt_flagfile_set, false, "force user set mdt.flag");
DEFINE_string(mdt_flagfile, "../conf/trace.flag", "search service flagfile");


