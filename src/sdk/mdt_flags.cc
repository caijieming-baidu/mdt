#include <gflags/gflags.h>

// nfs cluster location
DEFINE_string(env_fs_type, "dfs", "file system type");
DEFINE_string(env_nfs_mountpoint, "/disk/tera", "nfs mount point");
DEFINE_string(env_nfs_conf_path, "./conf/nfs.conf", "nfs's client configure file");
DEFINE_string(env_nfs_so_path, "./lib/libnfs.so", "nfs's client's .so lib");

// tera cluster location
DEFINE_string(tera_flag_file_path, "./conf/tera.flag", "tera's client's configure file");
DEFINE_string(tera_root_dir, "/disk/tera/trace-sys/", "tera cluster's dir");

// mdt cluster
DEFINE_string(database_root_dir, "/disk/tera/DatabaseDir", "database's data file dir");

// write ops param
DEFINE_int64(concurrent_write_handle_num, 10, "num of fs writer");
DEFINE_int64(max_write_handle_seq, 10, "max num of req can schedule to current write_handle");
DEFINE_int64(data_size_per_sync, 0, "num of data per Sync()");
DEFINE_bool(use_tera_async_write, true, "if true, use tera async write");
DEFINE_int64(write_batch_queue_size, 0, "the num of write request can batch in single thread");
DEFINE_int64(request_queue_flush_internal, 10, "the number of ms wait before flush request");
DEFINE_int64(max_timestamp_table_num, 10,  "num of timestamp index table");

// read ops param
DEFINE_int64(read_file_thread_num, 50,  "num of read file threads");
DEFINE_int64(cleaner_thread_num, 5,  "num of cleaner threads");
// read row with timestamp
DEFINE_bool(enable_multi_version_read, false, "enable search specify time version primary key row");
DEFINE_bool(read_by_index_filter, true, "read by index filter instead of index merger");
DEFINE_bool(enable_scan_control, false, "if true, batch scan in limit number");
DEFINE_int64(batch_scan_buffer_size, 1048576, "scan buffer size");
DEFINE_bool(enable_qu_range, true, "use qu to filter");
DEFINE_int64(tera_scan_pack_interval, 50000000, "scan timeout in one round");
DEFINE_bool(enable_number_limit, true, "use scan number limit");
DEFINE_int64(scan_number_limit, 30, "number of scan limit");

DEFINE_bool(enable_async_read, true, "use multithread for async filesystem read");
DEFINE_int64(async_read_thread_num, 50,  "num of async read threads");

