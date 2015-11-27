#include <gflags/gflags.h>

DEFINE_uint64(max_text_annotation_size, 10240, "max single text annotation size");
DEFINE_uint64(log_level, 20, "log level");

DEFINE_bool(use_mdt_flag, false, "determine use mdt.flag or not");
DEFINE_string(mdt_flagfile, "../conf/mdt.flag", "locate mdt.flag file");

DEFINE_bool(enable_debug_put_pb, false, "debug pb put in callback");
DEFINE_string(mdt_server_addr, "0.0.0.0:12390", "mdt server addr");
