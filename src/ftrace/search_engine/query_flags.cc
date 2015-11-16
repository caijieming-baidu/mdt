#include <gflags/gflags.h>

DEFINE_int32(se_num_threads, 10, "num of thread handle search req");
DEFINE_string(se_service_port, "12390", "listen port for query service");
DEFINE_bool(mdt_flagfile_set, false, "force user set mdt.flag");
