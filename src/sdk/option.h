#ifndef MDT_SDK_OPTION_H_
#define MDT_SDK_OPTION_H_

#include "util/env.h"
#include <tera.h>

namespace mdt {

struct FilesystemOptions {
    std::string fs_path_; // db_name + FileSystem
    Env* env_;
};

struct TeraOptions {
    std::string root_path_; // path of tera dir
    std::string tera_flag_; // path of tera.flag
    tera::Client* client_;
    std::vector<tera::Client*> extra_client_; // N - 1

    // schema table(kv), key = table_name, value = BigQueryTableSchema (define in kv.proto)
    std::string schema_table_name_;
    tera::Table* schema_table_;
};

}

#endif // MDT_SDK_OPTION_H_
