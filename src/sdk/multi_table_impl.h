// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_MULTI_TABLE_IMPL_H_
#define  MDT_SDK_NULTI_TABLE_IMPL_H_

#include <gflags/gflags.h>
#include <vector>
#include "sdk/table.h"
#include "sdk/sdk.h"
#include "sdk/option.h"

namespace mdt {

class MultiTableImpl : public Table {
public:
    MultiTableImpl();
    ~MultiTableImpl();
    int BatchWrite(BatchWriteContext* ctx);
    int Put(const StoreRequest* request, StoreResponse* response,
            StoreCallback callback = NULL, void* callback_param = NULL);
    Status Get(const SearchRequest* request, SearchResponse* response,
               SearchCallback callback = NULL, void* callback_param = NULL);
    const std::string& TableName();
    static Status OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                            const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                            Table** table_ptr);

private:
    uint32_t Shard(uint32_t i);

private:
    std::vector<Table*> table_array_;
    uint64_t cur_table_id_;
};

}
#endif

