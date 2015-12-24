// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_MULTI_TABLE_IMPL_H_
#define  MDT_SDK_NULTI_TABLE_IMPL_H_

#include <gflags/gflags.h>
#include <vector>
#include "sdk/multi_table_impl.h"
#include "sdk/table_impl.h"

DECLARE_int32(multi_table_nr);

namespace mdt {

MultiTableImpl::MultiTableImpl() {
    cur_table_id_ = 0;
    table_array_.clear();
}

MultiTableImpl::~MultiTableImpl() {
    // do some cleanup
}

Status MultiTableImpl::OpenTable(const std::string& db_name, const TeraOptions& tera_opt,
                                 const FilesystemOptions& fs_opt, const TableDescription& table_desc,
                                 Table** table_ptr) {
    if (FLAGS_multi_table_nr <= 0) {
        return Status::NotFound("mdt.flag error");
    }
    MultiTableImpl* multi_table = new MultiTableImpl;
    for (int i = 0; i < FLAGS_multi_table_nr; i++) {
        Table* table;
        TeraOptions opt = tera_opt;
        opt.client_ = opt.extra_client_[i];
        TableImpl::OpenTable(db_name, opt, fs_opt, table_desc, &table);
        if (table == NULL) {
            return Status::NotFound("table open error ");
        }
        multi_table->table_array_.push_back(table);
    }
    *table_ptr = multi_table;
    return Status::OK();
}

int MultiTableImpl::BatchWrite(BatchWriteContext* ctx) {
    if (table_array_.size() == 0) {
        return -1;
    }
    Table* table = table_array_[Shard(cur_table_id_++)];
    if (table == NULL) {
        return -1;
    }
    return table->BatchWrite(ctx);
}

int MultiTableImpl::Put(const StoreRequest* request, StoreResponse* response,
                        StoreCallback callback, void* callback_param) {
    if (table_array_.size() == 0) {
        return -1;
    }
    Table* table = table_array_[Shard(cur_table_id_++)];
    if (table == NULL) {
        return -1;
    }
    return table->Put(request, response, callback, callback_param);
}

Status MultiTableImpl::Get(const SearchRequest* request, SearchResponse* response,
                           SearchCallback callback, void* callback_param) {
    if (table_array_.size() == 0) {
        return Status::IOError("no such table");
    }
    Table* table = table_array_[Shard(cur_table_id_++)];
    if (table == NULL) {
        return Status::NotFound("table not found");
    }
    return table->Get(request, response, callback, callback_param);
}

const std::string& MultiTableImpl::TableName() {
    return (table_array_[0])->TableName();
}

uint32_t MultiTableImpl::Shard(uint32_t i) {
    if (FLAGS_multi_table_nr <= 0) {
        return 0;
    }
    return (i % FLAGS_multi_table_nr);
}

}
#endif

