// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "sdk/db.h"
#include "sdk/sdk.h"
#include "sdk/table.h"

namespace mdt {

// 打开数据库
Database* OpenDatabase(const std::string& db_name) {
    Database* db = NULL;
    Status s = Database::OpenDB(db_name, &db);
    return db;
}

// 关闭数据库
void CloseDatabase(Database* db) {
    delete db;
}

// 打开表格
Table* OpenTable(Database* db, const std::string& table_name) {
    Table* table = NULL;
    Status s = db->OpenTable(table_name, &table);
    return table;
}

// 关闭表格
void CloseTable(Table* table) {
    //delete table;
}

// 写入接口
void Put(Table* table, const StoreRequest* request, StoreResponse* response,
         StoreCallback callback, void* callback_param) {
    table->Put(request, response, callback, callback_param);
}

void BatchWrite(Table* table, BatchWriteContext* ctx) {
    table->BatchWrite(ctx);
}

// 查询接口
void Get(Table* table, const SearchRequest* request, SearchResponse* response,
         SearchCallback callback, void* callback_param) {
    table->Get(request, response, callback, callback_param);
}

// 建表接口
int CreateTable(Database* db, const TableDescription& table_desc) {
    return !db->CreateTable(table_desc).ok();
}

} // namespace mdt
