// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef  MDT_SDK_DB_IMPL_H_
#define  MDT_SDK_DB_IMPL_H_

#include <tera.h>
#include "util/env.h"
#include "sdk/db.h"
#include "sdk/table_impl.h"

namespace mdt {

///////////////////////////////////
//      DatabaseImpl class       //
///////////////////////////////////
class DatabaseImpl : public Database {
public:
    DatabaseImpl(const Options& options, const std::string& db_name);
    ~DatabaseImpl();

    static Status OpenDB(const std::string& db_name, Database** db_ptr);

    virtual Status CreateTable(const TableDescription& table_desc);
    virtual Status OpenTable(const std::string& table_name, Table** table_ptr);

    std::string& DatabaseName() {return db_name_;}

private:
    Status Init();
    static Status CreateDB(const Options& options, const std::string& db_name, Database** db_ptr);

    int InternalCreateTable(const TableDescription& table_desc, Table** table_ptr);
    Status ReleaseTables();

    static int AssembleTableSchema(const TableDescription& table_desc,
                                   BigQueryTableSchema* schema);
    static int DisassembleTableSchema(const BigQueryTableSchema& schema,
                                      TableDescription* table_desc);

    DatabaseImpl(const DatabaseImpl&);
    void operator=(const DatabaseImpl&);

private:
    std::string db_name_;
    const Options options_;
    FilesystemOptions fs_opt_;
    TeraOptions tera_opt_;
    TeraAdapter tera_adapter_;
    std::map<std::string, Table*> table_map_; // <table_name, table ptr>
};

// TableImpl relative

} // namespace mdt

#endif  // MDT_SDK_DB_IMPL_H_
