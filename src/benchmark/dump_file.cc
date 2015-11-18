#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <set>

#include "sdk/db.h"
#include "sdk/sdk.h"
#include "sdk/table.h"
#include "util/counter.h"
#include "util/env.h"
#include "util/mutex.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(op, "", "method:dumpfile, create, get_index");
DEFINE_string(logfile, "xxx.dat", "log file");
DEFINE_string(dbname, "TEST_db", "production name");
DEFINE_string(tablename, "TEST_table001", "table name");
DEFINE_string(index_list, "", "index table name list");
DEFINE_string(primary_key, "", "primary key name");
// split string by substring
DEFINE_string(string_delims, "||", "split string by substring");
// split string by char
DEFINE_string(logtailer_linedelims, "&", "log tailer's line delim");
DEFINE_string(logtailer_kvdelims, "=", "log tailer's kv delim");
DECLARE_string(flagfile);

///////////////////////////////////////////////////
//  TimesTamp, CostTime,
//  ResultCode, ResultData, SelfDefineLog,
//  RequestURL, RequestMethod, RequestData
///////////////////////////////////////////////////
/*
std::string log_columns[] = {
    "TimeStamp",
    "CostTime",
    "ResultCode",
    "ResultData",
    "SelfDefineLog",
    "RequestURL",
    "RequestMethod",
    "RequestData",
};
*/
// split string by substring
struct LogRecord {
    std::vector<std::string> columns;

    void Print() {
        std::cout << "LogRecord\n";
        for (int i = 0 ; i < (int)columns.size(); i++) {
            std::cout << "\t" << columns[i] << std::endl;
        }
    }

    int SplitLogItem(const std::string& str, const std::vector<std::string>& dim_vec) {
        std::size_t pos = 0, prev = 0;
        while (1) {
            std::size_t min_pos = std::string::npos;
            pos = min_pos;
            int min_idx = (int)(1 << 20);
            for (int i = 0; i < (int)dim_vec.size(); i++) {
                const std::string& dim = dim_vec[i];
                min_pos = str.find(dim, prev);
                if ((pos == std::string::npos) || (min_pos != std::string::npos && pos > min_pos)) {
                    pos = min_pos;
                    min_idx = i;
                }
            }
            if (pos > prev) {
                columns.push_back(str.substr(prev, pos - prev));
            }
            if ((pos == std::string::npos) || (min_idx == (int)(1 << 20))) {
                break;
            }
            prev = pos + dim_vec[min_idx].size();
        }
        return 0;
    }
};

std::string Uint64ToString(uint64_t val) {
    std::ostringstream os;
    os << val;
    return os.str();
}

void StoreCallback_Test(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    delete request;
    delete response;
}

// kv parser
// split string by char
struct LogTailerSpan {
    std::map<std::string, std::string> kv_annotation;

    uint32_t ParseKVpairs(const std::string& line, const std::string& linedelims,
                          const std::string& kvdelims) {
        uint32_t size = line.size();
        if (size == 0) return 0;

        std::vector<std::string> linevec;
        boost::split(linevec, line, boost::is_any_of("\n"));
        if (linevec.size() == 0 || linevec[0].size() == 0) return 0;
        //if (linevec[0].at(linevec.size() - 1) != '\n') return 0;

        std::map<std::string, std::string>& logkv = kv_annotation;
        //logkv.clear();
        std::vector<std::string> kvpairs;
        boost::split(kvpairs, linevec[0], boost::is_any_of(linedelims));
        for (uint32_t i = 0; i < kvpairs.size(); i++) {
            const std::string& kvpair = kvpairs[i];
            std::vector<std::string> kv;
            boost::split(kv, kvpair, boost::is_any_of(kvdelims));
            if (kv.size() == 2 && kv[0].size() > 0 && kv[1].size() > 0) {
                logkv.insert(std::pair<std::string, std::string>(kv[0], kv[1]));
            }
        }
        return linevec[0].size() + 1;
    }

    void PrintKVpairs() {
        const std::map<std::string, std::string>& logkv = kv_annotation;
        std::cout << "LogSpan kv: ";
        std::map<std::string, std::string>::const_iterator it = logkv.begin();
        for (; it != logkv.end(); ++it) {
            std::cout << "[" << it->first << ":" << it->second << "]  ";
        }
        std::cout << std::endl;
    }
};

// ./dump_file --flagfile=../conf/mdt.flag --op=create --index_list=passuid,mobile --dbname=TEST_db --tablename=TEST_table001
// ./dump_file --flagfile=../conf/mdt.flag --op=dumpfile --primary_key=id --index_list=passuid,mobile --dbname=TEST_db --tablename=TEST_table001 --logfile=xxxx.dat
// ./dump_file --flagfile=../conf/mdt.flag --op=get_index --logfile=xxxx.dat
int main(int ac, char* av[])
{
    // parse mdt.flag
    ::google::ParseCommandLineFlags(&ac, &av, true);
    std::string db_name = FLAGS_dbname;
    std::string table_name = FLAGS_tablename;

    std::cout << "conf " << FLAGS_flagfile << std::endl;
    if (access(FLAGS_flagfile.c_str(), F_OK)) {
        std::cout << " ***** WRITE TEST: use default param *****\n";
    }

    // split index
    std::vector<std::string> log_columns;
    if (FLAGS_index_list.size() != 0) {
        boost::split(log_columns, FLAGS_index_list, boost::is_any_of(","));
        std::cout << "DEBUG: split index table\n";
        for (int i = 0 ; i < (int)log_columns.size(); i++) {
            std::cout << log_columns[i] << "\t:\t";
        }
        std::cout << std::endl;
    }
    // split string delims
    std::vector<std::string> string_delims;
    if (FLAGS_string_delims.size() != 0) {
        boost::split(string_delims, FLAGS_string_delims, boost::is_any_of(","));
        std::cout << "DEBUG: get string delims\n";
        for (int i = 0; i < (int)string_delims.size(); i++) {
            std::cout << string_delims[i] << "\t:\t";
        }
        std::cout << std::endl;
    }

    if (FLAGS_op == "create") {
        // sanity check
        if (db_name.size() == 0) {
            std::cout << "create: miss dbname\n";
            return -1;
        }
        if (table_name.size() == 0) {
            std::cout << "create: miss table_name\n";
            return -1;
        }

        // open database
        std::cout << "open db ..." << std::endl;
        mdt::Database* db;
        db = mdt::OpenDatabase(db_name);

        // create table
        std::cout << "create table ..." << std::endl;
        mdt::TableDescription table_desc;
        table_desc.table_name = table_name;
        table_desc.primary_key_type = mdt::kBytes;

        for (int i = 0; i < (int)log_columns.size(); i++) {
            mdt::IndexDescription index_table;
            index_table.index_name = log_columns[i];
            index_table.index_key_type = mdt::kBytes;
            table_desc.index_descriptor_list.push_back(index_table);
        }
        CreateTable(db, table_desc);
    } else if (FLAGS_op == "get_index") {
        std::set<std::string> possible_index;
        std::ifstream file(FLAGS_logfile.c_str());
        std::string str;
        uint64_t line_no = 0;
        while (std::getline(file, str)) {
            line_no++;
            // string split
            LogRecord log;
            if (log.SplitLogItem(str, string_delims) < 0) {
                continue;
            }
            // char split
            LogTailerSpan kv;
            for (int i = 0; i < (int)log.columns.size(); i++) {
                kv.ParseKVpairs(log.columns[i], FLAGS_logtailer_linedelims, FLAGS_logtailer_kvdelims);
            }
            std::map<std::string, std::string>::iterator it = kv.kv_annotation.begin();
            for (; it != kv.kv_annotation.end(); ++it) {
                possible_index.insert(it->first);
            }
            if (line_no % 10 == 0) {
                std::set<std::string>::iterator it_set = possible_index.begin();
                std::cout << "<<<<< ";
                for (; it_set != possible_index.end(); ++it_set) {
                    std::cout << "\t" << *it_set;
                }
                std::cout << " >>>>>" << std::endl;
                kv.PrintKVpairs();
            }
        }
    } else if (FLAGS_op == "dumpfile") {
        // open database
        std::cout << "open db ..." << std::endl;
        mdt::Database* db;
        db = mdt::OpenDatabase(db_name);

        // open table
        std::cout << "open table ..." << std::endl;
        mdt::Table* table;
        table = OpenTable(db, table_name);

        std::cout << "dumpfile ..." << std::endl;
        std::ifstream file(FLAGS_logfile.c_str());
        std::string filename(FLAGS_logfile);
        std::string str;
        uint64_t line_no = 0;
        while (std::getline(file, str)) {
            line_no++;
            // string split
            LogRecord log;
            if (log.SplitLogItem(str, string_delims) < 0) {
                continue;
            }
            // char split
            LogTailerSpan kv;
            for (int i = 0; i < (int)log.columns.size(); i++) {
                kv.ParseKVpairs(log.columns[i], FLAGS_logtailer_linedelims, FLAGS_logtailer_kvdelims);
            }
            // construct req
            mdt::StoreRequest* req = new mdt::StoreRequest();
            for (int i = 0; i < (int)log_columns.size(); i++) {
                std::map<std::string, std::string>::iterator it = kv.kv_annotation.find(log_columns[i]);
                if (it != kv.kv_annotation.end()) {
                    if (log_columns[i].size() == 0) {
                        continue;
                    }
                    if (log_columns[i] == FLAGS_primary_key) {
                        req->primary_key = it->second;
                    } else {
                        mdt::Index query;
                        query.index_name = it->first;
                        query.index_key = it->second;
                        req->index_list.push_back(query);
                    }
                }
            }
            if (req->primary_key.size() == 0) {
                req->primary_key = filename + ":" + Uint64ToString(line_no - 1);
            }
            req->data = str;
            req->timestamp = time(NULL);

            mdt::StoreResponse* resp = new mdt::StoreResponse();
            mdt::StoreCallback TestCallback = StoreCallback_Test;
            table->Put(req, resp, TestCallback, NULL);
        }
        std::cout << "read finish...., wait\n";
        usleep(180000);
    } else {
        std::cout << "op: " << FLAGS_op << ", not support\n";
        return -1;
    }
    return 0;
}

