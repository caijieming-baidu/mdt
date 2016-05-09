#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <boost/regex.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <vector>
#include <set>

#include "mail/mail.h"
#include "sdk/db.h"
#include "sdk/sdk.h"
#include "sdk/table.h"
#include "utils/counter.h"
#include "utils/env.h"
#include "utils/mutex.h"
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(op, "", "method:dumpfile, create, get_index");
DEFINE_string(logfile, "xxx.dat", "log file");
DEFINE_string(dbname, "TEST_db", "production name");
DEFINE_string(tablename, "TEST_table001", "table name");
DEFINE_string(index_list, "passuid,deviceType,mobile,sign,pageType", "index table name list");
DEFINE_string(alias_index_list, "", "alias index table name list");
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

    uint32_t ParseKVpairs(const std::string& line, const std::string& linedelims,
                          const std::string& kvdelims,
                          const std::map<std::string, std::string>& alias_index_map) {
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
                std::map<std::string, std::string>::const_iterator it = alias_index_map.find(kv[0]);
                if (it != alias_index_map.end()) {
                    logkv.insert(std::pair<std::string, std::string>(it->second, kv[1]));
                }
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

int ParseJson() {
    std::string str = "{\"code\":0,\"images\":[{\"url\":\"fmn057/20111221/1130/head_kJoO_05d9000251de125c.jpg\"},{\"url\":\"fmn057/20111221/1130/original_kJoO_05d9000251de125c.jpg\"}]}";
    using namespace boost::property_tree;

    std::stringstream ss(str);
    ptree pt;
    try{
        read_json(ss, pt);
    }
    catch(ptree_error & e) {
        return 1;
    }

    try{
        int code = pt.get<int>("code");   // 得到"code"的value
        std::cout << code << std::endl;
        ptree image_array = pt.get_child("images");  // get_child得到数组对象

        // 遍历数组
        BOOST_FOREACH(boost::property_tree::ptree::value_type &v, image_array) {
            std::stringstream s;
            write_json(s, v.second);
            std::string image_item = s.str();

            ptree& child = v.second;
            std::string url = child.get<std::string>("url");
            std::cout << url << std::endl;
        }
    }
    catch (ptree_error & e) {
        return 2;
    }

    return 0;
}

int ParseNuomiJson() {
    //std::string result = "{\"errno\":-1,\"state\":0}";
    std::string result = "{\"isBlackUser\":0,\"leftDealNums\":{\"10192999\":6,\"10450500\":6},\"leftOrderNum\":8,\"totalDealNum\":6,\"totalOrderNum\":8}";
    std::stringstream nuomi_ss(result);
    boost::property_tree::ptree nuomi_ptree;
    boost::property_tree::json_parser::read_json(nuomi_ss, nuomi_ptree);
    std::string key_errno = "isBlackUser";
    std::string key_state = "totalOrderNum";
    int err;
    int state;
    try {
        err = nuomi_ptree.get<int>(key_errno);
        state = nuomi_ptree.get<int>(key_state);

        std::cout << "[]errno " << err << ", state " << state
            << std::endl;
        boost::property_tree::ptree dealnum = nuomi_ptree.get_child("leftDealNums");
        int nr1 = dealnum.get<int>("10192999");

        std::cout << "nr1 " << nr1 << std::endl;
    } catch (boost::property_tree::ptree_error& e) {}

    return 0;
}

int Regex() {
    std::cout << "#####################\n";
    std::string s = "Boost Libraries";
    std::string expr_str = "(\\w+)\\s(\\w+)";
    boost::regex expr(expr_str, boost::regex::icase);
    boost::smatch what;
    if (boost::regex_search(s, what, expr)) {
        std::cout << what[0] << '\n';
        std::cout << what[1] << "_" << what[2] << '\n';
    }

    try {
        std::string log = "NOTICE: 04-14 15:01:47: [IsHandler.php:124]  id=ea320df700000c0a ip=170929444 qid[77777] errno[0] ip[10.48.45.36] logId[2777884466] uri[/s?ie=utf-8&csq=1&pstg=20&mod=2&isbd=1&cqid=ffbe06aa0000b1f5&istc=873&ver=Qw5hSQEJ_bLaje7b5_OGy39X1bB-XSqKCIO&chk=570f405b&isid=6DC6BC0C18623253&ie=utf-8&f=8&rsv_bp=1&tn=baidu&wd=3DM%E4%B8%8B%E6%B8%B8%E6%88%8F%E9%82%A3%E4%B8%AA4MB%E7%9A%84%E6%98%AF%E5%95%A5%E5%95%8A&oq=%26lt%3B&rsv_pq=c50f9e69000090ff&rsv_t=53d7kH3LQki7rZDG6DcXcN5oL4%2Bz7hMNYk%2FB97kliXoueXjgOA8%2FtdZbhTY&rsv_enter=1&rsv_sug3=30&rsv_sug1=22&rsv_sug7=100&rsv_sug2=0&inputT=16427&rsv_sug4=16427&bs=3&f4s=1&_ck=231668.0.-1.-1.-1.-1.-1&isnop=1&rsv_stat=-2&rsv_bp=1] time_used[384.23] qid[cb86d84d0000335b] HHVM[1] time_pre_init[0.2] time_require_init[1.74] time_load_conf[0.09] time_InitProcessor[1.86] ui_query[3DM下游戏那个4MB的是啥啊] time_InputParser[0.46] tpl_sel[1] time_TplSelector[0.54] templateType[baidu] templateName[baidu] tnGroupName[organic] language[zh-CN] platform[pc] cook_enable[0] chunked_flag[0] split_tpl_flag[0] time_us_ready[0.01] time_unpack[0.15] us_cost[379.96] us_ret[11] sids[1425_19554_18241_19690_17942_19559_15159_11590] word[3DM下游戏那个4MB的是啥啊] is_mod[2] is_isbd[1] is_cqid[ffbe06aa0000b1f5] is_isid[6DC6BC0C18623253] is_chk[570f405b] is_ver[ffbe06aa0000b1f51460617306] is_f4s[0] is_predict_status[0] is_csq[1] is_pstg[20] is_querysign[] is_predict_module[0] login[0] baiduId[6DC6BC8E449486201AC89CC41340C186] ua[Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36] is_status[0] is_rd[0] is_swtime[0] time_used_self[384.13] hsug[0_0.01] time_ReqUsProcessor[380.23] processing request is ok";
        //std::string expr2 = "\\sqid\\[([a-z|0-9|A-Z]+)\\]\\s";
        //std::string expr2 = "\\sid=([a-z|0-9|A-Z]+)\\s";
        std::string expr2 = "\\sid=(\\d+)\\s";
        boost::regex qid_reg(expr2);
        boost::smatch swatch;
        if (boost::regex_search(log, swatch, qid_reg)) {
            if (swatch.size()) {
                std::cout << "size " << swatch.size() << swatch[0] << '\n';
                std::cout << swatch[1] << ", " << swatch[2] << '\n';
            }
        }

        std::string::const_iterator start, end;
        start = log.begin();
        end = log.end();
        boost::match_results<std::string::const_iterator> result_it;
        while (boost::regex_search(start, end, result_it, qid_reg)) {
            for (uint32_t i = 1; i < result_it.size(); i++) {
                std::cout << "result: " << result_it[i] << '\n';
            }
            start = result_it[0].second;
        }

    } catch (const boost::bad_expression& e) {

    }

    std::cout << "#####################\n";
    return 0;
}

int SendMail(const char *to, const char *from, const char *subject, const char *message) {
    int retval = -1;
    FILE *mailpipe = popen("/usr/sbin/sendmail -t ", "w");
    if (mailpipe != NULL) {
        fprintf(mailpipe, "To: %s\n", to);
        //fprintf(mailpipe, "From: %s\n", from);
        fprintf(mailpipe, "Subject: %s\n\n", subject);
        fwrite(message, 1, strlen(message), mailpipe);
        fwrite(".\n", 1, 2, mailpipe);
        pclose(mailpipe);
        retval = 0;
    }
    else {
        perror("Failed to invoke sendmail");
    }
    return retval;
}

void LineDelim() {
    std::string linedelims = " \t";
    std::string line = "abc def\tijk ubj\tkfc\tgg";
    std::vector<std::string> kvpairs;
    boost::split(kvpairs, line, boost::is_any_of(linedelims));
    for (uint32_t i = 0; i < kvpairs.size(); i++) {
        std::cout << kvpairs[i] << std::endl;
    }
}

// ./dump_file --flagfile=../conf/mdt.flag --op=create --index_list=passuid,mobile --dbname=TEST_db --tablename=TEST_table001
// ./dump_file --flagfile=../conf/mdt.flag --op=dumpfile --primary_key=id --index_list=passuid,mobile --dbname=TEST_db --tablename=TEST_table001 --logfile=xxxx.dat
// ./dump_file --flagfile=../conf/mdt.flag --op=get_index --logfile=xxxx.dat
int main(int ac, char* av[])
{
    LineDelim();
    ParseNuomiJson();
    ParseJson();
    Regex();

    // send mail
    std::string to = "caijieming@baidu.com";
    std::string from = "caijieming@baidu.com";
    std::string subject = "test";
    std::string message = "hi all:\n\tcaijieming.\n";
    ::mdt::Mail mail;
    mail.SendMail(to, from, subject, message);

    // parse mdt.flag
    ::google::ParseCommandLineFlags(&ac, &av, true);
    std::string db_name = FLAGS_dbname;
    std::string table_name = FLAGS_tablename;

    std::cout << "conf " << FLAGS_flagfile << std::endl;
    if (access(FLAGS_flagfile.c_str(), F_OK)) {
        std::cout << " ***** WRITE TEST: use default param *****\n";
    }

    // split alias index
    std::map<std::string, std::string> alias_index_map;
    if (FLAGS_alias_index_list.size() != 0) {
        std::vector<std::string> alias_index_vec;
        boost::split(alias_index_vec, FLAGS_alias_index_list, boost::is_any_of(";"));
        std::cout << "DEBUG: split alias index tablet\n";
        for (int i = 0; i < (int)alias_index_vec.size(); i++) {
            std::vector<std::string> alias_vec;
            boost::split(alias_vec, alias_index_vec[i], boost::is_any_of(":"));
            if ((alias_vec.size() >= 2) && alias_vec[1].size()) {
                std::vector<std::string> alias;
                boost::split(alias, alias_vec[1], boost::is_any_of(","));
                alias_index_map.insert(std::pair<std::string, std::string>(alias_vec[0], alias_vec[0]));
                std::cout << "=====> index: " << alias_vec[0] << std::endl;
                for (int j = 0; j < (int)alias.size(); j++) {
                    alias_index_map.insert(std::pair<std::string, std::string>(alias[j], alias_vec[0]));
                    std::cout << "parse alias list: " << alias[j] << std::endl;
                }
            }
        }
    }

    // split index
    std::vector<std::string> log_columns;
    if (FLAGS_index_list.size() != 0) {
        boost::split(log_columns, FLAGS_index_list, boost::is_any_of(","));
        std::cout << "DEBUG: split index table\n";
        for (int i = 0 ; i < (int)log_columns.size(); i++) {
            alias_index_map.insert(std::pair<std::string, std::string>(log_columns[i], log_columns[i]));
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
    } else if (FLAGS_op == "get_index2") {
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
                kv.ParseKVpairs(log.columns[i], FLAGS_logtailer_linedelims, FLAGS_logtailer_kvdelims, alias_index_map);
            }
            std::map<std::string, std::string>::iterator it = kv.kv_annotation.begin();
            for (; it != kv.kv_annotation.end(); ++it) {
                possible_index.insert(it->first);
            }
            if (line_no % 1 == 0) {
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

