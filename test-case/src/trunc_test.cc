// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <unistd.h>

#include <fstream>
#include <iostream>
#include <vector>

#include <com_log.h>

int main(int ac, char* av[]) {
    com_loadlog("../conf/", "trunc_test.conf");
    //com_writelog(COMLOG_WARNING, "crew url http://www.baidu.com, time %lu, reason %s", 10000, "ubm test");

    std::string sample_file = "../conf/log.sample";
    std::ifstream ifile(sample_file.c_str(), std::ifstream::in);
    std::string line;
    std::vector<std::string> line_vec;
    while (getline(ifile, line)) {
        line_vec.push_back(line);
    }

    while (1) {
        // 25KB per file
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
        for (unsigned int i = 0; i < line_vec.size(); i++) {
            com_writelog(COMLOG_NOTICE, "%s", line_vec[i].c_str());
        }
        usleep(100000);
    }
    return 0;
}

