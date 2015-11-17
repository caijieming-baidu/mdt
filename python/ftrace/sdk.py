# -*- coding:utf-8 -*-
# Copyright (c) 2015, Galaxy Authors. All Rights Reserved
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file.

import datetime
import logging
from sofa.pbrpc import client
from ftrace import query_pb2
LOG = logging.getLogger('ftrace')

class FtraceSDK(object):
    def __init__(self, addr):
        self.channel = client.Channel(addr)

    def simple_query(self, db, 
                          table,
                          id,
                          time_from, 
                          time_to,
                          limit = 100):
        if not db or not table or not id:
            return [], False
        ftrace = query_pb2.SearchEngineService_Stub(self.channel)
        controller = client.Controller()
        controller.SetTimeout(5)
        request = query_pb2.RpcSearchRequest()
        request.db_name = db
        request.table_name = table
        request.primary_key = id
        request.start_timestamp = time_from
        request.end_timestamp = time_to
        request.limit = limit
        response = ftrace.Search(controller, request)
        return response.result_list, True

def FtraceSDK_test_example():
    cli = FtraceSDK("127.0.0.1:12390")
    data_list, flag = cli.simple_query("TEST_db", "TEST_table001", "user_paytype_2015-11-16.dat:1", 0, 1457699592, 100)
    print len(data_list)
    print data_list[0]

FtraceSDK_test_example()

