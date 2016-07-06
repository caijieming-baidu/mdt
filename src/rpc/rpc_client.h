// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef RPC_RPC_CLIENT_H_
#define RPC_RPC_CLIENT_H_

#include <pthread.h>

#include <map>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>

#include "utils/mutex.h"
#include "utils/status.h"

namespace mdt {

class RpcClient {
public:
    RpcClient() {
        ::sofa::pbrpc::RpcClientOptions options;
        options.max_pending_buffer_size = 20;
        _rpc_client = new sofa::pbrpc::RpcClient(options);
        //pthread_spinlock_init(&_server_map_lock, PTHREAD_PROCESS_SHARED);
    }
    ~RpcClient() {
        //delete _rpc_client;
        //pthread_spinlock_destory(&_server_map_lock);
    }

    template <class ServiceType>
    Status GetMethodList(const std::string& server_addr, ServiceType** service) {
        ::sofa::pbrpc::RpcChannel* channel = NULL;
        //pthread_spin_lock(&_server_map_lock);
        _server_map_lock.Lock();
        ServerMap::iterator it = _server_map.find(server_addr);
        if (it == _server_map.end()) {
            ::sofa::pbrpc::RpcChannelOptions options;
            channel = new ::sofa::pbrpc::RpcChannel(_rpc_client, server_addr, options);
            _server_map[server_addr] = channel;
        } else {
            channel = it->second;
        }
        //pthread_spin_unlock(&_server_map_lock);
        _server_map_lock.Unlock();
        *service = new ServiceType(channel);
        return Status::OK();
    }

    // default timeout 3000ms, rpc_timeout (in second)
    template <class ServiceType, class Request, class Response, class Callback>
    Status SyncCall(ServiceType* service, void(ServiceType::*func)(
                    ::google::protobuf::RpcController*,
                    const Request*, Response*, Callback*),
                    const Request* request, Response* response,
                    int32_t rpc_timeout = 10, int32_t retry_times = 3) {
        ::sofa::pbrpc::RpcController ctrl;
        ctrl.SetTimeout(rpc_timeout * 1000L);
        for (int i = 0; i < retry_times; i++) {
            (service->*func)(&ctrl, request, response, NULL);
            if (!ctrl.Failed()) {
                return Status::OK();
            }
            ctrl.Reset();
        }
        return Status::OK();
    }

    template <class ServiceType, class Request, class Response, class Callback>
    Status AsyncCall(ServiceType* service, void(ServiceType::*func)(
                     ::google::protobuf::RpcController*,
                     const Request*, Response*, Callback*),
                     const Request* request, Response* response,
                     boost::function<void (const Request*, Response*, bool, int)> callback,
                     int32_t rpc_timeout = 600, int32_t retry_times = 3) {
        ::sofa::pbrpc::RpcController* ctrl = new ::sofa::pbrpc::RpcController();
        ctrl->SetTimeout(rpc_timeout * 1000L);
        ::google::protobuf::Closure* done =
            ::sofa::pbrpc::NewClosure(&RpcClient::template RpcCallback<Request, Response, Callback>,
                                      ctrl, request, response, callback);
        (service->*func)(ctrl, request, response, done);
        return Status::OK();
    }

    template <class Request, class Response, class Callback>
    static void RpcCallback(::sofa::pbrpc::RpcController* controller,
                            const Request* request, Response* response,
                            boost::function<void (const Request*, Response*, bool, int)> callback) {
        bool failed = controller->Failed();
        int error = controller->ErrorCode();
        delete controller;
        callback(request, response, failed, error);
    }

private:
    ::sofa::pbrpc::RpcClient* _rpc_client;
    typedef std::map<std::string, ::sofa::pbrpc::RpcChannel*> ServerMap;
    ///pthread_spinlock_t _server_map_lock;
    Mutex _server_map_lock;
    ServerMap _server_map;
};

}
#endif
