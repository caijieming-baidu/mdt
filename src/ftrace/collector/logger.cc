#include <stdarg.h>
#include <sstream>

#include <gflags/gflags.h>
#include "ftrace/collector/logger.h"
#include "ftrace/collector/trace.h"
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <sofa/pbrpc/pbrpc.h>
#include "proto/query.pb.h"

DECLARE_uint64(max_text_annotation_size);
DECLARE_bool(enable_debug_put_pb);
DECLARE_string(mdt_server_addr);

namespace mdt {

void InitTraceModule(const std::string& flagfile) {
    TraceModule::InitTraceModule(flagfile);
}

void OpenTrace(uint64_t trace_id,
               uint64_t parent_span_id,
               uint64_t span_id,
               const std::string& name,
               const std::string& trace_name) {
    Trace::OpenTrace(trace_id, parent_span_id, span_id, name, trace_name);
}

void AttachTrace(uint64_t attach_id) {
    Trace::AttachTrace(attach_id);
}

void DetachAndOpenTrace(uint64_t attach_id) {
    Trace::DetachAndOpenTrace(attach_id);
}

void ReleaseTrace() {
    Trace::ReleaseTrace();
}

/*
TraceIdentify GetTraceIdentify() {
    TraceIdentify id;
    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return id;
    }
    id.CopyFrom(trace->GetTraceID());
    return id;
}
*/

void Log(int level, const char* fmt, ...) {
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::Log(level, anno_buf);
    return;
}

void KvLog(int level, const char* value, const char* fmt, ...) {
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace::KvLog(level, value, anno_buf);
    return;
}

// LOG interface for galaxy
void OpenProtoBufLog(const std::string& dbname, const std::string& tablename) {
    TraceModule::OpenProtoBufLog(dbname, tablename);
}

void CloseProtoBufLog(const std::string& dbname, const std::string& tablename) {
    TraceModule::CloseProtoBufLog(dbname, tablename);
}

void StoreCallbackDummy(::sofa::pbrpc::RpcController* ctrl,
                         ::mdt::SearchEngine::RpcStoreRequest* req,
                         ::mdt::SearchEngine::RpcStoreResponse* resp) {
    delete ctrl;
    delete req;
    delete resp;
}

inline int64_t get_micros() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message) {
    const ::google::protobuf::Descriptor* descriptor = message->GetDescriptor();
    const std::string& tablename = descriptor->name();
    const std::string& fullname = descriptor->full_name();
    std::string dbname(fullname, 0, fullname.size() - tablename.size() - 1);

    // store pb, init rpc
    ::sofa::pbrpc::RpcChannelOptions channel_options;
    ::sofa::pbrpc::RpcChannel channel(&TraceModule::rpc_client, FLAGS_mdt_server_addr, channel_options);
    ::sofa::pbrpc::RpcController* ctrl = new ::sofa::pbrpc::RpcController();
    ctrl->SetTimeout(3000);
    ::mdt::SearchEngine::RpcStoreRequest* req = new ::mdt::SearchEngine::RpcStoreRequest();
    ::mdt::SearchEngine::RpcStoreResponse* resp = new ::mdt::SearchEngine::RpcStoreResponse();
    ::google::protobuf::Closure* done = ::sofa::pbrpc::NewClosure(
                                &StoreCallbackDummy, ctrl, req, resp);
    ::mdt::SearchEngine::SearchEngineService_Stub service(&channel);

    // prepare request
    req->set_db_name(dbname);
    req->set_table_name(tablename);
    bool log_valid = false;
    for (int i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::FieldDescriptor* field = descriptor->field(i);
        const ::google::protobuf::Reflection* reflection = message->GetReflection();
        if (field == NULL ||
            (field->label() == ::google::protobuf::FieldDescriptor::LABEL_REPEATED) ||
            !reflection->HasField(*message, field)) continue;
        // set primary key
        if (primary_key_name == field->name()) {
            req->set_primary_key(TraceModule::GetFieldValue(message, field));
            req->set_timestamp(get_micros());
            if (message->SerializeToString(req->mutable_data())) {
                log_valid = true;
            } else {
                std::cout << "serialstring fail\n";
            }
        } else {
            ::mdt::SearchEngine::RpcStoreIndex* idx = req->add_index_list();
            idx->set_index_table(field->name());
            idx->set_key(TraceModule::GetFieldValue(message, field));
        }
    }
    if (log_valid) {
        service.Store(ctrl, req, resp, done);
    } else {
        StoreCallbackDummy(ctrl, req, resp);
    }
#if 0
    bool log_valid = false;
    ::mdt::StoreRequest* req = new ::mdt::StoreRequest;
    ::mdt::StoreResponse* resp = new ::mdt::StoreResponse;
    for (int i = 0; i < descriptor->field_count(); i++) {
        const ::google::protobuf::FieldDescriptor* field = descriptor->field(i);
        const ::google::protobuf::Reflection* reflection = message->GetReflection();
        if (field == NULL || !reflection->HasField(*message, field)) continue;
        if (primary_key_name == field->name()) {
            req->primary_key = TraceModule::GetFieldValue(message, field);
            req->timestamp = ::mdt::timer::get_micros();
            if (message->SerializeToString(&req->data)) {
                log_valid = true;
            } else {
                std::cout << "serialstring fail\n";
            }
        } else {
            ::mdt::Index idx;
            idx.index_name = field->name();
            idx.index_key = TraceModule::GetFieldValue(message, field);
            if (idx.index_key.size())
                req->index_list.push_back(idx);
        }
    }

    ::mdt::StoreCallback callback = DummyPutCallback;
    if (log_valid) {
        table->Put(req, resp, callback, NULL);
    } else {
        callback(table, req, resp, NULL);
    }
#endif
    return;
}

}

