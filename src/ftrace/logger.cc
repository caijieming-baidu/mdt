#include <stdarg.h>
#include <sstream>

#include "sdk/sdk.h"
#include "sdk/table.h"
#include "sdk/db.h"
#include <gflags/gflags.h>
#include "ftrace/logger.h"
#include "ftrace/trace.h"
#include <google/protobuf/message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

DECLARE_uint64(max_text_annotation_size);
DECLARE_bool(enable_debug_put_pb);

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

static ::google::protobuf::DescriptorProto descriptor_proto;

void DummyPutCallback(mdt::Table* table, mdt::StoreRequest* request,
                        mdt::StoreResponse* response,
                        void* callback_param) {
    if (FLAGS_enable_debug_put_pb) {
        std::cout << "put callback\n";
        const ::google::protobuf::Descriptor* descriptor = descriptor_proto.descriptor();
        std::cout << descriptor->DebugString();
    }
    delete request;
    delete response;
}
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message) {
    const ::google::protobuf::Descriptor* descriptor = message->GetDescriptor();
    const std::string& tablename = descriptor->name();
    const std::string& fullname = descriptor->full_name();
    std::string dbname(fullname, 0, fullname.size() - tablename.size() - 1);
    ::mdt::Table* table = TraceModule::GetProtoBufTable(dbname, tablename);
    if (table == NULL) return ;

    descriptor_proto.Clear();
    descriptor->CopyTo(&descriptor_proto);

    // store pb
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
            std::ostringstream ostr;
            if (message->SerializeToOstream(&ostr)) {
                req->data = ostr.str();
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
    return;
}

}

