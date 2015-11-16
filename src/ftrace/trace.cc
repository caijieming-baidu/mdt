#include <unistd.h>
#include <stdarg.h>
#include <iostream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <gflags/gflags.h>

#include "ftrace/trace.h"
#include "util/coding.h"

DECLARE_string(flagfile);
DECLARE_uint64(max_text_annotation_size);
DECLARE_uint64(log_level);

DECLARE_bool(use_mdt_flag);
DECLARE_string(mdt_flagfile);

namespace mdt {

pthread_key_t TraceModule::thread_key; // thread_load variable
Mutex TraceModule::kmutex;
std::map<uint64_t, Trace*> TraceModule::ktrace_map;
std::map<uint64_t, Trace*> TraceModule::kspan_map; // gobal span map
DBMap TraceModule::kDBMap;

///////////////////////
///// TraceModule /////
///////////////////////
void NilCallback(void* arg) {}
void TraceModule::InitTraceModule(const std::string& flagfile) {
    // Get configure
    if (flagfile.size() == 0 || access(flagfile.c_str(), F_OK)) {
        std::cout << "ZIPLOG: use default configure\n";
    } else {
        int ac = 1;
        char** av = new char*[2];
        av[0] = (char*)"dummy";
        av[1] = NULL;
        std::string local_flagfile = FLAGS_flagfile;
        FLAGS_flagfile = flagfile;
        ::google::ParseCommandLineFlags(&ac, &av, true);
        delete av;
        FLAGS_flagfile = local_flagfile;
        std::cout << "ZIPLOG: use " << flagfile << " to configure lib\n";
    }

    //mdt.flag enable
    if (FLAGS_use_mdt_flag) {
        int ac = 1;
        char** av = new char*[2];
        av[0] = (char*)"dummy";
        av[1] = NULL;
        std::string local_flagfile = FLAGS_flagfile;
        FLAGS_flagfile = FLAGS_mdt_flagfile;
        ::google::ParseCommandLineFlags(&ac, &av, true);
        delete av;
        FLAGS_flagfile = local_flagfile;
    }
    pthread_key_create(&thread_key, NilCallback);
    return;
}

// galaxy interface
inline std::string Uint64ToString(uint64_t val) {
    char buf[8];
    char* p = buf;
    EncodeBigEndian(p, val);
    std::string str_val(buf, 8);
    return str_val;
}

std::string TraceModule::GetFieldValue(::google::protobuf::Message* message,
                                       const ::google::protobuf::FieldDescriptor* field) {
    const ::google::protobuf::Reflection* reflection = message->GetReflection();

    if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_UINT64) {
        uint64_t val = (uint64_t)reflection->GetUInt64(*message, field);
        return Uint64ToString(val);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_INT64) {
        uint64_t val = (uint64_t)reflection->GetInt64(*message, field);
        //std::cout << "GetInt64: " << val << std::endl;
        return Uint64ToString(val);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_INT32) {
        uint64_t val = (uint64_t)reflection->GetInt32(*message, field);
        //std::cout << "GetInt32: " << val << std::endl;
        return Uint64ToString(val);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_UINT32) {
        uint64_t val = (uint64_t)reflection->GetUInt32(*message, field);
        return Uint64ToString(val);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_STRING) {
        //std::cout << "GetString: " << reflection->GetString(*message, field) << std::endl;
        return reflection->GetString(*message, field);
    } else if (field->type() == ::google::protobuf::FieldDescriptor::TYPE_BYTES) {
        return reflection->GetString(*message, field);
    } else {
        std::cout << "not support type " << field->type() << std::endl;
    }
    return "";
}

::mdt::Table* TraceModule::GetProtoBufTable(const std::string& dbname, const std::string& tablename) {
    MutexLock mu(&TraceModule::kDBMap.mutex);
    std::map<std::string, ::mdt::Table*>& tablemap = TraceModule::kDBMap.table; // dbname#tablename

    std::string internal_tablename = dbname + "#" + tablename;
    std::map<std::string, ::mdt::Table*>::iterator table_it = tablemap.find(internal_tablename);
    ::mdt::Table* table_ptr = NULL;
    if (table_it != tablemap.end()) {
        table_ptr = table_it->second;
    }
    return table_ptr;
}

// TODO: do io in lock, but Open not frequent
void TraceModule::OpenProtoBufLog(const std::string& dbname, const std::string& tablename) {
    MutexLock mu(&TraceModule::kDBMap.mutex);
    std::map<std::string, ::mdt::Database*>& dbmap = TraceModule::kDBMap.db; // dbname
    std::map<std::string, ::mdt::Table*>& tablemap = TraceModule::kDBMap.table; // dbname#tablename

    // open db
    ::mdt::Database* db_ptr = NULL;
    std::map<std::string, mdt::Database*>::iterator db_it = dbmap.find(dbname);
    if (db_it == dbmap.end()) {
        db_ptr = ::mdt::OpenDatabase(dbname);
        if (db_ptr == NULL) {
            return;
        }
        dbmap.insert(std::pair<std::string, ::mdt::Database*>(dbname, db_ptr));
    } else {
        db_ptr = db_it->second;
    }

    // open table
    std::string internal_tablename = dbname + "#" + tablename;
    ::mdt::Table* table_ptr = NULL;
    std::map<std::string, ::mdt::Table*>::iterator table_it = tablemap.find(internal_tablename);
    if (table_it == tablemap.end()) {
        table_ptr = ::mdt::OpenTable(db_ptr, tablename);
        if (table_ptr == NULL) {
            return;
        }
        tablemap.insert(std::pair<std::string, ::mdt::Table*>(internal_tablename, table_ptr));
    }
    return;
}

void TraceModule::CloseProtoBufLog(const std::string& dbname, const std::string& tablename) {
    MutexLock mu(&TraceModule::kDBMap.mutex);
    std::map<std::string, ::mdt::Database*>& dbmap = TraceModule::kDBMap.db; // dbname
    std::map<std::string, ::mdt::Table*>& tablemap = TraceModule::kDBMap.table; // dbname#tablename

    // close table
    std::string internal_tablename = dbname + "#" + tablename;
    std::map<std::string, ::mdt::Table*>::iterator table_it = tablemap.find(internal_tablename);
    if (table_it != tablemap.end()) {
        tablemap.erase(table_it);
        ::mdt::CloseTable(table_it->second);
    }

    // close db
    std::map<std::string, mdt::Database*>::iterator db_it = dbmap.find(dbname);
    if (db_it != dbmap.end()) {
        dbmap.erase(db_it);
        ::mdt::CloseDatabase(db_it->second);
    }
    return;
}

/////////////////////////////////////////
/////   internal interface          /////
/////////////////////////////////////////
void TraceModule::SetTrace(uint64_t key, Trace* trace) {
    MutexLock mu(&TraceModule::kmutex);
    trace->ref.Inc();
    TraceModule::ktrace_map.insert(std::pair<uint64_t, Trace*>(key, trace));
    return;
}
// trace.ref++
Trace* TraceModule::ClearAndGetTrace(uint64_t key) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::ktrace_map.find(key);
    if (it != TraceModule::ktrace_map.end()) {
        trace = it->second;
        TraceModule::ktrace_map.erase(it);
        trace->ref.Dec(); // erase from ktrace_map, so dec ref
    }
    if (trace == NULL) {
        std::cout << "trace module, attach trace(key = " << key << "), not found\n";
    } else {
        // new thread will ref this trace obj, so add ref
        trace->ref.Inc();
    }
    return trace;
}

// trace.ref++
Trace* TraceModule::GetTraceBySpanId(uint64_t span_id) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(span_id);
    if (it != TraceModule::kspan_map.end()) {
        trace = it->second;
        trace->ref.Inc();
    }
    return trace;
}

// trace.ref++
void TraceModule::SetTraceBySpanId(uint64_t span_id, Trace* trace) {
    MutexLock mu(&TraceModule::kmutex);
    trace->ref.Inc();
    TraceModule::kspan_map.insert(std::pair<uint64_t, Trace*>(span_id, trace));
    return;
}

// trace.ref--, last one flush log
void TraceModule::ClearTraceBySpanId(uint64_t span_id) {
    Trace* trace = NULL;
    MutexLock mu(&TraceModule::kmutex);
    std::map<uint64_t, Trace*>::iterator it = TraceModule::kspan_map.find(span_id);
    if (it != TraceModule::kspan_map.end()) {
        trace = it->second;
        if (trace->ref.Dec() == 0) {
            // last one remove from map
            TraceModule::kspan_map.erase(it);
            TraceModule::kmutex.Unlock();
            delete trace;
            TraceModule::kmutex.Lock();
        }
    }
    return;
}

uint64_t TraceModule::GenerateUUID() {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();
    uint64_t uuid_val = 0;
    for (int i = 0; i < 8; i++) {
        uint64_t tmp = (uint64_t)uuid.data[i];
        uuid_val |= tmp << (i * 8);
    }
    return uuid_val;
}

///////////////////////////
/////   Trace impl    /////
///////////////////////////
void Trace::OpenTrace(uint64_t trace_id,
                      uint64_t parent_span_id,
                      uint64_t span_id,
                      const std::string& name,
                      const std::string& trace_name) {
    bool new_span = false;
    Trace* trace = NULL;
    if (trace_id == 0) {
        trace_id = TraceModule::GenerateUUID();
        parent_span_id = 0;
        span_id = TraceModule::GenerateUUID();
        new_span = true;
    } else if (span_id == 0) {
        span_id = TraceModule::GenerateUUID();
        new_span = true;
    } else {
        trace = TraceModule::GetTraceBySpanId(span_id);
        if (trace == NULL) {
            std::cout << "trace id " << trace_id << ", span id " << span_id << ", not exist, New it";
            new_span = true;
        }
    }
    if (new_span) {
        trace = new Trace(trace_id, parent_span_id, span_id, name, trace_name);
        TraceModule::SetTraceBySpanId(span_id, trace);
    }

    // store in thread local
    trace->PushThreadValue();
    return;
}

void Trace::ReleaseTrace() {
    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->PopThreadValue();
    TraceModule::ClearTraceBySpanId(trace->GetTraceID().span_id());
    return;
}

// BEGIN jump to next thread
void Trace::AttachTrace(uint64_t attach_id) {
    Trace* trace = TopThreadValue();
    if (trace == NULL) {
        return;
    }
    TraceModule::SetTrace(attach_id, trace);
}

// FINISH jump to next thread
void Trace::DetachAndOpenTrace(uint64_t attach_id) {
    Trace* trace = TraceModule::ClearAndGetTrace(attach_id);
    if (trace == NULL) {
        return;
    }
    // store in thread local
    trace->PushThreadValue();
}

void Trace::Log(int level, const char* fmt, ...) {
    if (FLAGS_log_level < (uint64_t)level) {
        return;
    }
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->AddTextAnnotation((char *)anno_buf);
    return;
}

void Trace::KvLog(int level, const char* value, const char* fmt, ...) {
    if (FLAGS_log_level < (uint64_t)level) {
        return;
    }
    char anno_buf[FLAGS_max_text_annotation_size];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(anno_buf, sizeof(anno_buf), fmt, ap);
    va_end(ap);

    Trace* trace = Trace::TopThreadValue();
    if (trace == NULL) {
        return;
    }
    trace->AddKvAnnotation(anno_buf, value);
}

///////////////////////////////////
/////   internal interface    /////
///////////////////////////////////
void Trace::FlushLog() {
    mu_.Lock();
    // TODO: flush annotations
    std::cout << "\n[stub]flush annotations =====>\n";
    TEST_PrintLog();
    span_.clear_annotations();
    mu_.Unlock();
}

void Trace::AddTextAnnotation(const std::string& text) {
    mu_.Lock();
    FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->set_text_context(text);
    mu_.Unlock();
}

void Trace::AddKvAnnotation(const std::string& key, const std::string& value) {
    mu_.Lock();
    FunctionAnnotation* anno = span_.add_annotations();
    anno->set_timestamp(timer::get_micros());
    anno->mutable_kv_context()->set_key(key);
    anno->mutable_kv_context()->set_value(value);
    mu_.Unlock();
}

Trace::Trace(uint64_t trace_id, uint64_t parent_span_id, uint64_t span_id,
             const std::string& name, const std::string& trace_name) {
    span_.mutable_id()->set_trace_id(trace_id);
    span_.mutable_id()->set_parent_span_id(parent_span_id);
    span_.mutable_id()->set_span_id(span_id);
    span_.set_span_name(name);
    span_.set_trace_name(trace_name);
    ref.Set(0);
}

Trace::~Trace() {
    FlushLog();
}

Trace* Trace::TopThreadValue() {
    Trace* trace = (Trace*)pthread_getspecific(TraceModule::thread_key);
    if (trace == NULL) {
        std::cout << "WARNING: trace in thread local is nil\n";
    }
    return trace;
}

// step into thread
void Trace::PushThreadValue() {
    uint64_t tid = (uint64_t)pthread_self();
    void* prev_trace = pthread_getspecific(TraceModule::thread_key);
    pthread_setspecific(TraceModule::thread_key, (void*)this);

    std::cout << "<tid> " << tid << ", <trace> " << (uint64_t)this << std::endl;

    mu_.Lock();
    std::map<uint64_t, std::stack<void*> >::iterator it = thread_value_.find(tid);
    if (it != thread_value_.end()) {
        std::stack<void*>& st = it->second;
        st.push(prev_trace);
    } else {
        std::stack<void*> st;
        st.push(prev_trace);
        thread_value_.insert(std::pair<uint64_t, std::stack<void*> >(tid, st));
    }
    mu_.Unlock();
}

// leave from thread
void Trace::PopThreadValue() {
    uint64_t tid = (uint64_t)pthread_self();
    void* prev_trace = NULL;

    mu_.Lock();
    std::map<uint64_t, std::stack<void*> >::iterator it = thread_value_.find(tid);
    if (it != thread_value_.end()) {
        std::stack<void*>& st = it->second;
        if (st.size() != 0) {
            prev_trace = st.top();
            st.pop();
        }
    }
    mu_.Unlock();
    pthread_setspecific(TraceModule::thread_key, prev_trace);
}

TraceIdentify Trace::GetTraceID() {
    return span_.id();
}

void Trace::MergeTrace(FunctionSpan span) {
    mu_.Lock();
    if ((span_.id().trace_id() == span.id().trace_id()) &&
       (span_.id().parent_span_id() == span.id().parent_span_id()) &&
       (span_.id().span_id() == span.id().span_id())) {
        for (int i = 0; i < span.annotations_size(); i++) {
            FunctionAnnotation* anno = span_.add_annotations();
            anno->CopyFrom(span.annotations(i));
        }
    }
    mu_.Unlock();
    return;
}

void Trace::TEST_PrintLog() {
    std::cout << "SPAN PRINTER: trace id " << span_.id().trace_id()
        << ", parent_span_id " << span_.id().parent_span_id()
        << ", span_id " << span_.id().span_id() << "\n";
    for (int i = 0; i < span_.annotations_size(); i++) {
        if (span_.annotations(i).has_text_context()) {
            std::cout << "\t\tTS: " << span_.annotations(i).timestamp()
                << ", TEXT: " << span_.annotations(i).text_context() << std::endl;
        }
        if (span_.annotations(i).has_kv_context()) {
            std::cout << "\t\tTS: " << span_.annotations(i).timestamp()
                << ", KV: " << span_.annotations(i).kv_context().key()
                << " = " << span_.annotations(i).kv_context().value() << std::endl;
        }
    }
}

}
