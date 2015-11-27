#ifndef FTRACE_TRACE_H_
#define FTRACE_TRACE_H_

#include <stack>

#include "util/counter.h"
#include "util/mutex.h"
#include "proto/ftrace.pb.h"
#include <sofa/pbrpc/pbrpc.h>

namespace mdt {

/*
enum TableColumnType {
    TYPE_UNKNOW = 0,
    TYPE_UINT64 = 3,
    TYPE_STRING = 9,
    TYPE_BYTES = 12,
};
enum TableColumnProperty {
    PROP_NONE = 0,
    PROP_PRIMARY_KEY = 1,
    PROP_INDEX_COL = 2,
}

class TableSchema {
public:
    TableSchema(const std::string& dbname, const std::string& tablename);
    AddColumn(const std::string& name, TableColumnProperty prop,
              TableColumnType type, const std::string& value);
    std::string GetDBname();
    std::string GetTableName();
};
*/

class Trace {
public:
    //////////////////////////////
    /////   User interface   /////
    //////////////////////////////
    static void OpenTrace(uint64_t trace_id,
                          uint64_t parent_span_id,
                          uint64_t span_id,
                          const std::string& name,
                          const std::string& trace_name);
    static void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads
    static void DetachAndOpenTrace(uint64_t attach_id);
    static void ReleaseTrace();

    static void Log(int level, const char* fmt, ...);
    static void KvLog(int level, const char* value, const char* fmt, ...);

    ////////////////////////////////
    /////  internal interface  /////
    ////////////////////////////////
    Trace(uint64_t trace_id, uint64_t parent_span_id, uint64_t span_id,
          const std::string& name, const std::string& trace_name);
    ~Trace();
    // rpc trace relatively
    TraceIdentify GetTraceID();

    static Trace* TopThreadValue(); // Get from thread_local
    void PushThreadValue();
    void PopThreadValue();

    void MergeTrace(FunctionSpan span);

    void TEST_PrintLog();

    void FlushLog();
    void AddTextAnnotation(const std::string& text);
    void AddKvAnnotation(const std::string& key, const std::string& value);

    Counter ref; // protect by TraceModule::kmutex, because Trace should be thread safe

private:
    Mutex mu_;
    FunctionSpan span_;
    std::map<uint64_t, std::stack<void*> > thread_value_; // <thread id, stack<Trace ptr> >
};

struct TraceModule {
    //////////////////////////////
    /////   User interface   /////
    //////////////////////////////
    static void InitTraceModule(const std::string& flagfile);

    // galaxy interface
    static std::string GetFieldValue(::google::protobuf::Message* message,
                                     const ::google::protobuf::FieldDescriptor* field);
    static void OpenProtoBufLog(const std::string& dbname, const std::string& tablename);
    static void CloseProtoBufLog(const std::string& dbname, const std::string& tablename);

    ////////////////////////////////
    /////  internal interface  /////
    ////////////////////////////////
    static uint64_t GenerateUUID();
    // trace bewteen threads
    static Trace* ClearAndGetTrace(uint64_t key); // Get and remove from trace_map_
    static void SetTrace(uint64_t key, Trace* trace); // Put into trace_map_

    static Trace* GetTraceBySpanId(uint64_t span_id);
    static void SetTraceBySpanId(uint64_t span_id, Trace* trace);
    static void ClearTraceBySpanId(uint64_t span_id);

    static pthread_key_t thread_key; // thread_load variable
    static Mutex kmutex;
    static std::map<uint64_t, Trace*> ktrace_map;
    static std::map<uint64_t, Trace*> kspan_map; // gobal span map

    // galaxy trace
    static ::sofa::pbrpc::RpcClient rpc_client;
};

}

#endif
