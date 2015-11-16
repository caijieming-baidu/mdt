#ifndef FTRACE_LOGGER_H_
#define FTRACE_LOGGER_H_

#include <iostream>
#include <stdint.h>
#include <google/protobuf/message.h>
//#include <proto/ftrace.pb.h>

namespace mdt {

void InitTraceModule(const std::string& flagfile);

// common interface
void OpenTrace(uint64_t trace_id,
               uint64_t parent_span_id,
               uint64_t span_id,
               const std::string& name,
               const std::string& trace_name);
void AttachTrace(uint64_t attach_id); // use for trace_context transform between threads
void DetachAndOpenTrace(uint64_t attach_id);
void ReleaseTrace();

void KvLog(int level, const char* value, const char* fmt, ...);
void Log(int level, const char* fmt, ...);

//TraceIdentify GetTraceIdentify();

// galaxy interface
void OpenProtoBufLog(const std::string& dbname, const std::string& tablename);
void CloseProtoBufLog(const std::string& dbname, const std::string& tablename);
void LogProtoBuf(const std::string& primary_key_name, ::google::protobuf::Message* message);

}
#endif
