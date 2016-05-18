include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
#OPT ?= -g2 -Wall -Werror      # (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -g2       # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = gcc
CXX = g++

PLATFORM=OS_LINUX
PLATFORM_LDFLAGS=-pthread
PLATFORM_LIBS=
PLATFORM_CCFLAGS= -fno-builtin-memcmp -pthread -DOS_LINUX -DLEVELDB_PLATFORM_POSIX -DLEVELDB_ATOMIC_PRESENT -DSNAPPY
PLATFORM_CXXFLAGS=-std=c++0x -fno-builtin-memcmp -lpthread -DLEVELDB_PLATFORM_POSIX -DLEVELDB_ATOMIC_PRESENT -DSNAPPY
PLATFORM_SHARED_CFLAGS=-fPIC
PLATFORM_SHARED_EXT=so
PLATFORM_SHARED_LDFLAGS=-shared -Wl,-soname -Wl,
PLATFORM_SHARED_VERSIONED=true

SHARED_CFLAGS = -fPIC 
SHARED_LDFLAGS = -shared -Wl,-soname -Wl, 

INCPATH += -I./src/leveldb -I./src/leveldb/include -I./src -I./include $(DEPS_INCPATH) 
#### CFLAGS += -std=c99 $(OPT) $(SHARED_CFLAGS) $(INCPATH) $(PLATFORM_CCFLAGS) 
CFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH) $(PLATFORM_CCFLAGS) 
CXXFLAGS += $(OPT) $(DEPS_LDPATH) $(DEPS_LDFLAGS) $(SHARED_CFLAGS) $(INCPATH) $(PLATFORM_CXXFLAGS) 
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz -ldl 

PROTO_FILES := $(wildcard src/proto/*.proto)
PROTO_OUT_CC := $(PROTO_FILES:.proto=.pb.cc)
PROTO_OUT_H := $(PROTO_FILES:.proto=.pb.h)

############################################################
OTHER_SRC := src/trace_flags.cc src/fs_inotify.cc
SDK_SRC := $(wildcard src/sdk/*.cc)
COMMON_SRC := $(wildcard src/common/*.cc)
UTIL_SRC := $(wildcard src/utils/*.cc)
PROTO_SRC := $(filter-out %.pb.cc, $(wildcard src/proto/*.cc)) $(PROTO_OUT_CC)
VERSION_SRC := src/version.cc
MDTTOOL_SRC := $(wildcard src/mdt-tool/mdt-tool.cc)
MAIL_SRC := $(wildcard src/mail/mail.cc)
UPDATESCHEMA_SRC := $(wildcard src/mdt-tool/test_update_schema.cc)
SAMPLE_SRC := $(wildcard src/sample/mdt_test.cc)
WRITE_TEST_SRC := $(wildcard src/benchmark/write_test.cc)
DUMPFILE_SRC := $(wildcard src/benchmark/dump_file.cc)
SYNC_WRITE_TEST_SRC := $(wildcard src/benchmark/sync_write_test.cc)
MULWRITE_TEST_SRC := $(wildcard src/benchmark/mulcli_write_test.cc)
SCAN_TEST_SRC := $(wildcard src/benchmark/scan_test.cc)
C_SAMPLE_SRC := $(wildcard src/sample/c_sample.c)

FTRACE_SRC := $(wildcard src/ftrace/*.cc)
###FTRACE_TEST_SRC := $(wildcard src/ftrace/test/*.cc)

AGENT_SRC := $(wildcard src/agent/*.cc)
COLLECTOR_SRC := $(wildcard src/collector/*.cc)
SCHEDULER_SRC := $(wildcard src/scheduler/*.cc)

############################################################
OTHER_OBJ := $(OTHER_SRC:.cc=.o)
SDK_OBJ := $(SDK_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
UTIL_OBJ := $(UTIL_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
VERSION_OBJ := $(VERSION_SRC:.cc=.o)
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)
MDTTOOL_OBJ := $(MDTTOOL_SRC:.cc=.o)
MAIL_OBJ := $(MAIL_SRC:.cc=.o)
UPDATESCHEMA_OBJ := $(UPDATESCHEMA_SRC:.cc=.o)
WRITE_TEST_OBJ := $(WRITE_TEST_SRC:.cc=.o)
DUMPFILE_OBJ := $(DUMPFILE_SRC:.cc=.o)
SYNC_WRITE_TEST_OBJ := $(SYNC_WRITE_TEST_SRC:.cc=.o)
MULWRITE_TEST_OBJ := $(MULWRITE_TEST_SRC:.cc=.o)
SCAN_TEST_OBJ := $(SCAN_TEST_SRC:.cc=.o)
C_SAMPLE_OBJ := $(C_SAMPLE_SRC:.c=.o)

FTRACE_OBJ := $(FTRACE_SRC:.cc=.o)
#FTRACE_TEST_OBJ := $(FTRACE_TEST_SRC:.cc=.o)
AGENT_OBJ := $(AGENT_SRC:.cc=.o)
COLLECTOR_OBJ := $(COLLECTOR_SRC:.cc=.o)
SCHEDULER_OBJ := $(SCHEDULER_SRC:.cc=.o)

CXX_OBJ := $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) \
           $(SAMPLE_OBJ) $(MDTTOOL_OBJ) $(MAIL_OBJ) $(WRITE_TEST_OBJ) $(MULWRITE_TEST_OBJ) \
           $(SCAN_TEST_OBJ) $(SYNC_WRITE_TEST_OBJ) $(UPDATESCHEMA_OBJ) $(DUMPFILE_OBJ) \
	   $(COLLECTOR_OBJ) $(SCHEDULER_OBJ) 
C_OBJ := $(C_SAMPLE_OBJ)
LEVELDB_LIB := src/leveldb/libleveldb.a
MEMENV_LIB := src/leveldb/libmemenv.a

############################################################
PROGRAM = agent_main collector_main scheduler_main 
FTRACELIBRARY = libftrace.a
#FTRACE_TEST = TEST_log

LIBRARY = libmdt.a
SAMPLE = sample
MDTTOOL = mdt-tool
UPDATESCHEMA = test_update_schema
MDTTOOL_TEST = mdt-tool-test
WRITE_TEST = write_test
DUMPFILE = dumpfile
SYNC_WRITE_TEST = sync_write_test
MULWRITE_TEST = mulcli_write_test
SCAN_TEST = scan_test
C_SAMPLE = c_sample

.PHONY: all clean cleanall test
all: $(PROGRAM) $(LIBRARY) $(FTRACELIBRARY) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL) $(WRITE_TEST) $(DUMPFILE) $(MULWRITE_TEST) $(SCAN_TEST) $(SYNC_WRITE_TEST) $(UPDATESCHEMA) $(FTRACE_TEST) 
	mkdir -p build/include build/lib build/bin
	cp $(PROGRAM) build/bin
	cp $(LIBRARY) build/lib
	cp src/sdk/sdk.h build/include/mdt.h
	cp src/sdk/c.h build/include/mdt_c.h
	cp src/sdk/mdt.go build/include/mdt.go
	cp -r conf build
	cp $(DEPS_LIBRARIES) build/lib
	echo 'Done'

clean:
	rm -rf $(CXX_OBJ) $(C_OBJ) $(FTRACE_OBJ) $(AGENT_OBJ) $(SCHEDULER_OBJ) $(COLLECTOR_OBJ)
	rm -rf $(PROGRAM) $(LIBRARY) $(FTRACELIBRARY) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL) $(WRITE_TEST) $(MULWRITE_TEST) $(SCAN_TEST) $(SYNC_WRITE_TEST) $(UPDATESCHEMA) $(FTRACE_TEST)
	rm -rf search_service
	rm -rf $(DUMPFILE)

cleanall:
	$(MAKE) clean
	rm -rf build

src/leveldb/libleveldb.a: FORCE
	$(MAKE) -C src/leveldb

sample: $(SAMPLE_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB)

mdt-tool: $(MDTTOOL_OBJ) $(LIBRARY) $(OTHER_OBJ) $(LEVELDB_LIB)
	$(CXX) -o $@ $(MDTTOOL_OBJ) $(LIBRARY) $(OTHER_OBJ) $(LDFLAGS) $(LEVELDB_LIB) -lreadline -lhistory -lncurses

test_update_schema: $(UPDATESCHEMA_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(UPDATESCHEMA_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB) 

write_test: $(WRITE_TEST_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(WRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB) 

dumpfile: $(DUMPFILE_OBJ) $(LIBRARY) $(LEVELDB_LIB) $(MAIL_OBJ)
	$(CXX) -o $@ $(DUMPFILE_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB) $(MAIL_OBJ) 

sync_write_test: $(SYNC_WRITE_TEST_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(SYNC_WRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB)

mulcli_write_test: $(MULWRITE_TEST_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(MULWRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB)

scan_test: $(SCAN_TEST_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(SCAN_TEST_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB)

c_sample: $(C_SAMPLE_OBJ) $(LIBRARY) $(LEVELDB_LIB)
	$(CXX) -o $@ $(C_SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS) $(LEVELDB_LIB)

libmdt.a: $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(LEVELDB_LIB)
	$(AR) -rs $@ $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(LEVELDB_LIB)

libftrace.a: $(FTRACE_OBJ) $(PROTO_OBJ)
	$(AR) -rs $@ $(FTRACE_OBJ) $(PROTO_OBJ)

#TEST_log: $(FTRACE_TEST_OBJ) $(FTRACELIBRARY)
#	$(CXX) -o $@ $(FTRACE_TEST_OBJ) $(FTRACELIBRARY) $(LDFLAGS)

agent_main: $(AGENT_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(LEVELDB_LIB) $(MEMENV_LIB) $(OTHER_OBJ) 
	$(CXX) -o agent_main $(AGENT_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(LDFLAGS) $(LEVELDB_LIB) $(MEMENV_LIB) $(OTHER_OBJ) 

collector_main: $(COLLECTOR_OBJ) $(LIBRARY) $(OTHER_OBJ) $(LEVELDB_LIB)
	$(CXX) -o collector_main $(COLLECTOR_OBJ) $(LIBRARY) $(OTHER_OBJ) $(LDFLAGS) $(LEVELDB_LIB)

scheduler_main: $(SCHEDULER_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(OTHER_OBJ) $(LEVELDB_LIB) $(MAIL_OBJ)
	$(CXX) -o scheduler_main $(SCHEDULER_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) $(OTHER_OBJ) $(LDFLAGS) $(LEVELDB_LIB) $(MAIL_OBJ)

$(CXX_OBJ): %.o: %.cc $(PROTO_OUT_H) $(LEVELDB_LIB) 
	$(CXX) $(CXXFLAGS)  -c $< -o $@ $(LEVELDB_LIB) 

$(C_OBJ): %.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(VERSION_SRC): FORCE
	sh build_version.sh

.PHONY: proto
proto: $(PROTO_OUT_CC) $(PROTO_OUT_H)
 
%.pb.cc %.pb.h: %.proto
	$(PROTOC) --proto_path=./src/proto/ --proto_path=$(PROTOBUF_INCDIR) \
              --proto_path=$(SOFA_PBRPC_INCDIR) \
              --cpp_out=./src/proto/ $< 
.PHONY: FORCE
FORCE:
