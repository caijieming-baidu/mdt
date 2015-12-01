include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
OPT ?= -g2 -Wall -Werror      # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = gcc
CXX = g++

SHARED_CFLAGS = -fPIC
SHARED_LDFLAGS = -shared -Wl,-soname -Wl,

INCPATH += -I./src -I./include $(DEPS_INCPATH) 
CFLAGS += -std=c99 $(OPT) $(SHARED_CFLAGS) $(INCPATH)
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH)
LDFLAGS += -rdynamic $(DEPS_LDPATH) $(DEPS_LDFLAGS) -lpthread -lrt -lz -ldl

PROTO_FILES := $(wildcard src/proto/*.proto)
PROTO_OUT_CC := $(PROTO_FILES:.proto=.pb.cc)
PROTO_OUT_H := $(PROTO_FILES:.proto=.pb.h)

SDK_SRC := $(wildcard src/sdk/*.cc)
COMMON_SRC := $(wildcard src/common/*.cc)
UTIL_SRC := $(wildcard src/util/*.cc)
PROTO_SRC := $(filter-out %.pb.cc, $(wildcard src/proto/*.cc)) $(PROTO_OUT_CC)
VERSION_SRC := src/version.cc
MDTTOOL_SRC := $(wildcard src/mdt-tool/mdt-tool.cc)
UPDATESCHEMA_SRC := $(wildcard src/mdt-tool/test_update_schema.cc)
SAMPLE_SRC := $(wildcard src/sample/mdt_test.cc)
WRITE_TEST_SRC := $(wildcard src/benchmark/write_test.cc)
DUMPFILE_SRC := $(wildcard src/benchmark/dump_file.cc)
SYNC_WRITE_TEST_SRC := $(wildcard src/benchmark/sync_write_test.cc)
MULWRITE_TEST_SRC := $(wildcard src/benchmark/mulcli_write_test.cc)
SCAN_TEST_SRC := $(wildcard src/benchmark/scan_test.cc)
C_SAMPLE_SRC := $(wildcard src/sample/c_sample.c)

###########################
#	trace collector   #
###########################
FTRACE_SRC := $(wildcard src/ftrace/collector/*.cc)
FTRACE_TEST_SRC := $(wildcard src/ftrace/test/TEST_log.cc)

###########################
#	search engine     #
###########################
FTRACE_SEARCHENGINE_SRC := $(wildcard src/ftrace/search_engine/*.cc)

SDK_OBJ := $(SDK_SRC:.cc=.o)
COMMON_OBJ := $(COMMON_SRC:.cc=.o)
UTIL_OBJ := $(UTIL_SRC:.cc=.o)
PROTO_OBJ := $(PROTO_SRC:.cc=.o)
VERSION_OBJ := $(VERSION_SRC:.cc=.o)
SAMPLE_OBJ := $(SAMPLE_SRC:.cc=.o)
MDTTOOL_OBJ := $(MDTTOOL_SRC:.cc=.o)
UPDATESCHEMA_OBJ := $(UPDATESCHEMA_SRC:.cc=.o)
WRITE_TEST_OBJ := $(WRITE_TEST_SRC:.cc=.o)
DUMPFILE_OBJ := $(DUMPFILE_SRC:.cc=.o)
SYNC_WRITE_TEST_OBJ := $(SYNC_WRITE_TEST_SRC:.cc=.o)
MULWRITE_TEST_OBJ := $(MULWRITE_TEST_SRC:.cc=.o)
SCAN_TEST_OBJ := $(SCAN_TEST_SRC:.cc=.o)
C_SAMPLE_OBJ := $(C_SAMPLE_SRC:.c=.o)

###########################
#	trace collector
###########################
FTRACE_OBJ := $(FTRACE_SRC:.cc=.o)
FTRACE_TEST_OBJ := $(FTRACE_TEST_SRC:.cc=.o)

###########################
#	search engine     #
###########################
FTRACE_SEARCHENGINE_OBJ := $(FTRACE_SEARCHENGINE_SRC:.cc=.o)

CXX_OBJ := $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ) \
           $(SAMPLE_OBJ) $(MDTTOOL_OBJ) $(WRITE_TEST_OBJ) $(MULWRITE_TEST_OBJ) \
           $(SCAN_TEST_OBJ) $(SYNC_WRITE_TEST_OBJ) $(UPDATESCHEMA_OBJ) $(DUMPFILE_OBJ)
C_OBJ := $(C_SAMPLE_OBJ)

PROGRAM = 
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
SEARCH_SERVICE = search_service
###########################
#	trace collector
###########################
FTRACELIBRARY = libftrace.a
FTRACE_TEST = TEST_log

.PHONY: all clean cleanall test
all: $(PROGRAM) $(LIBRARY) $(FTRACELIBRARY) $(SEARCH_SERVICE) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL) $(WRITE_TEST) $(DUMPFILE) $(MULWRITE_TEST) $(SCAN_TEST) $(SYNC_WRITE_TEST) $(UPDATESCHEMA) $(FTRACE_TEST) 
	mkdir -p build/include build/lib build/bin
	#cp $(PROGRAM) build/bin
	cp $(LIBRARY) $(FTRACELIBRARY) build/lib
	cp src/ftrace/collector/logger.h build/include
	cp src/sdk/sdk.h build/include/mdt.h
	cp src/sdk/c.h build/include/mdt_c.h
	cp src/sdk/mdt.go build/include/mdt.go
	cp $(DEPS_LIBRARIES) build/lib
	echo 'Done'

clean:
	rm -rf $(CXX_OBJ) $(C_OBJ) $(FTRACE_OBJ)
	rm -rf $(PROGRAM) $(LIBRARY) $(FTRACELIBRARY) $(SAMPLE) $(C_SAMPLE) $(MDTTOOL) $(WRITE_TEST) $(MULWRITE_TEST) $(SCAN_TEST) $(SYNC_WRITE_TEST) $(UPDATESCHEMA) $(FTRACE_TEST)
	rm -rf search_service
	rm -rf $(DUMPFILE)

cleanall:
	$(MAKE) clean
	rm -rf build

sample: $(SAMPLE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS)

mdt-tool: $(MDTTOOL_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(MDTTOOL_OBJ) $(LIBRARY) $(LDFLAGS) -lreadline -lhistory -lncurses

test_update_schema: $(UPDATESCHEMA_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(UPDATESCHEMA_OBJ) $(LIBRARY) $(LDFLAGS) 

write_test: $(WRITE_TEST_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(WRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS) 

dumpfile: $(DUMPFILE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(DUMPFILE_OBJ) $(LIBRARY) $(LDFLAGS) 

sync_write_test: $(SYNC_WRITE_TEST_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(SYNC_WRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS)

mulcli_write_test: $(MULWRITE_TEST_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(MULWRITE_TEST_OBJ) $(LIBRARY) $(LDFLAGS)

scan_test: $(SCAN_TEST_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(SCAN_TEST_OBJ) $(LIBRARY) $(LDFLAGS)

c_sample: $(C_SAMPLE_OBJ) $(LIBRARY)
	$(CXX) -o $@ $(C_SAMPLE_OBJ) $(LIBRARY) $(LDFLAGS)

libmdt.a: $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)
	$(AR) -rs $@ $(SDK_OBJ) $(COMMON_OBJ) $(UTIL_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)

libftrace.a: $(FTRACE_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)
	$(AR) -rs $@ $(FTRACE_OBJ) $(PROTO_OBJ) $(VERSION_OBJ)

TEST_log: $(FTRACE_TEST_OBJ) $(FTRACELIBRARY)
	$(CXX) -o $@ $(FTRACE_TEST_OBJ) $(FTRACELIBRARY) $(LDFLAGS)

search_service: $(FTRACE_SEARCHENGINE_OBJ) $(LIBRARY) 
	$(CXX) -o search_service $(FTRACE_SEARCHENGINE_OBJ) $(LIBRARY) $(LDFLAGS)

$(CXX_OBJ): %.o: %.cc $(PROTO_OUT_H)
	$(CXX) $(CXXFLAGS) -c $< -o $@

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
