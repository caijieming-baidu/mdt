#include depends.mk

# OPT ?= -O2 -DNDEBUG       # (A) Production use (optimized mode)
#OPT ?= -g2 -Wall -Werror      # (B) Debug mode, w/ full line-level debugging symbols
OPT ?= -g2       # (B) Debug mode, w/ full line-level debugging symbols
# OPT ?= -O2 -g2 -DNDEBUG   # (C) Profiling mode: opt, but w/debugging symbols

CC = gcc
CXX = g++

SHARED_CFLAGS = -fPIC 
SHARED_LDFLAGS = -shared -Wl,-soname -Wl, 
INCPATH += -I../thirdparty/include
LIBPATH += -L../thirdparty/lib
CXXFLAGS += $(OPT) $(SHARED_CFLAGS) $(INCPATH) $(LIBPATH) 
LDFLAGS += -rdynamic -lpthread -lrt -lz -ldl -lullib

############################################################
COMLOG_SRC := $(wildcard src/comlog_test.cc)
timecut_SRC := $(wildcard src/timecut_test.cc)
trunc_SRC := $(wildcard src/trunc_test.cc)

############################################################
COMLOG_OBJ := $(COMLOG_SRC:.cc=.o)
timecut_OBJ := $(timecut_SRC:.cc=.o)
trunc_OBJ := $(trunc_SRC:.cc=.o)

CXX_OBJ := $(COMLOG_OBJ) $(timecut_OBJ) $(trunc_OBJ)

############################################################
PROGRAM = comlog_test timecut_test trunc_test 

COMLOG_TEST = comlog_test timecut_test trunc_test 

.PHONY: all clean 
all: $(CXX_OBJ) $(PROGRAM)  
	mv $(PROGRAM) bin/
	rm -rf $(CXX_OBJ)

clean:
	rm -rf  $(CXX_OBJ); cd bin; rm -rf $(PROGRAM); cd ..  

comlog_test: $(COMLOG_OBJ) 
	$(CXX) -o $@ $(COMLOG_OBJ) $(CXXFLAGS) $(LDFLAGS) 
timecut_test: $(timecut_OBJ) 
	$(CXX) -o $@ $(timecut_OBJ) $(CXXFLAGS) $(LDFLAGS) 
trunc_test: $(trunc_OBJ) 
	$(CXX) -o $@ $(trunc_OBJ) $(CXXFLAGS) $(LDFLAGS) 


$(CXX_OBJ): %.o: %.cc
	$(CXX) $(CXXFLAGS)  -c $< -o $@  

.PHONY: FORCE
FORCE:
