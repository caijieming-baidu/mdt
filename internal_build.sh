#!/bin/bash

set -e -u -E # this script will exit if any sub-command fails

########################################
# download & build depend software
########################################

WORK_DIR=`pwd`
DEPS_SOURCE=`pwd`/thirdsrc
DEPS_PREFIX=`pwd`/thirdparty
DEPS_CONFIG="--prefix=${DEPS_PREFIX} --disable-shared --with-pic"

export PATH=${DEPS_PREFIX}/bin:$PATH
mkdir -p ${DEPS_SOURCE} ${DEPS_PREFIX}

git clone --depth=1 http://gitlab.baidu.com/baidups/third.git ${DEPS_SOURCE}
git clone --depth=1 http://gitlab.baidu.com/baidups/sofa-pbrpc.git ${DEPS_SOURCE}/sofa-pbrpc
git clone --depth=1 http://gitlab.baidu.com/baidups/ins.git ${DEPS_SOURCE}/ins
git clone --depth=1 http://gitlab.baidu.com/baidups/tera.git ${DEPS_SOURCE}/tera

cd ${DEPS_SOURCE}

# boost
tar zxf boost_1_57_0.tar.gz
rm -rf ${DEPS_PREFIX}/boost_1_57_0
mv boost_1_57_0 ${DEPS_PREFIX}

# protobuf
tar zxf protobuf-2.6.1.tar.gz
cd protobuf-2.6.1
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# snappy
tar zxf snappy-1.1.1.tar.gz
cd snappy-1.1.1
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# sofa-pbrpc
cd sofa-pbrpc
sed -i '/BOOST_HEADER_DIR=/ d' depends.mk
sed -i '/PROTOBUF_DIR=/ d' depends.mk
sed -i '/SNAPPY_DIR=/ d' depends.mk
echo "BOOST_HEADER_DIR=${DEPS_PREFIX}/boost_1_57_0" >> depends.mk
echo "PROTOBUF_DIR=${DEPS_PREFIX}" >> depends.mk
echo "SNAPPY_DIR=${DEPS_PREFIX}" >> depends.mk
echo "PREFIX=${DEPS_PREFIX}" >> depends.mk
make -j4
make install
cd -

cd sofa-pbrpc/python
python setup.py install
cd -

# zookeeper
tar zxf zookeeper-3.4.6.tar.gz
cd zookeeper-3.4.6/src/c
./configure ${DEPS_CONFIG}
make -j4
make install
cd -

# cmake for gflags
tar zxf CMake-3.2.1.tar.gz
cd CMake-3.2.1
./configure --prefix=${DEPS_PREFIX}
make -j4
make install
cd -

# gflags
tar zxf gflags-2.1.1.tar.gz
cd gflags-2.1.1
cmake -DCMAKE_INSTALL_PREFIX=${DEPS_PREFIX} -DGFLAGS_NAMESPACE=google -DCMAKE_CXX_FLAGS=-fPIC
make -j4
make install
cd -

# glog
tar zxf glog-0.3.3.tar.gz
cd glog-0.3.3
./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
make -j4
make install
cd -

# gtest
unzip gtest-1.7.0.zip
cd gtest-1.7.0
./configure ${DEPS_CONFIG}
make
cp -a lib/.libs/* ${DEPS_PREFIX}/lib
cp -a include/gtest ${DEPS_PREFIX}/include
cd -

# libunwind for gperftools
tar zxf libunwind-0.99-beta.tar.gz
cd libunwind-0.99-beta
./configure ${DEPS_CONFIG}
make CFLAGS=-fPIC -j4
make CFLAGS=-fPIC install
cd -

# gperftools (tcmalloc)
tar zxf gperftools-2.2.1.tar.gz
cd gperftools-2.2.1
./configure ${DEPS_CONFIG} CPPFLAGS=-I${DEPS_PREFIX}/include LDFLAGS=-L${DEPS_PREFIX}/lib
make -j4
make install
cd -

# ins
cd ins
sed -i "s|^PREFIX=.*|PREFIX=${DEPS_PREFIX}|" Makefile
sed -i "s|^PROTOC=.*|PROTOC=${DEPS_PREFIX}/bin/protoc|" Makefile
BOOST_PATH=${DEPS_PREFIX}/boost_1_57_0 make -j4 install_sdk
cd -

# tera
cd tera
sed -i 's/^SOFA_PBRPC_PREFIX=.*/SOFA_PBRPC_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^PROTOBUF_PREFIX=.*/PROTOBUF_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^SNAPPY_PREFIX=.*/SNAPPY_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^ZOOKEEPER_PREFIX=.*/ZOOKEEPER_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^GFLAGS_PREFIX=.*/GFLAGS_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^GLOG_PREFIX=.*/GLOG_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^GTEST_PREFIX=.*/GTEST_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=..\/..\/thirdparty/' depends.mk
sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=..\/..\/thirdparty\/boost_1_57_0/' depends.mk
sed -i 's/^INS_PREFIX=.*/INS_PREFIX=..\/..\/thirdparty/' depends.mk
make -j4 
cp -a build/lib/*.a ${DEPS_PREFIX}/lib
cp -a build/include/*.h ${DEPS_PREFIX}/include
cd -

cd ${WORK_DIR}

########################################
# config depengs.mk
########################################

sed -i 's/^SOFA_PBRPC_PREFIX=.*/SOFA_PBRPC_PREFIX=thirdparty/' depends.mk
sed -i 's/^PROTOBUF_PREFIX=.*/PROTOBUF_PREFIX=thirdparty/' depends.mk
sed -i 's/^SNAPPY_PREFIX=.*/SNAPPY_PREFIX=thirdparty/' depends.mk
sed -i 's/^ZOOKEEPER_PREFIX=.*/ZOOKEEPER_PREFIX=thirdparty/' depends.mk
sed -i 's/^GFLAGS_PREFIX=.*/GFLAGS_PREFIX=thirdparty/' depends.mk
sed -i 's/^GLOG_PREFIX=.*/GLOG_PREFIX=thirdparty/' depends.mk
sed -i 's/^GTEST_PREFIX=.*/GTEST_PREFIX=thirdparty/' depends.mk
sed -i 's/^GPERFTOOLS_PREFIX=.*/GPERFTOOLS_PREFIX=thirdparty/' depends.mk
sed -i 's/^BOOST_INCDIR=.*/BOOST_INCDIR=thirdparty\/boost_1_57_0/' depends.mk
sed -i 's/^INS_PREFIX=.*/INS_PREFIX=thirdparty/' depends.mk
sed -i 's/^TERA_PREFIX=.*/TERA_PREFIX=thirdparty/' depends.mk

########################################
# build tera
########################################

make -j4

