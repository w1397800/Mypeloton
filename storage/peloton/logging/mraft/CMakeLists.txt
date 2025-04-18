cmake_minimum_required(VERSION 2.8.10)
project(braft C CXX)

SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "braft authors") # required

INCLUDE(CPack)

# option(EXAMPLE_LINK_SO "Whether examples are linked dynamically" OFF)
option(BRPC_WITH_GLOG "With glog" OFF)
option(WITH_DEBUG_SYMBOLS "With debug symbols" ON)

set(WITH_GLOG_VAL "0")

if(BRPC_WITH_GLOG)
    set(WITH_GLOG_VAL "1")
endif()

if(WITH_DEBUG_SYMBOLS)
    set(DEBUG_SYMBOL "-g")
endif()

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

include(FindThreads)
include(FindProtobuf)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()

    if(NOT(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-aligned-new")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)

if((NOT LEVELDB_INCLUDE_PATH) OR(NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()

find_path(GFLAGS_INCLUDE_PATH NAMES gflags/gflags.h)
find_library(GFLAGS_LIB NAMES gflags)

if((NOT GFLAGS_INCLUDE_PATH) OR(NOT GFLAGS_LIB))
    message(FATAL_ERROR "Fail to find gflags")
endif()

if(BRPC_WITH_GLOG)
    find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
    find_library(GLOG_LIB NAMES glog)

    if((NOT GLOG_INCLUDE_PATH) OR(NOT GLOG_LIB))
        message(FATAL_ERROR "Fail to find glog")
    endif()

    include_directories(${GLOG_INCLUDE_PATH})
endif()

if(LEVELDB_WITH_SNAPPY)
    find_library(SNAPPY_LIB NAMES snappy)
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
find_library(BRPC_LIB NAMES libbrpc.a brpc)

if((NOT BRPC_INCLUDE_PATH) OR(NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()

if(NOT PROTOBUF_PROTOC_EXECUTABLE)
    get_filename_component(PROTO_LIB_DIR ${PROTOBUF_LIBRARY} DIRECTORY)
    set(PROTOBUF_PROTOC_EXECUTABLE "${PROTO_LIB_DIR}/../bin/protoc")
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OPENSSL_ROOT_DIR
        "/usr/local/opt/openssl" # Homebrew installed OpenSSL
    )
endif()

include(FindOpenSSL)

include_directories(
    ${GFLAGS_INCLUDE_PATH}
    ${PROTOBUF_INCLUDE_DIRS}
    ${LEVELDB_INCLUDE_PATH}
    ${BRPC_INCLUDE_PATH}
    ${OPENSSL_INCLUDE_DIR}
)

if(BRPC_WITH_GLOG)
    set(DYNAMIC_LIB
        ${BRPC_LIB}
        ${GFLAGS_LIB}
        ${GLOG_LIB}
        ${PROTOBUF_LIBRARY}
        ${LEVELDB_LIB}
        ${CMAKE_THREAD_LIBS_INIT}
        ${OPENSSL_LIBRARIES}
        ${OPENSSL_CRYPTO_LIBRARY}
        dl
        z
    )
else()
    set(DYNAMIC_LIB
        ${BRPC_LIB}
        ${GFLAGS_LIB}
        ${PROTOBUF_LIBRARY}
        ${LEVELDB_LIB}
        ${CMAKE_THREAD_LIBS_INIT}
        ${OPENSSL_LIBRARIES}
        ${OPENSSL_CRYPTO_LIBRARY}
        dl
        z
    )
endif()

if(LEVELDB_WITH_SNAPPY)
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        ${SNAPPY_LIB}
    )
endif()

# Make sure the directory exists
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/rpc)

# for *.a
file(GLOB BRAFT_PROTOS "${CMAKE_CURRENT_SOURCE_DIR}/protoc/*.proto")

foreach(PROTO ${BRAFT_PROTOS})
    get_filename_component(PROTO_WE ${PROTO} NAME_WE)
    list(APPEND PROTO_SRCS "${CMAKE_BINARY_DIR}/rpc/${PROTO_WE}.pb.cc")
    execute_process(
        COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTO_FLAGS}
        --cpp_out=${CMAKE_BINARY_DIR}/rpc # Update this line to the desired output directory
        --proto_path=${PROTOBUF_INCLUDE_DIR}
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/protoc
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/protoc ${PROTO}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        ERROR_VARIABLE PROTO_ERROR
        RESULT_VARIABLE PROTO_RESULT
    )

    if(NOT ${PROTO_RESULT} EQUAL 0)
        message(FATAL_ERROR "Fail to generate cpp of ${PROTO} : ${PROTO_ERROR}")
    endif()
endforeach()

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)

if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} -DBRPC_WITH_GLOG=${WITH_GLOG_VAL} -DGFLAGS_NS=${GFLAGS_NS}")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DBTHREAD_USE_FAST_PTHREAD_MUTEX -D__const__=__unused__ -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D__STRICT_ANSI__")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEBUG_SYMBOL}")

if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -msse4 -msse4.2")
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer -Werror=unused-variable -Werror=return-type -Werror=unused-parameter -Werror=unreachable-code -Werror=logical-op -Wno-sign-compare -Wno-reorder")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")

# 新旧版本都设定为 C++11 标准
macro(use_cxx11)
    if(CMAKE_VERSION VERSION_LESS "3.1.3")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    else()
        set(CMAKE_CXX_STANDARD 17)
        set(CMAKE_CXX_STANDARD_REQUIRED ON)
    endif()
endmacro(use_cxx11)

use_cxx11()

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
)

file(GLOB_RECURSE MRAFT_SOURSE_BRPC_TEST "${CMAKE_CURRENT_SOURCE_DIR}/brpc_test/*.cc")
file(GLOB_RECURSE MRAFT_SOURSE_RPC "${CMAKE_BINARY_DIR}/rpc/*.cc")
file(GLOB_RECURSE MRAFT_SOURSE_REPLICA "${CMAKE_CURRENT_SOURCE_DIR}/replica/*.cc")
file(GLOB_RECURSE MRAFT_SOURSE_COMMON "${CMAKE_CURRENT_SOURCE_DIR}/common/*.cc")
file(GLOB_RECURSE MRAFT_SOURSE_ELECTION "${CMAKE_CURRENT_SOURCE_DIR}/election/*.cpp")
file(GLOB_RECURSE MRAFT_SOURSE_STORAGE "${CMAKE_CURRENT_SOURCE_DIR}/storage/*.cc")
# file(GLOB_RECURSE MRAFT_SOURSE_TEST "${CMAKE_CURRENT_SOURCE_DIR}/test/*.cc")

set(MRAFT_SOURSE
    ${MRAFT_SOURSE_BRPC_TEST}
    ${MRAFT_SOURSE_RPC}
    ${MRAFT_SOURSE_REPLICA}
    ${MRAFT_SOURSE_COMMON}
    ${MRAFT_SOURSE_ELECTION}
    ${MRAFT_SOURSE_STORAGE}
    # ${MRAFT_SOURSE_TEST}
)

set(SOURCES
    ${MRAFT_SOURSE}
    ${PROTO_SRCS}
)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_BINARY_DIR}
)

add_library(MRAFT_LIB OBJECT ${SOURCES})
target_link_libraries(MRAFT_LIB ${DYNAMIC_LIB})

# brpc 测试用的可执行文件
add_executable(brpc_client brpc_test/client.cpp ${SOURCES})
target_link_libraries(brpc_client ${DYNAMIC_LIB})

add_executable(brpc_server brpc_test/server.cpp ${SOURCES})
target_link_libraries(brpc_server ${DYNAMIC_LIB})

# bthread 测试用的可执行文件
add_executable(bthread_test brpc_test/test.cpp ${SOURCES})
target_link_libraries(bthread_test ${DYNAMIC_LIB})

# ############################ gtest #############################

# # Enable testing
# enable_testing()

# # Include GoogleTest directory
# find_package(GTest REQUIRED)

# # Get all test source files that match the pattern
# file(GLOB TEST_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test/*test.cpp)

# # For each source file, create an executable and a test
# foreach(TEST_SOURCE ${TEST_SOURCES})
# # Get the filename without the extension
# get_filename_component(TEST_NAME ${TEST_SOURCE} NAME_WE)

# # Add an executable for this source file
# add_executable(${TEST_NAME} ${TEST_SOURCE})

# # Link the executable with GoogleTest and the project library
# target_link_libraries(${TEST_NAME} ${GTEST_LIBRARIES} MRAFT_LIB)

# # Add a test for this executable
# add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
# endforeach()

# ############################ gtest end #############################