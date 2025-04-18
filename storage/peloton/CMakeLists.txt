# Copyright (c) 2006, 2021, Oracle and/or its affiliates.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

#SET(PELOTON_SOURCES ha_peloton.cc data_table.cc
#        layout.cpp item_pointer.cpp util/string_util.cpp tile_group_header.cpp)



#find_package(TBB)
#INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/include)
#TARGET_LINK_LIBRARIES(mysqld ${TBB_LIBRARIES})



#list(APPEND Peloton_LINKER_LIBS ${TBB_LIBRARIES})
#if(TBB_FOUND)
#    SET(TBB_LIBRARY ${TBB_LIBRARIES} CACHE FILEPATH "TBB library")
#else(TBB_FOUND)
#    message(FATAL_ERROR ”tbb library not found”)
#endif(TBB_FOUND)

#link_directories("/home/wjh/8.0/tbb-master/build")
#target_link_libraries(libpeloton PUBLIC -ltbb ) #添加libmxnet.so
#SET(CMAKE_CXX_STANDARD 11)
#SET(CMAKE_CXX_STANDARD 17)

# ---[ intel tbb
find_package(TBB REQUIRED)
include_directories(SYSTEM ${TBB_INCLUDE_DIRS})

SET(PELOTON_PLUGIN_DYNAMIC "ha_peloton")
#set(CMAKE_C_FLAGS "-std=c99")
SET(PELOTON_SOURCES ha_peloton.cc
        store/layout.cpp
        store/abstract_table.cpp
        store/tuple.cpp
        store/tile_group.cpp
        store/tile_group_factory.cpp
        store/tile_group_header.cpp
        store/storage_manager.cpp
        store/tile_group_iterator.cpp
        store/data_table.cc
        store/table_factory.cpp
        store/tile.cpp
        store/logical_tile.cpp
        store/logical_tile_factory.cpp

        catalog/column.cpp
        catalog/manager.cpp
        catalog/schema.cpp
        catalog/constraint.cpp
#        catalog/catalog.cpp

        common/container/cuckoo_map.cpp
        common/internal_types.cpp
        common/item_pointer.cpp
        common/exception.cpp


        util/stringbox_util.cpp
        util/lock_free_array.cpp
        util/string_util.cpp

        type/type.cpp
        type/value.cpp
        type/array_type.cpp
        type/bigint_type.cpp
        type/boolean_type.cpp
        type/date_type.cpp
        type/decimal_type.cpp
        type/integer_parent_type.cpp
        type/integer_type.cpp
        type/numeric_type.cpp
        type/smallint_type.cpp
        type/timestamp_type.cpp
        type/tinyint_type.cpp
        type/varlen_type.cpp
        type/intel_pool.cpp

        function/date_functions.cpp

#        index/bwtree.cpp
        index/index.cpp
#        index/bwtree_index.cpp
        index/index_factory.cpp
        index/index_util.cpp
#       jy     
        index/masstree_index.cpp

        index/masstree/kvthread.cc
        index/masstree/clp.c
        index/masstree/json.cc
        index/masstree/kvrandom.cc
        index/masstree/str.cc
        index/masstree/string.cc
        index/masstree/string_slice.cc
#        index/masstree/masstree_scan.hh
        #index/masstree/mtd.cc
        index/masstree/epoch.cc


        concurrency/decentralized_epoch_manager.cpp
        concurrency/epoch_manager_factory.cpp
        concurrency/local_epoch.cpp
        concurrency/timestamp_ordering_transaction_manager.cpp
        concurrency/transaction_context.cpp
        concurrency/transaction_manager.cpp
        concurrency/transaction_manager_factory.cpp

        gc/gc_manager_factory.cpp
        gc/gc_manager.cpp
        gc/transaction_level_gc_manager.cpp

        traffic_cop/traffic_cop.cpp
#        settings/settings_manager.cpp
##logging part
        
#        logging/loggers/wal_backend_logger.cpp
#        logging/loggers/wal_frontend_logger.cpp
       logging/records/log_record_pool.cpp
       logging/records/table_record.cpp
       logging/records/transaction_record.cpp
       logging/records/tuple_record.cpp
       logging/backend_logger.cpp
       logging/circular_buffer_pool.cpp
#        logging/frontend_logger.cpp
       logging/log_buffer.cpp
#        logging/log_file.cpp
       logging/log_manager.cpp
#        logging/logger.cpp
       logging/logging_util.cpp
       logging/logger_configuration.cpp

        configuration/configuration.cpp
        common/printable.cpp
        store/database.cpp

        logging/checkpoint.cpp
        logging/checkpoint_manager.cpp
        logging/checkpoint/simple_checkpoint.cpp

        logging/frontend_logger.cpp

        # logging/log_storage/log_storage.cpp
        # logging/log_storage/storage_util.cpp
        # logging/logger_state_machine.cpp
        )
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-invalid-offsetof -Wno-error")

add_subdirectory(logging/mraft)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/logging/mraft/)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/logging/mraft/)
include_directories(${CMAKE_BINARY_DIR}/)

# 这下方要附上 mraft 的lib，也就是让编译器找到源文件
IF(WITH_PELOTON_STORAGE_ENGINE AND NOT WITHOUT_PELOTON_STORAGE_ENGINE)
    MYSQL_ADD_PLUGIN(peloton ${PELOTON_SOURCES} STORAGE_ENGINE DEFAULT
            LINK_LIBRARIES tbb MRAFT_LIB) 
ELSEIF(NOT WITHOUT_PELOTON_STORAGE_ENGINE)
	#    MYSQL_ADD_PLUGIN(peloton ${PELOTON_SOURCES} STORAGE_ENGINE MODULE_ONLY
    MYSQL_ADD_PLUGIN(peloton ${PELOTON_SOURCES} STORAGE_ENGINE DEFAULT
            LINK_LIBRARIES tbb MRAFT_LIB)
ENDIF()
