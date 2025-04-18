//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_util.h
//
// Identification: src/include/logging/logging_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
// #ifndef LOGGING_UTILL_H
// #define LOGGING_UTILL_H

#pragma once

#include <cstddef>
#include <memory>

#include "common/log_entry.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/logging/log_storage/storage_types.h"
#include "storage/peloton/logging/records/table_record.h"
#include "storage/peloton/logging/records/transaction_record.h"
#include "storage/peloton/logging/records/tuple_record.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/type/ephemeral_pool.h"
#include "storage/segment_log.h"

//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//

struct FakeTransaction;
class SimpleBuffer {
    SimpleBuffer(const SimpleBuffer &) = delete;
    SimpleBuffer &operator=(const SimpleBuffer &) = delete;
    SimpleBuffer(SimpleBuffer &&) = delete;
    SimpleBuffer &operator=(SimpleBuffer &&) = delete;

   public:
    static const size_t kDefaultSize = 1024 * 1024;
    static const size_t kDeMaxSize = 1024 * 1024 * 16;  // 16MB

    SimpleBuffer() : _size(kDefaultSize) { _buffer = new char[kDefaultSize]; }
    SimpleBuffer(size_t size) : _size(size) { _buffer = new char[size]; }

    void check_size(size_t size) {
        auto tmp = kDeMaxSize;
        CHECK_GE(tmp, size);
        if (size > _size) {
            delete[] _buffer;
            _buffer = new char[size];
            _size = size;
        }
    }

    size_t size() const { return _size; }

    char *data() { return _buffer; }

   private:
    char *_buffer;
    size_t _size;
};

class LoggingUtil {
   public:
    static size_t ReadNextFrame(butil::IOBuf &entry_data, SimpleBuffer &buffer);

    static LogRecordType GetNextLogRecordType(butil::IOBuf &entry_data);

    static bool ReadTupleRecordHeader(TupleRecord &tuple_record,
                                      butil::IOBuf &entry_data,
                                      SimpleBuffer &buffer);

    static unique_ptr<Tuple> ReadTupleRecordBody(const Schema *schema,
                                                 butil::IOBuf &entry_data,
                                                 SimpleBuffer &buffer);

    static bool ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                            butil::IOBuf &entry_data,
                                            SimpleBuffer &buffer);

    static size_t GetNextFrameSize(butil::IOBuf &entry_data);

    static void SkipRecordBody(butil::IOBuf &entry_data, SimpleBuffer &buffer);

    static void InsertTuple(TupleRecord *record);
    static void DeleteTuple(TupleRecord *record);
    static void UpdateTuple(TupleRecord *record);

    // FakeTransaction &txn
    static void *RecoveryOneTransaction(void *arg);

    //===--------------------------------------------------------------------===//
    // Old
    //===--------------------------------------------------------------------===//
    static BackendType GetBackendType(const LoggingType &logging_type);

    static bool IsBasedOnWriteAheadLogging(const LoggingType &logging_type);

    static bool IsBasedOnWriteBehindLogging(const LoggingType &logging_type);

    static void FFlushFsync(FileHandle &file_handle);

    static void FlushFdatasync(FileHandle &file_handle);

    static bool InitFileHandle(const char *name, FileHandle &file_handle,
                               const char *mode);

    static size_t GetLogFileSize(FileHandle &file_handle);

    static bool IsFileTruncated(FileHandle &file_handle, size_t size_to_read);

    static size_t GetNextFrameSize(FileHandle &file_handle);

    static LogRecordType GetNextLogRecordType(FileHandle &file_handle);

    static int ExtractNumberFromFileName(const char *name);
    static bool ReadTableRecordHeader(TableRecord &table_record,
                                      FileHandle &file_handle);
    static Schema *ReadTableRecordBody(FileHandle &file_handle);
    static Schema *ReadTableRecordBody(FileHandle &file_handle,
                                       size_t &body_size);
    static void SerializeSchemaTo(Schema *schema, SerializeOutput &output);
    static Schema *DeserializeSchemaFrom(SerializeInput &schema_input);
    static bool ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                            FileHandle &file_handle);

    static bool ReadTupleRecordHeader(TupleRecord &tuple_record,
                                      FileHandle &file_handle);

    static Tuple *ReadTupleRecordBody(const Schema *schema, AbstractPool *pool,
                                      FileHandle &file_handle);

    static void DeserializeTupleFrom(Tuple *tuple, SerializeInput &input);

    static void SkipTupleRecordBody(FileHandle &file_handle);
    static void SkipTableRecordBody(FileHandle &file_handle);

    static int GetFileCidFromFilName(const char *name);
    static int GetFileSizeFromFileName(const char *);

    static bool CreateDirectory(const char *dir_name, int mode);

    static bool RemoveDirectory(const char *dir_name, bool only_remove_file);

    // TODO: 确认 IsVisible 的正确性
    static bool IsVisible(const TileGroupHeader *const tile_group_header,
                          const uint &tuple_id, cid_t start_cid);
    // Wrappers
    /**
     * @brief Read get table based on tuple record
     * @param tuple record
     * @return data table
     */
    static DataTable *GetTable(TupleRecord &tupleRecord);
};

// #endif