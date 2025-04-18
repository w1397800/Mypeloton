//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_record.h
//
// Identification: src/include/logging/records/transaction_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/printable.h"
#include "storage/peloton/logging/log_record.h"
#include "storage/peloton/type/serializeio.h"
#include "storage/peloton/type/serializer.h"

//===--------------------------------------------------------------------===//
// TableRecord
//===--------------------------------------------------------------------===//

class TableRecord : public LogRecord, Printable {
   public:
    TableRecord(LogRecordType log_record_type)
        : LogRecord(log_record_type, INVALID_CID) {
        db_oid = INVALID_OID;
        table_oid = INVALID_OID;
        table_name = "placeholder";
        data = nullptr;
    }

    TableRecord(LogRecordType log_record_type, cid_t cid, uint db_oid,
                uint table_oid, std::string table_name, uint16 move_pos,
                vector<uint> pk_cols, std::string primary_index_name,
                std::vector<std::pair<std::string, std::vector<uint>>>
                    unique_index_info,
                const void *data = nullptr)
        : LogRecord(log_record_type, cid),
          db_oid(db_oid),
          table_oid(table_oid),
          table_name(table_name),
          move_pos(move_pos),
          data(data),
          primary_index_name_(primary_index_name),
          pk_cols_(pk_cols),
          unique_index_info_(unique_index_info) {}

    ~TableRecord() {
        // Clean up the message
        delete[] message;
    }

    //===--------------------------------------------------------------------===//
    // Serial/Deserialization
    //===--------------------------------------------------------------------===//

    bool Serialize(CopySerializeOutput &output);

    void SerializeHeader(CopySerializeOutput &output);

    void DeserializeHeader(CopySerializeInput &input);

    //===--------------------------------------------------------------------===//
    // Accessors
    //===--------------------------------------------------------------------===//

    uint GetDatabaseOid() const { return db_oid; }

    uint GetTableId(void) const { return table_oid; }

    std::string GetTableName(void) const { return table_name; }

    uint16 GetMovePos(void) const { return move_pos; }

    void SetSchema(Schema *tuple);

    void SetSchemaData(char *data, size_t size);

    char *GetSchemaData();

    size_t GetSchemaSize() const;

    Schema *GetSchema();

    static size_t GetTableRecordSize(void);

    // Get a string representation for debugging
    const std::string GetInfo() const;

    std::string GetPrimaryIndexName() { return primary_index_name_; }

    std::vector<std::pair<std::string, std::vector<uint>>>
    GetUniqueIndexInfo() {
        return unique_index_info_;
    }

    std::vector<uint> GetPk_cols() { return pk_cols_; }

   private:
    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // table id
    uint table_oid = INVALID_OID;

    // database id
    uint db_oid = 12345;  // DEFAULT_DB_ID;

    std::string table_name;

    uint16 move_pos = 0;

    // message
    const void *data = nullptr;

    // schema (for deserialize
    Schema *schema = nullptr;

    // size of schema (serialized)
    char *schema_data_ = nullptr;
    uint schema_size_ = 0;

    std::string primary_index_name_;
    std::vector<uint> pk_cols_;  //主键列们

    // 第一个为索引名，第二个为索引列们
    std::vector<std::pair<std::string, std::vector<uint>>> unique_index_info_;
};
