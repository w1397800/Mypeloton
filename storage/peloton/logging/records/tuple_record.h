//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_record.h
//
// Identification: src/include/logging/records/tuple_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/common/printable.h"
#include "storage/peloton/logging/log_record.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/type/serializeio.h"
#include "storage/peloton/type/serializer.h"

//===--------------------------------------------------------------------===//
// TupleRecord
//===--------------------------------------------------------------------===//

class TupleRecord : public LogRecord, Printable {
   public:
    TupleRecord(LogRecordType log_record_type)
        : LogRecord(log_record_type, INVALID_CID) {
        db_oid = INVALID_OID;
        table_oid = INVALID_OID;

        data = nullptr;
    }

    TupleRecord(LogRecordType log_record_type, const cid_t cid, uint table_oid,
                const void *data = nullptr, uint _db_oid = INVALID_OID)
        : LogRecord(log_record_type, cid),
          table_oid(table_oid),
          data(data),
          db_oid(_db_oid) {
    }

    TupleRecord(LogRecordType log_record_type, const cid_t cid, ItemPointer item_pointer);

    ~TupleRecord() {
        // Clean up the message
        delete[] message;
    }

    //===--------------------------------------------------------------------===//
    // Serial/Deserialization
    //===--------------------------------------------------------------------===//
    void SerializeTuple(CopySerializeOutput &output);

    bool Serialize(CopySerializeOutput &output);

    void SerializeHeader(CopySerializeOutput &output);

    void DeserializeHeader(CopySerializeInput &input);

    //===--------------------------------------------------------------------===//
    // Accessor
    //===--------------------------------------------------------------------===//

    uint GetDatabaseOid() const { return db_oid; }

    uint GetTableId(void) const { return table_oid; }

    void SetTuple(Tuple *tuple);

    void SetTuple(std::unique_ptr<Tuple> tuple) {
        _tuple.reset(tuple.release());
    }

    Tuple *GetTuple();

    // Get a string representation for debugging
    const std::string GetInfo() const;

   private:
    //===--------------------------------------------------------------------===//
    // Member Variables
    //===--------------------------------------------------------------------===//

    // table id
    uint table_oid = INVALID_OID;

    // database id
    uint db_oid = 12345;  // DEFAULT_DB_ID;

    // message
    const void *data = nullptr;

    std::unique_ptr<Tuple> _tuple;

    ItemPointer item_pointer_;
};
