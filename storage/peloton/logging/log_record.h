//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_record.h
//
// Identification: src/include/logging/log_record.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/* The following entry types are distinguished:
 *
 * Possible Log Entries:
 *
 *     Transaction Record :
 *       - LogRecordType         : enum
 *     - HEADER
 *       - Header length         : int
 *       - Transaction Id        : txn_id_t(cid)
 *       - Epoch Id              : eid_t
 *       - Txn Id                : uint32_t (for recovery epoch manager)
 *
 *     Tuple Record :
 *       - LogRecordType         : enum
 *     -HEADER
 *       - Header length         : int
 *       - Database Oid          : uint
 *       - Table Oid             : uint
 *       - Transaction Id        : txn_id_t
 *       - Inserted Location     : ItemPointer
 *       - Deleted Location      : ItemPointer
 *     -BODY
 *       - Body length           : int
 *       - Data                  : void*
 *
 *     Table Record :
 *       - LogRecordType         : enum
 *     -HEADER
 *       - Header length         : int
 *       - Database Oid          : uint
 *       - Table Oid             : uint
 *       - Table Name            : std::string
 *       - Move Pos              : uint16
 *     -BODY
 *       - Body length           : int
 *       - Data                  : void*
 *
 */

#pragma once

#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/type/serializeio.h"
#include "storage/peloton/type/serializer.h"

//===--------------------------------------------------------------------===//
// LogRecord
//===--------------------------------------------------------------------===//

class LogRecord {
   public:
    LogRecord(LogRecordType log_record_type, cid_t cid)
        : log_record_type(log_record_type), cid(cid) {
        PELOTON_ASSERT(log_record_type != LOGRECORD_TYPE_INVALID);
    }

    virtual ~LogRecord() {}

    LogRecordType GetType() const { return log_record_type; }

    cid_t GetTransactionId() const { return cid; }

    virtual bool Serialize(CopySerializeOutput &output) = 0;

    char *GetMessage(void) const { return message; }

    size_t GetMessageLength(void) const { return message_length; }

   protected:
    LogRecordType log_record_type = LOGRECORD_TYPE_INVALID;

    cid_t cid;

    // serialized message
    char *message = nullptr;

    // length of the message
    size_t message_length = 0;
};
