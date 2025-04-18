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

#include "storage/peloton/common/printable.h"
#include "storage/peloton/logging/log_record.h"
#include "storage/peloton/type/serializeio.h"
#include "storage/peloton/type/serializer.h"
//#include "sql/handler.h" /* handler */

//===--------------------------------------------------------------------===//
// TransactionRecord
//===--------------------------------------------------------------------===//

class TransactionRecord : public LogRecord, Printable {
   public:
    TransactionRecord(LogRecordType log_record_type,
                      const cid_t cid = INVALID_CID,
                      const eid_t epoch_id = INVALID_CID,
                      const uint32_t txn_id = 0)
        : LogRecord(log_record_type, cid), epoch_id(epoch_id), txn_id(txn_id) {}

    TransactionRecord(std::shared_ptr<PXID> xid, LogRecordType log_record_type,
                      const cid_t cid)
        : LogRecord(log_record_type, cid) {
        assert(xid);
        this->xid = xid;
    }

    ~TransactionRecord() {
        // Clean up the message
        delete[] message;
    }

    //===--------------------------------------------------------------------===//
    // Serial/Deserialization
    //===--------------------------------------------------------------------===//

    bool Serialize(CopySerializeOutput &output);

    void Deserialize(CopySerializeInput &input);

    static size_t GetTransactionRecordSize(void);

    //===--------------------------------------------------------------------===//
    // Accessors
    //===--------------------------------------------------------------------===//
    uint GetEpochId() const { return epoch_id; }

    uint GetTxnId(void) const { return txn_id; }

    std::shared_ptr<PXID> GetXid() const { return xid; }

    // Get a string representation for debugging
    const std::string GetInfo() const;

    eid_t epoch_id = 0;
    uint32_t txn_id = 0;

    // bin-log 中的事务 id
    std::shared_ptr<PXID> xid = nullptr;
};
