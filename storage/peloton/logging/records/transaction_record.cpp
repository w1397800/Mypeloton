//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_record.cpp
//
// Identification: src/logging/records/transaction_record.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/logging/records/transaction_record.h"

#include <iostream>

#include "storage/peloton/common/macros.h"

/**
 * @brief Serialize given data
 * @return true if we serialize data otherwise false
 */
bool TransactionRecord::Serialize(CopySerializeOutput &output) {
    bool status = true;
    output.Reset();

    // First, write out the log record type
    output.WriteEnumInSingleByte(log_record_type);

    // Then reserve 4 bytes for the header size to be written later
    size_t start = output.Position();
    output.WriteInt(0);
    output.WriteLong(cid);

    // Write out the header now
    int32_t header_length =
        static_cast<int32_t>(output.Position() - start - sizeof(int32_t));
    output.WriteIntAt(start, header_length);

    message_length = output.Size();
    message = new char[message_length];
    PELOTON_MEMCPY(message, output.Data(), message_length);

    return status;
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input
 */
void TransactionRecord::Deserialize(CopySerializeInput &input) {
    // Get the message length
    input.ReadInt();

    // Just grab the transaction id
    cid = (txn_id_t)(input.ReadLong());
}

// Used for peloton logging
size_t TransactionRecord::GetTransactionRecordSize(void) {
    // log_record_type + header_legnth + transaction_id
    return sizeof(char) + sizeof(int) + sizeof(long) + sizeof(long) +
           sizeof(int);
}

const std::string TransactionRecord::GetInfo() const {
    std::ostringstream os;

    os << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
    os << " #Txn ID:" << GetTransactionId() << "\n";
    os << "\n";

    return os.str();
}
