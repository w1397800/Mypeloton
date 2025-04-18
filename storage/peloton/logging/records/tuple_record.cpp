//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_record.cpp
//
// Identification: src/logging/records/tuple_record.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/logging/records/tuple_record.h"

#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/logging/logging_util.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/tile.h"
#include "storage/peloton/store/tuple.h"

TupleRecord::TupleRecord(LogRecordType log_record_type, const cid_t cid,
                         ItemPointer item_pointer)
    : LogRecord(log_record_type, cid), item_pointer_(item_pointer) {
    auto storage_manager = StorageManager::GetInstance();
    auto tile_group = storage_manager->GetTileGroup(item_pointer.block);
    table_oid = tile_group->GetTableId();
    db_oid = tile_group->GetDatabaseId();
}

/**
 * @brief Serialize tuple from given item pointer
 * @return true if we serialize data otherwise false
 */
void TupleRecord::SerializeTuple(CopySerializeOutput &output) {
    auto tile_group =
        StorageManager::GetInstance()->GetTileGroup(item_pointer_.block).get();
    Tile *tile = tile_group->GetTile(0);
    auto tuple_id = item_pointer_.offset;
    auto tuple_pos = tile->GetTupleLocation(tuple_id);
    auto schema = tile->GetSchema();
    auto column_count = schema->GetColumnCount();

    size_t start = output.ReserveBytes(4);
    for (int col = 0; col < column_count; col++) {
        const Column &column = schema->GetColumn(col);
        const auto pos = tuple_pos + schema->GetOffset(col);
        auto type = column.GetType();
        if (TypeId::VARCHAR == type) {
            // VARCHAR in peloton is stored as length(uint32) + data(char*) in
            // a detached place. So the actual data is only a pointer.
            const char *actual_pos =
                *reinterpret_cast<const char *const *>(pos);
            uint32_t actual_len =
                *reinterpret_cast<const uint32_t *>(actual_pos);
            output.WriteBytes(actual_pos, actual_len + sizeof(uint32_t));

        } else {
            output.WriteBytes(pos, column.GetLength());
        }
    }
    output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start -
                                                  sizeof(int32_t)));
}

/**
 * @brief Serialize given data
 * @return true if we serialize data otherwise false
 */
bool TupleRecord::Serialize(CopySerializeOutput &output) {
    bool status = true;
    output.Reset();

    // Serialize the common variables such as database oid, table oid, etc.
    SerializeHeader(output);

    // Serialize other parts depends on type
    switch (GetType()) {
        case LOGRECORD_TYPE_TUPLE_INSERT:
        case LOGRECORD_TYPE_TUPLE_UPDATE:
        case LOGRECORD_TYPE_TUPLE_DELETE: {
            SerializeTuple(output);
            break;
        }

        default: {
            LOG_TRACE("Unsupported TUPLE RECORD TYPE");
            status = false;
            break;
        }
    }

    message_length = output.Size();
    message = new char[message_length];
    PELOTON_MEMCPY(message, output.Data(), message_length);
    LOG(INFO) << "TupleRecord::Serialize() message_length = " << message_length;
    return status;
}

/**
 * @brief Serialize LogRecordHeader
 * @param output
 */
void TupleRecord::SerializeHeader(CopySerializeOutput &output) {
    // Record LogRecordType first
    output.WriteEnumInSingleByte(log_record_type);

    size_t start = output.Position();
    // then reserve 4 bytes for the header size
    output.WriteInt(0);

    output.WriteLong(db_oid);
    output.WriteLong(table_oid);
    output.WriteLong(cid);

    output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start -
                                                  sizeof(int32_t)));
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input
 */
void TupleRecord::DeserializeHeader(CopySerializeInput &input) {
    input.ReadInt();
    db_oid = (uint)(input.ReadLong());
    table_oid = (uint)(input.ReadLong());
    cid = (txn_id_t)(input.ReadLong());
    PELOTON_ASSERT(cid);
}

Tuple *TupleRecord::GetTuple() { return _tuple.get(); }

const std::string TupleRecord::GetInfo() const {
    std::ostringstream os;

    os << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
    os << " #Db  ID:" << GetDatabaseOid() << "\n";
    os << " #Tb  ID:" << GetTableId() << "\n";
    os << " #Txn ID:" << GetTransactionId() << "\n";
    os << "\n";

    return os.str();
}
