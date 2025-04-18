#include "storage/peloton/logging/records/table_record.h"

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/logging/logging_util.h"


/**
 * @brief Serialize given data
 * @return true if we serialize data otherwise false
 */
bool TableRecord::Serialize(CopySerializeOutput &output) {
    bool status = true;
    output.Reset();

    // Serialize the common variables such as database oid, table oid, etc.
    SerializeHeader(output);

    // Serialize other parts depends on type
    switch (GetType()) {
        case LOGRECORD_TYPE_DDL_TABLE_CREATE: {
            auto schema = (Schema *)data;
            LoggingUtil::SerializeSchemaTo(
                schema, output);  // schema->SerializeTo(output);
            break;
        }
        case LOGRECORD_TYPE_DDL_TABLE_DROP:
        case LOGRECORD_TYPE_DDL_TABLE_ALTER:
            // TODO
            break;
        default: {
            LOG_TRACE("Unsupported TABLE RECORD TYPE");
            status = false;
            break;
        }
    }

    message_length = output.Size();
    message = new char[message_length];
    PELOTON_MEMCPY(message, output.Data(), message_length);

    return status;
}

/**
 * @brief Serialize LogRecordHeader
 * @param output
 */
void TableRecord::SerializeHeader(CopySerializeOutput &output) {
    // Record LogRecordType first
    output.WriteEnumInSingleByte(log_record_type);

    size_t start = output.Position();
    // then reserve 4 bytes for the header size
    output.WriteInt(0);
    output.WriteLong(cid);
    output.WriteLong(db_oid);
    output.WriteLong(table_oid);
    output.WriteTextString(table_name);
    output.WriteShort(move_pos);

    // 写主键索引信息
    uint primary_idx_col_size = (uint)pk_cols_.size();
    output.WriteLong(primary_idx_col_size);
    // 只有当 pk_cols 有东西时，说明才有主键索引
    if (primary_idx_col_size > 0) {
        assert(!primary_index_name_.empty());
        output.WriteTextString(primary_index_name_);
        for (const auto col : pk_cols_) {
            output.WriteLong(col);
        }
    }

    // 写其他索引信息
    uint unique_idx_info_num = (uint)unique_index_info_.size();
    output.WriteLong(unique_idx_info_num);
    // 只有当 unique_index_info_ 有东西时，说明才有其他索引
    if (unique_idx_info_num > 0) {
        for (const auto &info : unique_index_info_) {
            auto idx_name = info.first;
            auto cols = info.second;
            auto col_size = (uint)cols.size();

            output.WriteTextString(idx_name);
            output.WriteLong((col_size));
            for (const auto col : cols) {
                output.WriteLong(col);
            }
        }
    }

    // header length
    output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start -
                                                  sizeof(int32_t)));
}

/**
 * @brief Deserialize LogRecordHeader
 * @param input
 */
void TableRecord::DeserializeHeader(CopySerializeInput &input) {
    input.ReadInt();
    cid = (cid_t)(input.ReadLong());
    db_oid = (uint)(input.ReadLong());
    table_oid = (uint)(input.ReadLong());
    table_name = (std::string)(input.ReadTextString());
    move_pos = (uint16)(input.ReadShort());

    // 读主键索引信息
    uint primary_idx_col_size = input.ReadLong();
    // 只有当 pk_cols 有东西时，说明才有主键索引
    if (primary_idx_col_size > 0) {
        primary_index_name_ = input.ReadTextString();
        for (uint i = 0; i < primary_idx_col_size; i++) {
            pk_cols_.emplace_back(input.ReadLong());
        }
    }

    // 写其他索引信息
    uint unique_idx_info_num = input.ReadLong();
    // 只有当 unique_index_info_ 有东西时，说明才有其他索引
    if (unique_idx_info_num > 0) {
        for (uint i = 0; i < unique_idx_info_num; i++) {
            std::string idx_name = input.ReadTextString();
            uint col_size = input.ReadLong();
            vector<uint> cols;

            for (uint i = 0; i < col_size; i++) {
                cols.emplace_back(input.ReadLong());
            }

            unique_index_info_.emplace_back(idx_name, cols);
        }
    }
}

// Used for write behind logging
size_t TableRecord::GetTableRecordSize(void) {
    // log_record_type + header_length + db_oid + table_oid + table_name
    return 0; /*sizeof(char) + sizeof(int) + sizeof(uint) + sizeof(uint) + 20;*/
}

void TableRecord::SetSchema(Schema *schema) { this->schema = schema; }

void TableRecord::SetSchemaData(char *schema_data, size_t size) {
    this->schema_data_ = schema_data;
    this->schema_size_ = size;
}

char *TableRecord::GetSchemaData() { return this->schema_data_; }

size_t TableRecord::GetSchemaSize() const { return this->schema_size_; }

Schema *TableRecord::GetSchema() { return schema; }

const std::string TableRecord::GetInfo() const {
    std::ostringstream os;

    os << "#LOG TYPE:" << LogRecordTypeToString(GetType()) << "\n";
    os << " #Db  ID:" << GetDatabaseOid() << "\n";
    os << " #Tb  ID:" << GetTableId() << "\n";
    os << " #Tb Name:" << GetTableName() << "\n";
    os << "\n";

    return os.str();
}
