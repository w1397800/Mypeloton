//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.cpp
//
// Identification: src/storage/tuple.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <sstream>

#include "storage/peloton/common/macros.h"
#include "storage/peloton/catalog/schema.h"
#include "tuple.h"
#include "storage/peloton/type/abstract_pool.h"
#include "storage/peloton/type/intel_pool.h"
//#include "common/exception.h"
//#include "common/logger.h"
//#include "storage/peloton/type/value.h"



// Does not delete SCHEMA
Tuple::~Tuple() {
  // delete the tuple data
  if (allocated_) delete[] tuple_data_;
}

// Get the value of a specified column (const)
Value Tuple::GetValue(uint column_id) const {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  const TypeId column_type = tuple_schema_->GetType(column_id);
  const char *data_ptr = GetDataPtr(column_id);
  const bool is_inlined = tuple_schema_->IsInlined(column_id);
  return Value::DeserializeFrom(data_ptr, column_type, is_inlined);
}

// Set all columns by value into this tuple.
void Tuple::SetValue(const uint column_offset, const Value &value,
                     AbstractPool *data_pool) {
  const TypeId type = tuple_schema_->GetType(column_offset);
//  LOG_TRACE("c offset: %d; using pool: %p", column_offset, data_pool);

  const bool is_inlined = tuple_schema_->IsInlined(column_offset);
  char *value_location = GetDataPtr(column_offset);
  UNUSED_ATTRIBUTE size_t column_length =
      tuple_schema_->GetLength(column_offset);

//  LOG_TRACE("column_offset: %d; value_location %p; column_length %lu; type %s",
//            column_offset, value_location, column_length,
//            TypeIdToString(type).c_str());

  // Skip casting if type is same
  if (type == value.GetTypeId()) {
    if ((type == TypeId::VARCHAR || type == TypeId::VARBINARY)
        && (column_length != 0 && value.GetLength() != PELOTON_VALUE_NULL)
        && value.GetLength() > column_length + 1) {      // value.GetLength() == strlen(value) + 1 because of '\0'
//      throw ValueOutOfRangeException(type, column_length);
    }
    value.SerializeTo(value_location, is_inlined, data_pool);
  } else {
    Value casted_value = (value.CastAs(type));
    casted_value.SerializeTo(value_location, is_inlined, data_pool);
  }
}

//1119 直接将buf的内容按照长度copy至tuple中的tuple_data_
void Tuple::SetValueNew(const uint column_offset, char *src, uint var_len, AbstractPool *pool) {
  const TypeId typeId = tuple_schema_->GetType(column_offset);

  char *value_location = GetDataPtr(column_offset);

//  char *value_location_nvm = GetDataPtr_nvm(column_offset);
//    size_t column_length = tuple_schema_->GetLength(column_offset);

  if (TypeId::TINYINT == typeId ||TypeId::BOOLEAN ==  typeId) {
    Fix8SerializeTo(value_location, *(int8_t*)src);
  }else if (TypeId::SMALLINT == typeId) {
    Fix16SerializeTo(value_location, *(int16_t*)src);
  }else if (TypeId::INTEGER == typeId ||TypeId::DATE == typeId ) {
    Fix32SerializeTo(value_location, *(int32_t*)src);
  }else if (TypeId::BIGINT == typeId) {
    FixBigIntSerializeTo(value_location, *(int64_t*)src);
  }else if (TypeId::DECIMAL == typeId ) {
    FixDoubleSerializeTo(value_location, *(double*)src);
  }else if (TypeId::CHAR == typeId ) {
    CharSerializeTo(var_len, value_location, src, pool);
  }else if (TypeId::VARCHAR == typeId) {
    //1119
    //字符串
    VarSerializeTo(var_len, value_location, src, pool);
  }
}

void Tuple::SetValueMemAndNVM(const uint column_offset, char *src, uint var_len, AbstractPool *pool) {
  const TypeId typeId = tuple_schema_->GetType(column_offset);

  char *value_location = GetDataPtr(column_offset);
  char *value_location_nvm;
  if(FOR_NVM){
    value_location_nvm = GetDataPtr_nvm(column_offset);
  }

  if (TypeId::TINYINT == typeId ||TypeId::BOOLEAN ==  typeId) {
    Fix8SerializeTo(value_location, *(int8_t*)src);
    if(FOR_NVM){
      Fix8SerializeTo_nvm(value_location_nvm, *(int8_t*)src);
    }
  }else if (TypeId::SMALLINT == typeId) {
    Fix16SerializeTo(value_location, *(int16_t*)src);
    if(FOR_NVM){
      Fix16SerializeTo_nvm(value_location_nvm, *(int16_t*)src);
    }
  }else if (TypeId::INTEGER == typeId ||TypeId::DATE == typeId ) {
//  }else if (TypeId::INTEGER == typeId ) {
    Fix32SerializeTo(value_location, *(int32_t*)src);
    if(FOR_NVM){
      Fix32SerializeTo_nvm(value_location_nvm, *(int32_t*)src);
    }
  }else if (TypeId::BIGINT == typeId||TypeId::TIMESTAMP == typeId ) {
//  }else if (TypeId::BIGINT == typeId||TypeId::DATE == typeId ) {
    FixBigIntSerializeTo(value_location, *(int64_t*)src);
    if(FOR_NVM){
      FixBigIntSerializeTo_nvm(value_location_nvm, *(int64_t*)src);
    }
  }else if (TypeId::DECIMAL == typeId ) {
    FixDoubleSerializeTo(value_location, *(double*)src);
    if(FOR_NVM){
      FixDoubleSerializeTo_nvm(value_location_nvm, *(double*)src);
    }
  }else if (TypeId::CHAR == typeId ) {
    CharSerializeTo(var_len, value_location, src, pool);
    if(FOR_NVM){
      CharSerializeTo_nvm(var_len, value_location_nvm, src, pool);
    }
  }else if (TypeId::VARCHAR == typeId) {
    //1119
    //字符串
    VarSerializeTo(var_len, value_location, src, pool);
    if(FOR_NVM){
      VarSerializeTo_nvm(var_len, value_location_nvm, src, pool);
    }
  }
}

//字段长度, dist: 最终tuple里的tuple_data_, src.
void Tuple::CharSerializeTo(uint32_t len, char *dist, char *src, AbstractPool *pool)  {
  (void)pool;
  PELOTON_MEMCPY(dist, src, len);
}

//字段长度, dist: 最终tuple里的tuple_data_, src.
void Tuple::CharSerializeTo_nvm(uint32_t len, char *dist, char *src, AbstractPool *pool)  {
  char *data;
  //  pool = IntelPool::GetInstance();
  //  len+=1;
  if (len == PELOTON_VALUE_NULL) {
    data = nullptr;
  } else {
    uint32_t size = len + sizeof(uint32_t);
    //    data = new char[size];
    data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
    PELOTON_MEMCPY(data, &len, sizeof(uint32_t));//字符串长度
    PELOTON_MEMCPY(data + sizeof(uint32_t), src, len);
  }
  *reinterpret_cast<const char **>(dist) = data;
}

void Tuple::FixBigIntSerializeTo(char *dist, int64_t src)  {
  *reinterpret_cast<int64_t *>(dist) = src;
}

void Tuple::FixDoubleSerializeTo(char *dist, double src)  {
  *reinterpret_cast<double *>(dist) = src;
}

void Tuple::Fix32SerializeTo(char *dist, int32_t src)  {
  *reinterpret_cast<int32_t *>(dist) = src;
}

void Tuple::Fix16SerializeTo(char *dist, int16_t src)  {
  *reinterpret_cast<int16_t *>(dist) = src;
}

void Tuple::Fix8SerializeTo(char *dist, int8_t src)  {
  *reinterpret_cast<int8_t *>(dist) = src;
}

void Tuple::FixBigIntSerializeTo_nvm(char *dist, int64_t src)  {
  *reinterpret_cast<int64_t *>(dist) = src;
}

void Tuple::FixDoubleSerializeTo_nvm(char *dist, double src)  {
  *reinterpret_cast<double *>(dist) = src;
}

void Tuple::Fix32SerializeTo_nvm(char *dist, int32_t src)  {
  *reinterpret_cast<int32_t *>(dist) = src;
}

void Tuple::Fix16SerializeTo_nvm(char *dist, int16_t src)  {
  *reinterpret_cast<int16_t *>(dist) = src;
}

void Tuple::Fix8SerializeTo_nvm(char *dist, int8_t src)  {
  *reinterpret_cast<int8_t *>(dist) = src;
}

//字段长度, dist: 最终tuple里的tuple_data_, src.
void Tuple::VarSerializeTo(uint32_t len, char *dist, char *src, AbstractPool *pool)  {
  char *data;
//  pool = IntelPool::GetInstance();
//  len+=1;
  if (len == PELOTON_VALUE_NULL) {
    data = nullptr;
  } else {
    uint32_t size = len + sizeof(uint32_t);
//    data = new char[size];
    data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
    PELOTON_MEMCPY(data, &len, sizeof(uint32_t));//字符串长度
    PELOTON_MEMCPY(data + sizeof(uint32_t), src, len);
  }
  *reinterpret_cast<const char **>(dist) = data;
}

//字段长度, dist: 最终tuple里的tuple_data_, src.
void Tuple::VarSerializeTo_nvm(uint32_t len, char *dist, char *src, AbstractPool *pool)  {
  char *data;
//  pool = IntelPool::GetInstance();
//  len+=1;
  if (len == PELOTON_VALUE_NULL) {
    data = nullptr;
  } else {
    uint32_t size = len + sizeof(uint32_t);
//    data = new char[size];
    data = (pool == nullptr) ? new char[size] : (char *)pool->Allocate(size);
    PELOTON_MEMCPY(data, &len, sizeof(uint32_t));//字符串长度
    PELOTON_MEMCPY(data + sizeof(uint32_t), src, len);
  }
  *reinterpret_cast<const char **>(dist) = data;
}

void Tuple::SetFromTuple(const AbstractTuple *tuple,
                         const std::vector<uint> &columns,
                         AbstractPool *pool) {
  // We don't do any checks here about the source tuple and
  // this tuple's schema
  uint this_col_itr = 0;
  for (auto col : columns) {
    Value fetched_value = (tuple->GetValue(col));
    SetValue(this_col_itr, fetched_value, pool);
    this_col_itr++;
  }
}

void Tuple::SetFromTupleAsIndex(const Tuple *tuple,
                            const std::vector<uint> &columns) {
  assert(tuple);
  uint column_id = 0;
  const Schema *schema = tuple_schema_;
  const auto src_schema = tuple->GetSchema();
  char *tuple_pos = tuple->GetData();
  for (auto col : columns) {
    const Column &column = schema->GetColumn(column_id);
    auto typeId = column.GetType();
    const auto pos = tuple_pos + src_schema->GetOffset(col);
    if (TypeId::VARCHAR == typeId) {
      auto var_len = *(uint32_t*)(pos);

      SetValueNew(column_id, pos + sizeof(uint32_t), var_len+INDEXPARM, nullptr);
      key_length_+=column.GetLength()+4;

    }else {
      SetValueNew(column_id, pos, -1, nullptr);
      key_length_+=column.GetLength()+1;
    }
    column_id++;
  }
}

void Tuple::SetFromTupleNew(const Tuple *tuple,
                            const std::vector<uint> &columns) {
  // We don't do any checks here about the source tuple and
  // this tuple's schema
  uint column_id = 0;
  const Schema *schema = tuple_schema_;
  char *data_buf = tuple->GetDataAddress();
  vector<uint> buf_offset = tuple->GetBuf_offset();
  for (auto col : columns) {
    uint cur_offset = buf_offset.at(col);
    //SetValueNew(const uint column_offset, char *src, uint var_len)
    const Column &column = schema->GetColumn(column_id);
    auto typeId = column.GetType();
    if (TypeId::VARCHAR == typeId) {
      uint var_len = 0;
      uint temp = 0;
      if(column.GetLength()<86){
        var_len = *(int8*)(data_buf+cur_offset);
        temp = 1;
//        data_buf += 1;
      }else {
        var_len = *(int16*)(data_buf+cur_offset);
//        data_buf += 2;
        temp = 2;
      }
      //这边+1是为了配合bwtree字符串.
//      SetValueNew(column_id, data_buf+cur_offset+temp, var_len+1, nullptr);
      SetValueNew(column_id, data_buf+cur_offset+temp, var_len+INDEXPARM, nullptr);
      key_length_+=column.GetLength()+4;
//      data_buf += column.GetLength()*3;
    }else {
      SetValueNew(column_id, data_buf+cur_offset, -1, nullptr);
      key_length_+=column.GetLength()+1;
//      data_buf += column.GetLength();
    }
    column_id++;
  }
}


// For an insert, the copy should do an allocation for all uninlinable columns
// This does not do any schema checks. They must match.
void Tuple::Copy(const void *source, AbstractPool *pool) {
  const bool is_inlined = tuple_schema_->IsInlined();
  const uint uninlineable_column_count =
      tuple_schema_->GetUninlinedColumnCount();

  if (is_inlined) {
    // copy the data
    PELOTON_MEMCPY(tuple_data_, source, tuple_schema_->GetLength());
  } else {
    // copy the data
    PELOTON_MEMCPY(tuple_data_, source, tuple_schema_->GetLength());

    // Copy each uninlined column doing an allocation for copies.
    for (uint column_itr = 0; column_itr < uninlineable_column_count;
         column_itr++) {
      const uint unlineable_column_id =
          tuple_schema_->GetUninlinedColumn(column_itr);

      // Get original value from uninlined pool
      Value value = GetValue(unlineable_column_id);

      // Make a copy of the value at a new location in uninlined pool
      SetValue(unlineable_column_id, value, pool);
    }
  }
}

/**
 * Determine the maximum number of bytes when serialized for Export.
 * Excludes the bytes required by the row header (which includes
 * the null bit indicators) and ignores the width of metadata columns.
 */
size_t Tuple::ExportSerializationSize() const {
  size_t bytes = 0;
  int column_count = GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; ++column_itr) {
    switch (GetType(column_itr)) {
      case TypeId::TINYINT:
      case TypeId::SMALLINT:
      case TypeId::INTEGER:
      case TypeId::BIGINT:
      case TypeId::TIMESTAMP:
      case TypeId::DECIMAL:
        // case Type::DOUBLE:
        bytes += sizeof(int64_t);
        break;

      // case TypeId::DECIMAL:
      // Decimals serialized in ascii as
      // 32 bits of length + max prec digits + radix pt + sign
      // bytes += sizeof(int32_t) + Value::kMaxDecPrec + 1 + 1;
      // break;

      case TypeId::VARCHAR:
      case TypeId::VARBINARY:
        // 32 bit length preceding value and
        // actual character data without null string terminator.
      if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) + GetValue(column_itr).GetLength());
        }
        break;

      default:
        break;
//        throw UnknownTypeException(
//            static_cast<int>(GetType(column_itr)),
//            "Unknown ValueType found during Export serialization.");
        return (size_t)0;
    }
  }
  return bytes;
}

// Return the amount of memory allocated for non-inlined objects
size_t Tuple::GetUninlinedMemorySize() const {
  size_t bytes = 0;
  int column_count = GetColumnCount();

  // fast-path for no inlined cols
  if (tuple_schema_->IsInlined() == false) {
    for (int column_itr = 0; column_itr < column_count; ++column_itr) {
      // peekObjectLength is unhappy with non-varchar
      if ((GetType(column_itr) == TypeId::VARCHAR ||
           (GetType(column_itr) == TypeId::VARBINARY)) &&
          !tuple_schema_->IsInlined(column_itr)) {
        if (!GetValue(column_itr).IsNull()) {
          bytes += (sizeof(int32_t) + GetValue(column_itr).GetLength());
        }
      }
    }
  }
  return bytes;
}

void Tuple::DeserializeFrom(UNUSED_ATTRIBUTE SerializeInput &input,
                            UNUSED_ATTRIBUTE AbstractPool *dataPool) {
  /*PELOTON_ASSERT(tuple_schema);
  PELOTON_ASSERT(tuple_data);

  input.ReadInt();
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema_->GetType(column_itr);*/

  /**
   * DeserializeFrom is only called when we serialize/deserialize tables.
   * The serialization format for Strings/Objects in a serialized table
   * happens to have the same in memory representation as the Strings/Objects
   * in a Tuple. The goal here is to wrap the serialized representation of
   * the value in an Value and then serialize that into the tuple from the
   * Value. This makes it possible to push more value specific functionality
   * out of Tuple. The memory allocation will be performed when serializing
   * to tuple storage.
   */
  /*const bool is_inlined = tuple_schema_->IsInlined(column_itr);
  int32_t column_length;
  char *data_ptr = GetDataPtr(column_itr);

  if (is_inlined) {
    column_length = tuple_schema_->GetLength(column_itr);
  } else {
    column_length = tuple_schema_->GetVariableLength(column_itr);
  }

  // TODO: Not sure about arguments
  const bool is_in_bytes = false;
  Value::DeserializeFrom(input, dataPool, data_ptr, type, is_inlined,
                         column_length, is_in_bytes);
}*/
}

void Tuple::DeserializeWithHeaderFrom(SerializeInput &input UNUSED_ATTRIBUTE) {
  /*PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);

  input.ReadInt();  // Read in the tuple size, discard

  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const ValueType type = tuple_schema_->GetType(column_itr);

    const bool is_inlined = tuple_schema_->IsInlined(column_itr);
    char *data_ptr = GetDataPtr(column_itr);
    const int32_t column_length = tuple_schema_->GetLength(column_itr);

    // TODO: Not sure about arguments
    const bool is_in_bytes = false;
    Value::DeserializeFrom(input, NULL, data_ptr, type, is_inlined,
                           column_length, is_in_bytes);*/
}

void Tuple::SerializeWithHeaderTo(SerializeOutput &output) {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);

  size_t start = output.Position();
  output.WriteInt(0);  // reserve first 4 bytes for the total tuple size

  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = GetValue(column_itr);
    value.SerializeTo(output);
  }

  int32_t serialized_size =
      static_cast<int32_t>(output.Position() - start - sizeof(int32_t));

  // write out the length of the tuple at start
  output.WriteIntAt(start, serialized_size);
}

void Tuple::SerializeTo(SerializeOutput &output) {
  PELOTON_ASSERT(tuple_schema_);
  size_t start = output.ReserveBytes(4);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value(GetValue(column_itr));
    value.SerializeTo(output);
  }

  output.WriteIntAt(
      start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

void Tuple::SerializeToExport(SerializeOutput &output, int colOffset,
                              uint8_t *null_array) {
  const int column_count = GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    // NULL doesn't produce any bytes for the Value
    // Handle it here to consolidate manipulation of the nullarray.
    if (IsNull(column_itr)) {
      // turn on relevant bit in nullArray
      int byte = (colOffset + column_itr) >> 3;
      int bit = (colOffset + column_itr) % 8;
      int mask = 0x80 >> bit;
      null_array[byte] = (uint8_t)(null_array[byte] | mask);
      continue;
    }

    Value val = GetValue(column_itr);
    val.SerializeTo(output);
  }
}

bool Tuple::operator==(const Tuple &other) const {
  if (tuple_schema_ != other.tuple_schema_) {
    return false;
  }

  return EqualsNoSchemaCheck(other);
}

bool Tuple::operator!=(const Tuple &other) const { return !(*this == other); }

bool Tuple::EqualsNoSchemaCheck(const AbstractTuple &other) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value lhs = (GetValue(column_itr));
    Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareNotEquals(rhs) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

bool Tuple::EqualsNoSchemaCheck(const AbstractTuple &other,
                                const std::vector<uint> &columns) const {
  for (auto column_itr : columns) {
    Value lhs = (GetValue(column_itr));
    Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareNotEquals(rhs) == CmpBool::CmpTrue) {
      return false;
    }
  }
  return true;
}

void Tuple::SetAllNulls() {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = (ValueFactory::GetNullValueByType(
        tuple_schema_->GetType(column_itr)));
    SetValue(column_itr, value, nullptr);
  }
}

void Tuple::SetAllZeros() {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value(ValueFactory::GetZeroValueByType(
        tuple_schema_->GetType(column_itr)));
    SetValue(column_itr, value, nullptr);
  }
}

int Tuple::Compare(const Tuple &other) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value lhs = (GetValue(column_itr));
    Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
      return 1;
    }
    if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
      return -1;
    }
  }
  return 0;
}

int Tuple::Compare(const Tuple &other,
                   const std::vector<uint> &columns) const {
  for (auto column_itr : columns) {
    Value lhs = (GetValue(column_itr));
    Value rhs = (other.GetValue(column_itr));
    if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
      return 1;
    }
    if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
      return -1;
    }
  }
  return 0;
}

size_t Tuple::HashCode(size_t seed) const {
  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Value value = (GetValue(column_itr));
    value.HashCombine(seed);
  }
  return seed;
}

void Tuple::MoveToTuple(const void *address) {
  PELOTON_ASSERT(tuple_schema_);
  tuple_data_ = reinterpret_cast<char *>(const_cast<void *>(address));
}

size_t Tuple::HashCode() const {
  size_t seed = 0;
  return HashCode(seed);
}

char *Tuple::GetDataPtr(const uint column_id) {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  return &tuple_data_[tuple_schema_->GetOffset(column_id)];
}

char *Tuple::GetDataPtr_nvm(const uint column_id) {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  return &tuple_data_nvm_[tuple_schema_->GetOffset(column_id)];
}

const char *Tuple::GetDataPtr(const uint column_id) const {
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple_data_);
  return &tuple_data_[tuple_schema_->GetOffset(column_id)];
}

const std::string Tuple::GetInfo() const {
  std::stringstream os;

  uint column_count = GetColumnCount();
  bool first = true;
  os << "(";
  for (uint column_itr = 0; column_itr < column_count; column_itr++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (IsNull(column_itr)) {
      os << "<NULL>";
    } else {
//      Value val = (GetValue(column_itr));
//      os << val.ToString();
    }
  }
  os << ")";
  return os.str();
}
