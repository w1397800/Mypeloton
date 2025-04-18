//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.h
//
// Identification: src/include/storage/tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TUPLE
//#define TUPLE
#include <memory>

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/abstract_tuple.h"
#include "storage/peloton/type/abstract_pool.h"

#include "storage/peloton/type/serializeio.h"
#include "storage/peloton/type/serializer.h"
#include "storage/peloton/type/value.h"
#include "storage/peloton/type/value_factory.h"

//===--------------------------------------------------------------------===//
// Tuple class
//===--------------------------------------------------------------------===//

class Tuple : public AbstractTuple {
  friend class Schema;
//  friend class ValuePeeker;
  friend class Tile;

 public:
  // Default constructor (don't use this)
  inline Tuple()
      : tuple_schema_(nullptr), tuple_data_(nullptr), allocated_(false) {}

  // Setup the tuple given a tuple
  inline Tuple(const Tuple &rhs)
      : tuple_schema_(rhs.tuple_schema_),
        tuple_data_(rhs.tuple_data_),
        allocated_(false) {}

  // Setup the tuple given a schema
  inline Tuple(const Schema *schema)
      : tuple_schema_(schema), tuple_data_(nullptr), allocated_(false) {
    PELOTON_ASSERT(tuple_schema_);
  }

  // Setup the tuple given a schema and location
  inline Tuple(const Schema *schema, char *data)
      : tuple_schema_(schema), tuple_data_(data), allocated_(false) {
    PELOTON_ASSERT(tuple_schema_);
    PELOTON_ASSERT(tuple_data_);
  }

  // Setup the tuple given a schema and location and nvm location
  inline Tuple(const Schema *schema, char *data, char *data_nvm)
      : tuple_schema_(schema), tuple_data_(data), allocated_(false),
        tuple_data_nvm_(data_nvm){
    PELOTON_ASSERT(tuple_schema_);
    PELOTON_ASSERT(tuple_data_);
  }


  // Setup the tuple given a schema and allocate space
  inline Tuple(const Schema *schema, bool allocate)
      : tuple_schema_(schema), tuple_data_(nullptr), allocated_(allocate) {
    PELOTON_ASSERT(tuple_schema_);
    key_length_ = 0;
    if (allocated_) {
      // initialize heap allocation
      tuple_data_ = new char[tuple_schema_->GetLength()]();
    }
  }

  // Deletes tuple data
  // Does not delete either SCHEMA
  ~Tuple();

  // Setup the tuple given the specified data location and schema
  Tuple(char *data, Schema *schema);

  // Assignment operator
  Tuple &operator=(const Tuple &rhs);

  void Copy(const void *source, AbstractPool *pool = NULL);

  /**
   * Set the tuple to point toward a given address in a table's
   * backing store
   */
  inline void Move(void *address) {
    tuple_data_ = reinterpret_cast<char *>(address);
  }

  bool operator==(const Tuple &other) const;
  bool operator!=(const Tuple &other) const;

  int Compare(const Tuple &other) const;

  int Compare(const Tuple &other, const std::vector<uint> &columns) const;

  //===--------------------------------------------------------------------===//
  // Getters and Setters
  //===--------------------------------------------------------------------===//

  // This is used to access the internal array to read simple data types
  // such as integer type
  template <typename ColumnType>
  inline ColumnType GetInlinedDataOfType(uint column_id) const;

  // Get the value of a specified column (const)
  // (expensive) checks the schema to see how to return the Value.
    Value GetValue(uint column_id) const;

  /**
   * Allocate space to copy strings that can't be inlined rather
   * than copying the pointer.
   * It is also possible to provide NULL for stringPool in which case
   * the strings will be allocated on the heap.
   */
  void SetValue(const uint column_id, const Value &value,
                AbstractPool *dataPool);

  // set value without data pool.
  // This just calls the other SetValue with a nullptr pool
  void SetValue(uint column_id, const Value &value) {
    SetValue(column_id, value, nullptr);
  }

  void SetValueNew(const uint column_offset, char *src, uint var_len, AbstractPool *pool);

  void SetValueMemAndNVM(const uint column_offset, char *src, uint var_len, AbstractPool *pool);

  inline int GetLength() const { return tuple_schema_->GetLength(); }

  inline int GetKeyLength() const { return key_length_; }

  inline void SetKeyLength(uint len) { key_length_ = len; }

  // Is the column value null ?
  inline bool IsNull(const uint64_t column_id) const {
    Value value = (GetValue(column_id));
    return value.IsNull();
  }

  // Is the tuple null ?
  inline bool IsNull() const { return tuple_data_ == NULL; }

  // Get the type of a particular column in the tuple
  inline TypeId GetType(int column_id) const {
    return tuple_schema_->GetType(column_id);
  }

  inline const Schema *GetSchema() const { return tuple_schema_; }

  // Get the address of this tuple in the table's backing store
  inline char *GetData() const { return tuple_data_; }

  char *GetDataPtr(const uint column_id);
  char *GetDataPtr_nvm(const uint column_id);

  inline void SetDataAddress (char* buf){


    tuple_data_buf = buf;
  }

  inline char *GetDataAddress() const{
    return tuple_data_buf;

  }

  inline void SetBuf_offset (vector<uint> buf_offset){
    buf_offset_ = buf_offset;
  }
  inline vector<uint> GetBuf_offset() const {
    return buf_offset_;

  }

  inline void SetData_nvm (char * tuple_data_nvm){
    tuple_data_nvm_ = tuple_data_nvm;
  }

  const char *GetDataPtr(const uint column_id) const;

  // Return the number of columns in this tuple
  inline uint GetColumnCount() const {
    return tuple_schema_->GetColumnCount();
  }

  bool EqualsNoSchemaCheck(const AbstractTuple &other) const;

  bool EqualsNoSchemaCheck(const AbstractTuple &other,
                           const std::vector<uint> &columns) const;

  // this does set NULL in addition to clear string count.
  void SetAllNulls();

  void SetNull() { tuple_data_ = NULL; }

  // this does set 0 to all values. VarlenValue is set to "0"
  void SetAllZeros();

  /**
   * Determine the maximum number of bytes when serialized for Export.
   * Excludes the bytes required by the row header (which includes
   * the null bit indicators) and ignores the width of metadata columns.
   */
  size_t ExportSerializationSize() const;

  // Return the amount of memory allocated for non-inlined objects
  size_t GetUninlinedMemorySize() const;

  // This sets the relevant columns from the source tuple
  void SetFromTuple(const AbstractTuple *tuple,
                    const std::vector<uint> &columns,
                    AbstractPool *pool);

  void SetFromTupleAsIndex(const Tuple *tuple,
          const std::vector<uint> &columns);

  void SetFromTupleNew(const Tuple *tuple, const std::vector<uint> &columns);

  // Used to wrap read only tuples in indexing code.
  void MoveToTuple(const void *address);

  //===--------------------------------------------------------------------===//
  // Serialization utilities
  //===--------------------------------------------------------------------===//

  void SerializeTo(SerializeOutput &output);

  void VarSerializeTo(uint32_t len, char *dist, char *src, AbstractPool *pool);//1119
  void CharSerializeTo(uint32_t len, char *dist, char *src, AbstractPool *pool);//1119
  void FixBigIntSerializeTo(char *dist, int64_t src);//1119
  void FixDoubleSerializeTo(char *dist, double src);//1119
  void Fix32SerializeTo(char *dist, int32_t src);//1119
  void Fix16SerializeTo(char *dist, int16_t src);//1119
  void Fix8SerializeTo(char *dist, int8_t src);//1119

  void VarSerializeTo_nvm(uint32_t len, char *dist, char *src, AbstractPool *pool);//1119
  void CharSerializeTo_nvm(uint32_t len, char *dist, char *src, AbstractPool *pool);//1119
  void FixBigIntSerializeTo_nvm(char *dist, int64_t src);//1119
  void FixDoubleSerializeTo_nvm(char *dist, double src);//1119
  void Fix32SerializeTo_nvm(char *dist, int32_t src);//1119
  void Fix16SerializeTo_nvm(char *dist, int16_t src);//1119
  void Fix8SerializeTo_nvm(char *dist, int8_t src);//1119

  void SerializeToExport(SerializeOutput &output, int col_offset,
                         uint8_t *null_array);

  void SerializeWithHeaderTo(SerializeOutput &output);

  void DeserializeFrom(SerializeInput &input, AbstractPool *pool);
  void DeserializeWithHeaderFrom(SerializeInput &input);

  size_t HashCode(size_t seed) const;
  size_t HashCode() const;

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // The types of the columns in the tuple
  const Schema *tuple_schema_;

  // The tuple data, padded at the front by the TUPLE_HEADER
  char *tuple_data_;

  char *tuple_data_nvm_;

  char *tuple_data_buf;

  std::vector<uint> buf_offset_;//mysql buf offset 1119
  uint key_length_;

  // Allocated or not ?
  bool allocated_;
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

/*
 * GetInlineDataOfType() - This functions returns a reinterpreted object of
 *                         a type given by the caller
 *
 * Please note this function simply translates column ID into offsets into
 * the data array. Non-inlined objects and objects that need special treatment
 * could not be copied like this, and they must use a Value object
 *
 * NOTE: It assumes all fileds are inlined. This should be checked elsewhere
 */
template <typename ColumnType>
inline ColumnType Tuple::GetInlinedDataOfType(uint column_id) const {
  // The requested field must be inlined
  PELOTON_ASSERT(tuple_schema_->IsInlined(column_id) == true);
  PELOTON_ASSERT(column_id < GetColumnCount());

  // Translates column ID into a pointer and converts it to the
  // requested type
  const ColumnType *ptr =
      reinterpret_cast<const ColumnType *>(GetDataPtr(column_id));

  return *ptr;
}

// Setup the tuple given the specified data location and schema
inline Tuple::Tuple(char *data, Schema *schema) {
  PELOTON_ASSERT(data);
  PELOTON_ASSERT(schema);

  tuple_data_ = data;
  tuple_schema_ = schema;
  allocated_ = false;  // ???
}

inline Tuple &Tuple::operator=(const Tuple &rhs) {
  tuple_schema_ = rhs.tuple_schema_;
  tuple_data_ = rhs.tuple_data_;
  return *this;
}

//===--------------------------------------------------------------------===//
// Tuple Hasher
//===--------------------------------------------------------------------===//

struct TupleHasher : std::unary_function<Tuple, std::size_t> {
  // Generate a 64-bit number for the key value
  size_t operator()(Tuple tuple) const { return tuple.HashCode(); }
};

//===--------------------------------------------------------------------===//
// Tuple Comparator
//===--------------------------------------------------------------------===//

class TupleComparator {
 public:
  bool operator()(const Tuple lhs, const Tuple rhs) const {
    return lhs.EqualsNoSchemaCheck(rhs);
  }
};
//#endif