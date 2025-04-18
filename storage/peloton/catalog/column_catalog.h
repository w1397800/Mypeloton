//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.h
//
// Identification: src/include/catalog/column_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_attribute
//
// Schema: (column offset: column_name)
// 0: table_oid (pkey)
// 1: column_name (pkey)
// 2: column_id (logical position, starting from 0, then 1, 2, 3....)
// 3: column_offset (physical offset instead of logical position)
// 4: column_type (type of the data stored in this column)
// 5: is_inlined (false for VARCHAR type, true for every type else)
// 6: is_primary (is this column belongs to primary key)
// 7: is_not_null
//
// Indexes: (index offset: indexed columns)
// 0: table_oid & column_name (unique & primary key)
// 1: table_oid & column_id (unique)
// 2: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_catalog.h"
#include "storage/peloton/store/logical_tile.h"


class ColumnCatalogEntry {
 public:
  ColumnCatalogEntry(LogicalTile *tile, int tupleId = 0);

  inline uint GetTableOid() { return table_oid_; }
  inline const std::string &GetColumnName() { return column_name_; }
  inline uint GetColumnId() { return column_id_; }
  inline uint GetColumnOffset() { return column_offset_; }
  inline TypeId GetColumnType() { return column_type_; }
  inline size_t GetColumnLength() { return column_length_; }
  inline bool IsInlined() { return is_inlined_; }
  inline bool IsNotNull() { return is_not_null_; }
  inline bool HasDefault() { return has_default_; }
  inline const Value &GetDefaultValue() { return default_value_; }

 private:
  // member variables
  uint table_oid_;
  std::string column_name_;
  uint column_id_;
  uint column_offset_;
  TypeId column_type_;
  size_t column_length_;
  bool is_inlined_;
  bool is_not_null_;
  bool has_default_;
  Value default_value_;
};

class ColumnCatalog : public AbstractCatalog {
  friend class ColumnCatalogEntry;
  friend class TableCatalogEntry;
  friend class Catalog;

 public:
  ColumnCatalog(TransactionContext *txn,
                Database *pg_catalog,
                AbstractPool *pool);

  ~ColumnCatalog();

  // No use
  inline uint GetNextOid() { return INVALID_OID; }

  void UpdateOid(uint add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertColumn(TransactionContext *txn,
                    uint table_oid,
                    const std::string &column_name,
                    uint column_id,
                    uint column_offset,
                    TypeId column_type,
                    size_t column_length,
                    bool is_inlined,
                    bool is_not_null,
                    bool is_default,
                    const std::shared_ptr<Value> default_value,
                    AbstractPool *pool);

  bool DeleteColumn(TransactionContext *txn,
                    uint table_oid,
                    const std::string &column_name);

  bool DeleteColumns(TransactionContext *txn, uint table_oid);

  bool UpdateNotNullConstraint(TransactionContext *txn,
                               uint table_oid,
                               const std::string &column_name,
                               bool is_not_null);

  bool UpdateDefaultConstraint(TransactionContext *txn,
                               uint table_oid,
                               const std::string &column_name,
                               bool has_default,
                               const Value *default_value);

 private:
  //===--------------------------------------------------------------------===//
  // Read Related API(only called within table catalog object)
  //===--------------------------------------------------------------------===//
  const std::unordered_map<uint, std::shared_ptr<ColumnCatalogEntry>>
    GetColumnCatalogEntries(TransactionContext *txn,
                            uint table_oid);

  std::unique_ptr<Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    COLUMN_NAME = 1,
    COLUMN_ID = 2,
    COLUMN_OFFSET = 3,
    COLUMN_TYPE = 4,
    COLUMN_LENGTH = 5,
    IS_INLINED = 6,
    IS_NOT_NULL = 7,
    HAS_DEFAULT = 8,
    DEFAULT_VALUE_SRC = 9,
    DEFAULT_VALUE_BIN = 10,
    // Add new columns here in creation order
  };
  std::vector<uint> all_column_ids_ = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_COLUMN_ID = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

