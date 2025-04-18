//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_catalog.h
//
// Identification: src/include/catalog/index_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Index Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_index
//
// Schema: (column: column_name)
// 0: index_oid (pkey)
// 1: index_name
// 2: table_oid (which table this index belongs to)
// 3: schema_name (which namespace this index belongs to)
// 4: index_type (default value is BWTREE)
// 5: index_constraint
// 6: unique_keys (is this index supports duplicate keys)
// 7: indexed_attributes (indicate which table columns this index indexes. For
// example a value of 0 2 would mean that the first and the third table columns
// make up the index.)
//
// Indexes: (index offset: indexed columns)
// 0: index_oid (unique & primary key)
// 1: index_name & schema_name (unique)
// 2: table_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_catalog.h"
#include "storage/peloton/store/logical_tile.h"


class IndexCatalogEntry {
  friend class TableCatalogEntry;

 public:
  IndexCatalogEntry(LogicalTile *tile, int tupleId = 0);

  inline uint GetIndexOid() { return index_oid_; }
  inline const std::string &GetIndexName() { return index_name_; }
  inline uint GetTableOid() { return table_oid_; }
  inline const std::string &GetSchemaName() { return schema_name_; }
  inline IndexType GetIndexType() { return index_type_; }
  inline IndexConstraintType GetIndexConstraint() { return index_constraint_; }
  inline bool HasUniqueKeys() { return unique_keys_; }
  inline const std::vector<uint> &GetKeyAttrs() { return key_attrs_; }

 private:
  // member variables
  uint index_oid_;
  std::string index_name_;
  uint table_oid_;
  std::string schema_name_;
  IndexType index_type_;
  IndexConstraintType index_constraint_;
  bool unique_keys_;
  std::vector<uint> key_attrs_;
};

class IndexCatalog : public AbstractCatalog {
  friend class IndexCatalogEntry;
  friend class TableCatalogEntry;
  friend class Catalog;

 public:
  IndexCatalog(TransactionContext *txn,
               Database *pg_catalog,
               AbstractPool *pool);

  ~IndexCatalog();

  inline uint GetNextOid() { return oid_++ | INDEX_OID_MASK; }

  void UpdateOid(uint add_value) { oid_ += add_value; }

  /** Write Related API */
  bool InsertIndex(TransactionContext *txn,
                   const std::string &schema_name,
                   uint table_oid,
                   uint index_oid,
                   const std::string &index_name,
                   IndexType index_type,
                   IndexConstraintType index_constraint,
                   bool unique_keys,
                   std::vector<uint> index_keys,
                   AbstractPool *pool);

  bool DeleteIndex(TransactionContext *txn,
                   uint database_oid,
                   uint index_oid);

  /** Read Related API */
  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntry(TransactionContext *txn,
                                                          const std::string &database_name,
                                                          const std::string &schema_name,
                                                          const std::string &index_name);

 private:
  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntry(TransactionContext *txn,
                                                          uint database_oid,
                                                          uint index_oid);

  const std::unordered_map<uint, std::shared_ptr<IndexCatalogEntry>>
  GetIndexCatalogEntries(
      TransactionContext *txn,
      uint table_oid);

  std::unique_ptr<Schema> InitializeSchema();

  enum ColumnId {
    INDEX_OID = 0,
    INDEX_NAME = 1,
    TABLE_OID = 2,
    SCHEMA_NAME = 3,
    INDEX_TYPE = 4,
    INDEX_CONSTRAINT = 5,
    UNIQUE_KEYS = 6,
    INDEXED_ATTRIBUTES = 7,
    // Add new columns here in creation order
  };
  std::vector<uint> all_column_ids = {0, 1, 2, 3, 4, 5, 6, 7};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_INDEX_NAME = 1,
    SKEY_TABLE_OID = 2,
    // Add new indexes here in creation order
  };
};

