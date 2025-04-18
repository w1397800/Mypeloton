//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// schema_catalog.h
//
// Identification: src/include/catalog/schema_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_namespace
//
// Schema: (column offset: column_name)
// 0: schema_oid (pkey)
// 1: schema_name
//
// Indexes: (index offset: indexed columns)
// 0: schema_oid (unique & primary key)
// 1: schema_name (unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_catalog.h"
#include "storage/peloton/store/logical_tile.h"



class SchemaCatalogEntry {
  friend class DatabaseCatalogEntry;

 public:
  SchemaCatalogEntry(TransactionContext *txn,
                     LogicalTile *tile);

  inline uint GetSchemaOid() { return schema_oid_; }

  inline const std::string &GetSchemaName() { return schema_name_; }

 private:
  // member variables
  uint schema_oid_;
  std::string schema_name_;
  // Pointer to its corresponding transaction
  // This object is only visible during this transaction
  TransactionContext *txn_;
};

class SchemaCatalog : public AbstractCatalog {
  friend class SchemaCatalogEntry;
  friend class Catalog;

 public:
  SchemaCatalog(TransactionContext *txn,
                Database *peloton,
               AbstractPool *pool);

  ~SchemaCatalog();

  inline uint GetNextOid() { return oid_++ | SCHEMA_OID_MASK; }

  void UpdateOid(uint add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertSchema(TransactionContext *txn,
                    uint schema_oid,
                    const std::string &schema_name,
                   AbstractPool *pool);

  bool DeleteSchema(TransactionContext *txn,
                    const std::string &schema_name);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
  std::shared_ptr<SchemaCatalogEntry> GetSchemaCatalogEntry(TransactionContext *txn,
                                                            const std::string &schema_name);

 private:
  std::unique_ptr<Schema> InitializeSchema();

  enum ColumnId {
    SCHEMA_OID = 0,
    SCHEMA_NAME = 1,
    // Add new columns here in creation order
  };
  std::vector<uint> all_column_ids_ = {0, 1};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_SCHEMA_NAME = 1,
    // Add new indexes here in creation order
  };
};

