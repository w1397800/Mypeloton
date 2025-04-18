//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_catalog.h
//
// Identification: src/include/catalog/table_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_table
//
// Schema: (column position: column_name)
// 0: table_oid (pkey)
// 1: table_name,
// 2: schema_name (the namespace name that this table belongs to)
// 3: database_oid
// 4: version_id: for fast ddl(alter table)
//
// Indexes: (index offset: indexed columns)
// 0: table_oid (unique & primary key)
// 1: table_name & schema_name(unique)
// 2: database_oid (non-unique)
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <unordered_map>

#include "abstract_catalog.h"
//#include "storage/peloton/catalog/abstract_catalog.h"
#include "storage/peloton/store/logical_tile.h"




class Layout;



class IndexCatalogEntry;
class ColumnCatalogEntry;
class ConstraintCatalogEntry;

class TableCatalogEntry {
  friend class TableCatalog;
  friend class IndexCatalog;
  friend class ColumnCatalog;
  friend class LayoutCatalog;
  friend class ConstraintCatalog;

 public:
  TableCatalogEntry(TransactionContext *txn,
                    LogicalTile *tile,
                    int tupleId = 0);

 public:
  // Get indexes
  void EvictAllIndexCatalogEntries();

  std::unordered_map<uint, std::shared_ptr<IndexCatalogEntry>>
  GetIndexCatalogEntries(bool cached_only = false);


  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntries(uint index_oid,
                                                            bool cached_only = false);

  std::shared_ptr<IndexCatalogEntry> GetIndexCatalogEntry(
      const std::string &index_name, bool cached_only = false);

  // Get columns
  void EvictAllColumnCatalogEntries();

  std::unordered_map<uint, std::shared_ptr<ColumnCatalogEntry>>
  GetColumnCatalogEntries(bool cached_only = false);

  std::unordered_map<std::string, std::shared_ptr<ColumnCatalogEntry>>
  GetColumnCatalogEntriesByName(bool cached_only = false);

  std::shared_ptr<ColumnCatalogEntry> GetColumnCatalogEntry(
      uint column_id, bool cached_only = false);

  std::shared_ptr<ColumnCatalogEntry> GetColumnCatalogEntry(
      const std::string &column_name, bool cached_only = false);

  // Evict all layouts from the cache
  void EvictAllLayouts();

  // Get layouts
  std::unordered_map<uint, std::shared_ptr<const Layout>> GetLayouts(
      bool cached_only = false);
  std::shared_ptr<const Layout> GetLayout(uint layout_id,
                                                   bool cached_entry = false);

  // Evict all constraints from the cache
  void EvictAllConstraintCatalogEntries();

  // Get constraints
  std::unordered_map<uint, std::shared_ptr<ConstraintCatalogEntry>>
	GetConstraintCatalogEntries(bool cached_only = false);
  std::shared_ptr<ConstraintCatalogEntry>
  GetConstraintCatalogEntry(uint constraint_oid, bool cached_entry = false);

  inline uint GetTableOid() { return table_oid; }
  inline const std::string &GetTableName() { return table_name; }
  inline const std::string &GetSchemaName() { return schema_name; }
  inline uint GetDatabaseOid() { return database_oid; }
  inline uint32_t GetVersionId() { return version_id; }
  inline uint GetDefaultLayoutOid() { return default_layout_oid; }

 private:
  // member variables
  uint table_oid;
  std::string table_name;
  std::string schema_name;
  uint database_oid;
  uint32_t version_id;
  uint default_layout_oid;

  // Insert/Evict index catalog entries
  bool InsertIndexCatalogEntry(std::shared_ptr<IndexCatalogEntry> index_catalog_entry);
  bool EvictIndexCatalogEntry(uint index_oid);
  bool EvictIndexCatalogEntry(const std::string &index_name);

  // Insert/Evict column catalog entries
  bool InsertColumnCatalogEntry(std::shared_ptr<ColumnCatalogEntry> column_catalog_entry);
  bool EvictColumnCatalogEntry(uint column_id);
  bool EvictColumnCatalogEntry(const std::string &column_name);

  // Insert layout catalog entry into table catalog entry
  bool InsertLayout(std::shared_ptr<const Layout> layout);
  // Evict layout_id from the table catalog entry
  bool EvictLayout(uint layout_id);

  // Insert constraint catalog entry into table catalog entry
  bool InsertConstraintCatalogEntry(
  		std::shared_ptr<ConstraintCatalogEntry> constraint_catalog_entry);
  // Evict constraint_oid from the table catalog entry
  bool EvictConstraintCatalogEntry(uint constraint_oid);
  void SetValidConstraintCatalogEntries(bool valid = true) {
    valid_constraint_catalog_entries_ = valid;
  }

  // cache for *all* index catalog entries in this table
  std::unordered_map<uint, std::shared_ptr<IndexCatalogEntry>> index_catalog_entries;
  std::unordered_map<std::string, std::shared_ptr<IndexCatalogEntry>>
      index_catalog_entries_by_name_;
  bool valid_index_catalog_entries_;

  // cache for *all* column catalog entries in this table
  std::unordered_map<uint, std::shared_ptr<ColumnCatalogEntry>>
      column_catalog_entries_;
  std::unordered_map<std::string, std::shared_ptr<ColumnCatalogEntry>>
      column_names_;
  bool valid_column_catalog_entries_;

  // cache for *all* layout catalog entries in the table
  std::unordered_map<uint, std::shared_ptr<const Layout>>
      layout_catalog_entries_;
  bool valid_layout_catalog_entries_;

  // cache for *all* constraint catalog entries in the table
  std::unordered_map<uint, std::shared_ptr<ConstraintCatalogEntry>>
      constraint_catalog_entries_;
  bool valid_constraint_catalog_entries_;

  // Pointer to its corresponding transaction
  TransactionContext *txn_;
};

class TableCatalog : public AbstractCatalog {
  friend class TableCatalogEntry;
  friend class DatabaseCatalogEntry;
  friend class ColumnCatalog;
  friend class IndexCatalog;
  friend class LayoutCatalog;
  friend class ConstraintCatalog;
  friend class Catalog;

 public:
  TableCatalog(TransactionContext *txn,
               Database *pg_catalog,
               AbstractPool *pool);

  ~TableCatalog();

  inline uint GetNextOid() { return oid_++ | TABLE_OID_MASK; }

  void UpdateOid(uint add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTable(TransactionContext *txn,
                   uint database_oid,
                   const std::string &schema_name,
                   uint table_oid,
                   const std::string &table_name,
                   uint layout_oid,
                   AbstractPool *pool);

  bool DeleteTable(TransactionContext *txn, uint table_oid);

  bool UpdateVersionId(TransactionContext *txn,
                       uint table_oid,
                       uint update_val);

  bool UpdateDefaultLayoutOid(TransactionContext *txn,
                              uint table_oid,
                              uint update_val);

  //===--------------------------------------------------------------------===//
  // Read Related API
  //===--------------------------------------------------------------------===//
 private:
  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(TransactionContext *txn,
                                                          uint table_oid);

  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(TransactionContext *txn,
                                                          const std::string &schema_name,
                                                          const std::string &table_name);

  std::unordered_map<uint, std::shared_ptr<TableCatalogEntry>>
  GetTableCatalogEntries(TransactionContext *txn);

  std::unique_ptr<Schema> InitializeSchema();

  enum ColumnId {
    TABLE_OID = 0,
    TABLE_NAME = 1,
    SCHEMA_NAME = 2,
    DATABASE_OID = 3,
    VERSION_ID = 4,
    DEFAULT_LAYOUT_OID = 5,
    // Add new columns here in creation order
  };
  std::vector<uint> all_column_ids_ = {0, 1, 2, 3, 4, 5};

  enum IndexId {
    PRIMARY_KEY = 0,
    SKEY_TABLE_NAME = 1,
    SKEY_DATABASE_OID = 2,
    // Add new indexes here in creation order
  };
};


