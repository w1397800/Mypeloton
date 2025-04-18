//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "database_catalog.h"

#include <memory>

#include "catalog.h"
#include "system_catalogs.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/store/logical_tile.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/type/value_factory.h"



DatabaseCatalogEntry::DatabaseCatalogEntry(TransactionContext *txn,
                                           LogicalTile *tile)
    : database_oid_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_OID)
                       .GetAs<uint>()),
      database_name_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_NAME)
                        .ToString()),
      table_catalog_entries_cache_(),
      table_catalog_entries_cache_by_name(),
      valid_table_catalog_entries(false),
      txn_(txn) {}

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogEntry::InsertTableCatalogEntry(
    std::shared_ptr<TableCatalogEntry> table_catalog_entry) {
  if (!table_catalog_entry || table_catalog_entry->GetTableOid() == INVALID_OID) {
    return false;  // invalid object
  }

  // check if already in cache
  if (table_catalog_entries_cache_.find(table_catalog_entry->GetTableOid()) !=
      table_catalog_entries_cache_.end()) {
    //LOG_DEBUG("Table %u already exists in cache!", table_catalog_entry->GetTableOid());
    return false;
  }

  std::string key =
      table_catalog_entry->GetSchemaName() + "." + table_catalog_entry->GetTableName();
  if (table_catalog_entries_cache_by_name.find(key) != table_catalog_entries_cache_by_name.end()) {
    //LOG_DEBUG("Table %s already exists in cache!",
    //          table_catalog_entry->GetTableName().c_str());
    return false;
  }

  table_catalog_entries_cache_.insert(
      std::make_pair(table_catalog_entry->GetTableOid(), table_catalog_entry));
  table_catalog_entries_cache_by_name.insert(std::make_pair(key, table_catalog_entry));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableCatalogEntry(uint table_oid) {
  // find table name from table name cache
  auto it = table_catalog_entries_cache_.find(table_oid);
  if (it == table_catalog_entries_cache_.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_catalog_entries_cache_.erase(it);
  // erase from table name cache
  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  table_catalog_entries_cache_by_name.erase(key);
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableCatalogEntry(const std::string &table_name,
                                                  const std::string &schema_name) {
  std::string key = schema_name + "." + table_name;
  // find table name from table name cache
  auto it = table_catalog_entries_cache_by_name.find(key);
  if (it == table_catalog_entries_cache_by_name.end()) {
    return false;  // table name not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_catalog_entries_cache_by_name.erase(it);
  table_catalog_entries_cache_.erase(table_object->GetTableOid());
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
 */
void DatabaseCatalogEntry::EvictAllTableCatalogEntries() {
  table_catalog_entries_cache_.clear();
  table_catalog_entries_cache_by_name.clear();
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableCatalogEntry(
    uint table_oid, bool cached_only) {
  auto it = table_catalog_entries_cache_.find(table_oid);
  if (it != table_catalog_entries_cache_.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableCatalogEntry(txn_, table_oid);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_name     table name of the requested table catalog object
 * @param   schema_name    schema name of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableCatalogEntry(
    const std::string &table_name, const std::string &schema_name,
    bool cached_only) {
  std::string key = schema_name + "." + table_name;
  auto it = table_catalog_entries_cache_by_name.find(key);
  if (it != table_catalog_entries_cache_by_name.end()) {
    return it->second;
  }

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableCatalogEntry(txn_, schema_name, table_name);
  }
}

/*@brief    Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   schema_name
 * @return  table catalog objects
 */
std::vector<std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableCatalogEntries(const std::string &schema_name) {
  // read directly from pg_table
  if (!valid_table_catalog_entries) {
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    // insert every table object into cache
    pg_table->GetTableCatalogEntries(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_catalog_entries);
  std::vector<std::shared_ptr<TableCatalogEntry>> result;
  for (auto it : table_catalog_entries_cache_) {
    if (it.second->GetSchemaName() == schema_name) {
      result.push_back(it.second);
    }
  }

  return result;
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::unordered_map<uint, std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableCatalogEntries(bool cached_only) {
  if (!cached_only && !valid_table_catalog_entries) {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    return pg_table->GetTableCatalogEntries(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_catalog_entries);
  return table_catalog_entries_cache_;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_oid
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexCatalogEntry(
    uint index_oid) {
  for (auto it = table_catalog_entries_cache_.begin(); it != table_catalog_entries_cache_.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexCatalogEntries(index_oid, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_name
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexCatalogEntry(
    const std::string &index_name, const std::string &schema_name) {
  for (auto it = table_catalog_entries_cache_.begin(); it != table_catalog_entries_cache_.end();
       ++it) {
    auto table_object = it->second;
    if (table_object != nullptr &&
        table_object->GetSchemaName() == schema_name) {
      auto index_object = table_object->GetIndexCatalogEntry(index_name, true);
      if (index_object) return index_object;
    }
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(TransactionContext *txn,
                                             Database *pg_catalog,
                                             AbstractPool *pool) {
  static DatabaseCatalog
      database_catalog{txn, pg_catalog, pool};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(TransactionContext *,
                                 Database *pg_catalog,
                                 AbstractPool *)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      DATABASE_CATALOG_OID,
                      DATABASE_CATALOG_NAME) {
  // Add indexes for pg_database
  AddIndex(DATABASE_CATALOG_NAME "_pkey",
           DATABASE_CATALOG_PKEY_OID,
           {ColumnId::DATABASE_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(DATABASE_CATALOG_NAME "_skey0",
           DATABASE_CATALOG_SKEY0_OID,
           {ColumnId::DATABASE_NAME},
           IndexConstraintType::UNIQUE);
}

DatabaseCatalog::~DatabaseCatalog() {}

/*@brief   private function for initialize schema of pg_database
 * @return  unqiue pointer to schema
 */
std::unique_ptr<Schema> DatabaseCatalog::InitializeSchema() {
  auto database_id_column = Column(
      TypeId::INTEGER, Type::GetTypeSize(TypeId::INTEGER),
      "database_oid", true);
  database_id_column.SetNotNull();

  auto database_name_column = Column(
      TypeId::VARCHAR, max_name_size_, "database_name", false);
  database_name_column.SetNotNull();

  std::unique_ptr<Schema> database_catalog_schema(
      new Schema({database_id_column, database_name_column}));

  database_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      DATABASE_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      DATABASE_CATALOG_OID, std::vector<uint>{ColumnId::DATABASE_OID},
      DATABASE_CATALOG_PKEY_OID));

  database_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      DATABASE_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      DATABASE_CATALOG_OID, std::vector<uint>{ColumnId::DATABASE_NAME},
      DATABASE_CATALOG_SKEY0_OID));

  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(TransactionContext *txn,
                                     uint database_oid,
                                     const std::string &database_name,
                                     AbstractPool *pool) {
  std::unique_ptr<Tuple> tuple(
      new Tuple(catalog_table_->GetSchema(), true));

  auto val0 = ValueFactory::GetIntegerValue(database_oid);
  auto val1 = ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_NAME, val1, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool DatabaseCatalog::DeleteDatabase(TransactionContext *txn, uint database_oid) {
  uint index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(database_oid).Copy());

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithIndexScan(txn, index_offset, values);
}

std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseCatalogEntry(
    TransactionContext *txn,
    uint database_oid) {
  if (txn == nullptr) {
    // throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<uint> column_ids(all_column_ids_);
  uint index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get());
    // insert into cache
    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return database_object;
  } else {
    //LOG_DEBUG("Found %lu database tiles with oid %u", result_tiles->size(),
    //          database_oid);
  }

  // return empty object if not found
  return nullptr;
}

/* @brief   First try get cached object from transaction cache. If cache miss,
 *          construct database object from pg_database, and insert into the
 *          cache.
 */
std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseCatalogEntry(
    TransactionContext *txn,
    const std::string &database_name) {
  if (txn == nullptr) {
    // throw CatalogException("Transaction is invalid!");
  }
  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object) return database_object;

  // cache miss, get from pg_database
  std::vector<uint> column_ids(all_column_ids_);
  uint index_offset = IndexId::SKEY_DATABASE_NAME;  // Index of database_name
  std::vector<Value> values;
  values.push_back(
      ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object =
        std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get());
    if (database_object) {
      // insert into cache
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      PELOTON_ASSERT(success == true);
      (void)success;
    }
    return database_object;
  }

  // return empty object if not found
  return nullptr;
}
