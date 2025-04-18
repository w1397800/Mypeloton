//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.cpp
//
// Identification: src/storage/storage_manager.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage_manager.h"

#include "storage/peloton/store/database.h"
#include "data_table.h"
#include "tile_group.h"


std::shared_ptr<TileGroup> StorageManager::empty_tile_group_;
std::vector<AbstractPool *> StorageManager::gPools_;
//StorageManager::StorageManager() = default;
StorageManager::StorageManager() {
//  for (size_t i = 0; i < MEM_POOL_COUNT; ++i) {
//    IntelPool *pool = new IntelPool();
//    gPools_.push_back( pool);
//  }
}


StorageManager::~StorageManager() = default;

// Get instance of the global catalog storage manager
StorageManager *StorageManager::GetInstance() {
  static StorageManager global_catalog_storage_manager;
  return &global_catalog_storage_manager;
}

int *StorageManager::InitMemPool() {
  std::vector<std::shared_ptr<AbstractPool>> gPools;

  for (size_t i = 0; i < MEM_POOL_COUNT; ++i) {
//    IntelPool *pool = new IntelPool();
    EphemeralPool *pool = new EphemeralPool();

    gPools_.push_back( pool);
  }
  return 0;
}

AbstractPool* StorageManager::GetPoolByThdId(uint curr_thd_id){
  return gPools_[curr_thd_id%MEM_POOL_COUNT];
//  (void )curr_thd_id;
//  return gPools_[0];
}

//===--------------------------------------------------------------------===//
// GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
//===--------------------------------------------------------------------===//

/* Find a database using its oid from storage layer,
 * throw exception if not exists
 * */
Database *StorageManager::GetDatabaseWithOid(
   uint database_oid) const {
 for (auto database : databases_)
   if (database->GetOid() == database_oid) return database;
//  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
//                         " is not found");
 return nullptr;
}

Database *StorageManager::GetDatabaseWithName(
   std::string db_name) const {
 for (auto database : databases_)
   if (database->GetDBName() == db_name) return database;
//  throw CatalogException("Database with oid = " + std::to_string(database_oid) +
//                         " is not found");
 return nullptr;
}

/* Find a table using its oid from storage layer,
 * throw exception if not exists
 * */
DataTable *StorageManager::GetTableWithOid(
    uint database_oid, uint table_oid) const {
//  LOG_TRACE("Getting table with oid %d from database with oid %d", database_oid,
//            table_oid);
  //Lookup DB from storage layer
 auto database =
     GetDatabaseWithOid(database_oid);  // Throw exception if not exists
  //Lookup table from storage layer
 return database->GetTableWithOid(table_oid);  // Throw exception if not exists
}

/* Find a index using its oid from storage layer,
 * throw exception if not exists
 * */
//index::Index *StorageManager::GetIndexWithOid(uint database_oid,
//                                              uint table_oid,
//                                              uint index_oid) const {
//  // Lookup table from storage layer
//  auto table = GetTableWithOid(database_oid,
//                               table_oid);  // Throw exception if not exists
//  // Lookup index from storage layer
//  return table->GetIndexWithOid(index_oid)
//      .get();  // Throw exception if not exists
//}

//===--------------------------------------------------------------------===//
// DEPRECATED
//===--------------------------------------------------------------------===//

// This is used as an iterator
//Database *StorageManager::GetDatabaseWithOffset(
//    uint database_offset) const {
//  PELOTON_ASSERT(database_offset < databases_.size());
//  auto database = databases_.at(database_offset);
//  return database;
//}

//===--------------------------------------------------------------------===//
// HELPERS
//===--------------------------------------------------------------------===//

// Only used for testing
bool StorageManager::HasDatabase(uint db_oid) const {
 for (auto database : databases_)
   if (database->GetOid() == db_oid) return (true);
  return (false);
}

//Invoked when catalog is destroyed
void StorageManager::DestroyDatabases() {
//  LOG_TRACE("Deleting databases");
 for (auto database : databases_) delete database;
//  LOG_TRACE("Finish deleting database");
}

bool StorageManager::RemoveDatabaseFromStorageManager(uint database_oid) {
 for (auto it = databases_.begin(); it != databases_.end(); ++it) {
   if ((*it)->GetOid() == database_oid) {
     delete (*it);
     databases_.erase(it);
     return true;
   }
 }
  return false;
}


//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void StorageManager::AddTileGroup(const uint oid,
                                  std::shared_ptr<TileGroup> location) {
  // add/update the catalog reference to the tile group
  tile_group_locator_.Upsert(oid, location);
}

void StorageManager::DropTileGroup(const uint oid) {
  // drop the catalog reference to the tile group
  tile_group_locator_.Erase(oid);
}

std::shared_ptr<TileGroup> StorageManager::GetTileGroup(const uint oid) {
  std::shared_ptr<TileGroup> location;
  if (tile_group_locator_.Find(oid, location)) {
    return location;
  }
  return empty_tile_group_;
}

// used for logging test
void StorageManager::ClearTileGroup() { tile_group_locator_.Clear(); }

