//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// storage_manager.h
//
// Identification: src/include/storage/storage_manager.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef STORAGE_MANAGER
//#define STORAGE_MANAGER
#include <vector>
#include <atomic>
#include "tile_group.h"


#include "storage/peloton/common/container/cuckoo_map.h"
#include "storage/peloton/store/database.h"
//#include "storage/peloton/type/abstract_pool.h"
#include "storage/peloton/type/intel_pool.h"
#include "storage/peloton/type/ephemeral_pool.h"
//namespace storage {
class TileGroup;
//  class IndirectionArray;
//}

//namespace index {
//class Index;
//}


class Database;
class DataTable;

class StorageManager {
 public:
  // Global Singleton
  static StorageManager *GetInstance(void);
//  Global_Pools gPools;

  static int *InitMemPool();
//  AbstractPool*  GetPoolByThdId(uint curr_thd_id);
  static AbstractPool*  GetPoolByThdId(uint curr_thd_id);
  // Deconstruct the catalog database when destroying the catalog.
  ~StorageManager();

  //===--------------------------------------------------------------------===//
  // DEPRECATED FUNCTIONs
  //===--------------------------------------------------------------------===//
  /*
  * We're working right now to remove metadata from storage level and eliminate
  * multiple copies, so those functions below will be DEPRECATED soon.
  */

  // Find a database using vector offset
//  Database *GetDatabaseWithOffset(uint database_offset) const;

  //===--------------------------------------------------------------------===//
  // GET WITH OID - DIRECTLY GET FROM STORAGE LAYER
  //===--------------------------------------------------------------------===//

  /* Find a database using its oid from storage layer,
   * throw exception if not exists
   * */
Database *GetDatabaseWithName(std::string db_name) const;
 Database *GetDatabaseWithOid(uint db_oid) const;

  /* Find a table using its oid from storage layer,
   * throw exception if not exists
   * */
  DataTable *GetTableWithOid(uint database_oid,
                             uint table_oid) const;

  /* Find a index using its oid from storage layer,
   * throw exception if not exists
   * */
//  index::Index *GetIndexWithOid(uint database_oid, uint table_oid,
//                                uint index_oid) const;

  //===--------------------------------------------------------------------===//
  // HELPERS
  //===--------------------------------------------------------------------===//
  // Returns true if the catalog contains the given database with the id
  bool HasDatabase(uint db_oid) const;
//  uint GetDatabaseCount() { return databases_.size(); }
  uint GetDatabaseCount() { return 0; }

  //===--------------------------------------------------------------------===//
  // FUNCTIONS USED BY CATALOG
  //===--------------------------------------------------------------------===//

  void AddDatabaseToStorageManager(Database *db) {
   databases_.push_back(db);
 }

  bool RemoveDatabaseFromStorageManager(uint database_oid);

  void DestroyDatabases();


  //===--------------------------------------------------------------------===//
  // TILE GROUP ALLOCATION
  //===--------------------------------------------------------------------===//

  uint GetNextTileId() { return ++tile_oid_; }

  uint GetNextTileGroupId() { return ++tile_group_oid_; }

  uint GetCurrentTileGroupId() { return tile_group_oid_; }

  void SetNextTileGroupId(uint next_oid) { tile_group_oid_ = next_oid; }

  void AddTileGroup(const uint oid,
                    std::shared_ptr<TileGroup> location);

  void DropTileGroup(const uint oid);

  std::shared_ptr<TileGroup> GetTileGroup(const uint oid);

  void ClearTileGroup(void);

  // used after recovery
  void ResetInfo() {
    tile_group_oid_ = ATOMIC_VAR_INIT(START_OID);
    tile_group_locator_.Clear();
//    tile_oid_ = ATOMIC_VAR_INIT(START_OID);
  }
 private:
  StorageManager();

  // A vector of the database pointers in the catalog
  std::vector<Database *> databases_;

  //===--------------------------------------------------------------------===//
  // Data member for tile allocation
  //===--------------------------------------------------------------------===//

  std::atomic<uint> tile_oid_ = ATOMIC_VAR_INIT(START_OID);

  //===--------------------------------------------------------------------===//
  // Data members for tile group allocation
  //===--------------------------------------------------------------------===//
  std::atomic<uint> tile_group_oid_ = ATOMIC_VAR_INIT(START_OID);

  CuckooMap<uint, std::shared_ptr<TileGroup>> tile_group_locator_;
  static std::shared_ptr<TileGroup> empty_tile_group_;
//  std::vector<AbstractPool*> gPools_;
  static std::vector<AbstractPool *> gPools_;
  //  typedef tbb::concurrent_unordered_map<uint, AbstractPool*>
//      Global_Pools;


};

//#endif