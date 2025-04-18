//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.h
//
// Identification: src/include/catalog/manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef MANAGER
//#define MANAGER
#include <atomic>
#include <utility>
#include <mutex>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>

#include "storage/peloton/common/macros.h"
//#include "tbb/concurrent_unordered_map.h"
#include "storage/peloton/store/tile_group.h"
#include "storage/peloton/common/indirection_array.h"
//class TileGroup;
//class IndirectionArray;


//===--------------------------------------------------------------------===//
// Manager
//===--------------------------------------------------------------------===//

class Manager {
 public:
  Manager() {}

  // Singleton
  static Manager &GetInstance();

  //===--------------------------------------------------------------------===//
  // INDIRECTION ARRAY ALLOCATION
  //===--------------------------------------------------------------------===//

  uint GetNextIndirectionArrayId() { return ++indirection_array_oid_; }

  uint GetCurrentIndirectionArrayId() { return indirection_array_oid_; }

  void AddIndirectionArray(const uint oid,
                           std::shared_ptr<IndirectionArray> location);

  void DropIndirectionArray(const uint oid);

  void ClearIndirectionArray();

  DISALLOW_COPY(Manager);

 private:

  //===--------------------------------------------------------------------===//
  // Data members for indirection array allocation
  //===--------------------------------------------------------------------===//
  std::atomic<uint> indirection_array_oid_ = ATOMIC_VAR_INIT(START_OID);

  tbb::concurrent_unordered_map<uint, std::shared_ptr<IndirectionArray>>
      indirection_array_locator_;
//  std::map<uint, std::shared_ptr<IndirectionArray>>
//      indirection_array_locator_;
  static std::shared_ptr<IndirectionArray> empty_indirection_array_;
};

//#endif