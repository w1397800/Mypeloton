//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// manager.cpp
//
// Identification: src/catalog/manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//#include "common/exception.h"
//#include "common/logger.h"
#include "storage/peloton/catalog/manager.h"
//#include "storage/database.h"
#include "storage/peloton/store/data_table.h"
//#include "concurrency/transaction_manager_factory.h"


std::shared_ptr<IndirectionArray> Manager::empty_indirection_array_;

Manager &Manager::GetInstance() {
  static Manager manager;
  return manager;
}

//===--------------------------------------------------------------------===//
// OBJECT MAP
//===--------------------------------------------------------------------===//

void Manager::AddIndirectionArray(
    const uint oid, std::shared_ptr<IndirectionArray> location) {
  // add/update the catalog reference to the indirection array
  auto ret = indirection_array_locator_[oid] = location;
}

void Manager::DropIndirectionArray(const uint oid) {
  // drop the catalog reference to the tile group
  indirection_array_locator_[oid] = empty_indirection_array_;
}

// used for logging test
void Manager::ClearIndirectionArray() { indirection_array_locator_.clear(); }

