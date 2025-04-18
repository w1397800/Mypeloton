//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/storage/data_table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <mutex>
#include <utility>
#include "sql/log.h"
//#include "storage/peloton/catalog/layout_catalog.h"
//#include "storage/peloton/catalog/system_catalogs.h"
//#include "storage/peloton/catalog/table_catalog.h"


//#include "storage/peloton/catalog/catalog.h"

#include "storage/peloton/common/container_tuple.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/exception.h"
//#include "storage/peloton/common/platform.h"

#include "storage/peloton/concurrency/transaction_context.h"

#include "storage/peloton/concurrency/epoch_manager_factory.h"
#include "storage/peloton/concurrency/decentralized_epoch_manager.h"
//#include "storage/peloton/common/logger.h"
//#include "storage/peloton/common/thread_pool.h"

#include "storage/peloton/concurrency/transaction_manager_factory.h"

//#include "executor/executor_context.h"
#include "storage/peloton/gc/gc_manager_factory.h"

#include "storage/peloton/index/index.h"
//#include "log_manager.h"
#include "abstract_table.h"
#include "data_table.h"
//#include "database.h"
#include "storage_manager.h"
#include "tile.h"
//#include "tile_group.h"
#include "tile_group_factory.h"
//#include "tile_group_header.h"
#include "tuple.h"
//#include "transaction_context.h"
//#include "tuning/clusterer.h"
//#include "tuning/sample.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<uint> sdbench_column_ids;

double peloton_projectivity;

int peloton_num_groups;


uint DataTable::invalid_tile_group_id = -1;

size_t DataTable::default_active_tilegroup_count_ = ACTIVE_COUNT;
size_t DataTable::default_active_indirection_array_count_ = ACTIVE_COUNT;

DataTable::DataTable(Schema *schema, const std::string &table_name,
                     const uint &database_oid, const uint &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table, const bool is_catalog,
                     const LayoutType layout_type)
    : AbstractTable(table_oid, schema, own_schema, layout_type),
      database_oid(database_oid),
      table_name(table_name),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      current_layout_oid_(ATOMIC_VAR_INIT(COLUMN_STORE_LAYOUT_OID)),
      adapt_table_(adapt_table),
      trigger_list_(0) {//下午从这里开始
  if (is_catalog == true) {
    active_tilegroup_count_ = ACTIVE_COUNT;
    active_indirection_array_count_ = ACTIVE_COUNT;
  } else {
    active_tilegroup_count_ = ACTIVE_COUNT;
    active_indirection_array_count_ = ACTIVE_COUNT;
  }

  active_tile_groups_.resize(active_tilegroup_count_);

  active_indirection_arrays_.resize(active_indirection_array_count_);
  // Create tile groups.
  for (size_t i = 0; i < active_tilegroup_count_; ++i) {
    AddDefaultTileGroup(i);
  }

  // Create indirection layers.
  for (size_t i = 0; i < active_indirection_array_count_; ++i) {
    AddDefaultIndirectionArray(i);
  }
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
//  auto &catalog_manager = Manager::GetInstance();
//  auto storage_manager = StorageManager::GetInstance();
//  auto tile_groups_size = tile_groups_.GetSize();
//  std::size_t tile_groups_itr;
//
//  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
//       tile_groups_itr++) {
//    auto tile_group_id = tile_groups_.Find(tile_groups_itr);
//
//    if (tile_group_id != invalid_tile_group_id) {
//      //LOG_TRACE("Dropping tile group : %u ", tile_group_id);
//      // drop tile group in catalog
//      storage_manager->DropTileGroup(tile_group_id);
//    }
//  }
//
//  // drop all indirection arrays
//  for (auto indirection_array : active_indirection_arrays_) {
//    auto oid = indirection_array->GetOid();
//    catalog_manager.DropIndirectionArray(oid);
//  }
  // AbstractTable cleans up the schema
}
//
////===-------------------------------------------------------------------===//
//// TUPLE HELPER OPERATIONS
////===--------------------------------------------------------------------===//
//
bool DataTable::CheckNotNulls(const AbstractTuple *tuple,
                              uint column_idx) const {
//  if (tuple->GetValue(column_idx).IsNull()) {
//    //LOG_TRACE(
//        "%u th attribute in the tuple was NULL. It is non-nullable "
//        "attribute.",
//        column_idx);
//    return false;
//  }
  (void)tuple;
  (void)column_idx;
  return true;
}
//
bool DataTable::CheckConstraints(const AbstractTuple *tuple) const {
  // make sure that the given tuple does not violate constraints.

  // NOT NULL constraint
//  for (uint column_id : schema->GetNotNullColumns()) {
//    if (schema->AllowNull(column_id) == false &&
//        CheckNotNulls(tuple, column_id) == false) {
//      std::string error =
//          StringUtil::Format("NOT NULL constraint violated on column '%s' : %s",
//                             schema->GetColumn(column_id).GetName().c_str(),
//                             tuple->GetInfo().c_str());
//      throw ConstraintException(error);
//    }
//  }
//
//  // DEFAULT constraint should not be handled here
//  // Handled in higher hierarchy
//
//  // multi-column constraints
//  for (auto cons_pair : schema->GetConstraints()) {
//    auto cons = cons_pair.second;
//    ConstraintType type = cons->GetType();
//    switch (type) {
//      case ConstraintType::CHECK: {
//        //          std::pair<ExpressionType, type::Value> exp =
//        //          cons.GetCheckExpression();
//        //          if (CheckExp(tuple, column_itr, exp) == false) {
//        //            //LOG_TRACE("CHECK EXPRESSION constraint violated");
//        //            throw ConstraintException(
//        //                "CHECK EXPRESSION constraint violated : " +
//        //                std::string(tuple->GetInfo()));
//        //          }
//        break;
//      }
//      case ConstraintType::UNIQUE: {
//        break;
//      }
//      case ConstraintType::PRIMARY: {
//        break;
//      }
//      case ConstraintType::FOREIGN: {
//        break;
//      }
//      case ConstraintType::EXCLUSION: {
//        break;
//      }
//      default: {
//        std::string error =
//            StringUtil::Format("ConstraintType '%s' is not supported",
//                               ConstraintTypeToString(type).c_str());
//        //LOG_TRACE("%s", error.c_str());
//        throw ConstraintException(error);
//      }
//    }  // SWITCH
//  }    // FOR (constraints)

  (void)tuple;
  return true;
}
// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
// when updating a tuple, we will invoke this function with the argument set to
// nullptr.
// this is because we want to minimize data copy overhead by performing
// in-place update at executor level.
// however, when performing insert, we have to copy data immediately,
// and the argument cannot be set to nullptr.
extern long number_of_invalids_;
long number_of_invalids_ = 0;
ItemPointer DataTable::GetEmptyTupleSlot(const Tuple *tuple) {
  //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto &gc_manager = GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group = StorageManager::GetInstance()->GetTileGroup(
          free_item_pointer.block);
      tile_group->CopyTuple(tuple, free_item_pointer.offset);
    }
    return free_item_pointer;
  }
  //====================================================

  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<TileGroup> tile_group;
  uint tuple_slot = INVALID_OID;
  uint tile_group_id = INVALID_OID;
//  number_of_invalids_++;//循环次数.

//  sql_print_error("number_of_invalids_=%ld",number_of_invalids_);
  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTuple(tuple);//瓶颈在这里.

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }

//    break;
//    number_of_invalids_++;//循环次数
//    sql_print_error("number_of_invalids_=%ld",number_of_invalids_);
  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

//  //LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
//            tile_group_count_.load(), tile_group->GetTileGroupId(),
//            tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);
  return location;

//  (void)tuple;
//  return INVALID_ITEMPOINTER;
}

//deprecated
ItemPointer DataTable::GetEmptyTupleSlotNew(Tuple *tuple) {

  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<TileGroup> tile_group;
  uint tuple_slot = INVALID_OID;
  uint tile_group_id = INVALID_OID;

  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTupleNew(tuple, 0);//瓶颈在这里.

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }

  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

  ItemPointer location(tile_group_id, tuple_slot);
  return location;

}

ItemPointer DataTable::GetEmptyTupleSlotForThd(Tuple *tuple, uint curr_thd_id) {

//  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
//  size_t active_tile_group_id = number_of_tuples_ % curr_thd_id;
  size_t active_tile_group_id =  curr_thd_id%active_tilegroup_count_;
  std::shared_ptr<TileGroup> tile_group;
  uint tuple_slot = INVALID_OID;
  uint tile_group_id = INVALID_OID;

  auto &gc_manager = GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group = StorageManager::GetInstance()->GetTileGroup(
          free_item_pointer.block);
      // tile_group->CopyTuple(tuple, free_item_pointer.offset);
//      tile_group->CopyTupleNew(tuple, free_item_pointer.offset, curr_thd_id);
      uint offset = free_item_pointer.offset;
      tile_group->CopyTupleNew(tuple, offset, curr_thd_id);

    }
    return free_item_pointer;
  }

  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTupleNew(tuple, curr_thd_id);//瓶颈在这里.

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }

  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

  ItemPointer location(tile_group_id, tuple_slot);
  return location;

}

// Rewrite the CopyTupleNew function, that function is for mysql tuple buffer, whitch is not 
// suitable for recoveried tuple.
ItemPointer DataTable::GetEmptyTupleSlotForRecovery(Tuple *tuple, uint curr_thd_id) {
  size_t active_tile_group_id =  curr_thd_id%active_tilegroup_count_;
  std::shared_ptr<TileGroup> tile_group;
  uint tuple_slot = INVALID_OID;
  uint tile_group_id = INVALID_OID;

  auto &gc_manager = GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group = StorageManager::GetInstance()->GetTileGroup(
          free_item_pointer.block);
      uint offset = free_item_pointer.offset;
      tile_group->CopyTupleFromRecovery(tuple, offset, curr_thd_id);

    }
    return free_item_pointer;
  }

  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];
    tuple_slot = tile_group->InsertTupleFromRecovery(tuple, curr_thd_id);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    AddDefaultTileGroup(active_tile_group_id);
  }

  ItemPointer location(tile_group_id, tuple_slot);
  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
//    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

//  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
//  ItemPointer location = GetEmptyTupleSlotForThd(nullptr);
  if (location.block == INVALID_OID) {
//    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

//  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion(uint curr_thd_id) {
  // First, claim a slot
//  ItemPointer location = GetEmptyTupleSlot(nullptr);
  ItemPointer location = GetEmptyTupleSlotForThd(nullptr, curr_thd_id);
  if (location.block == INVALID_OID) {
//    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

//  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple,
//                               const TargetList *targets_ptr,
                               const uint *targets_ptr,
                               TransactionContext *transaction,
                               ItemPointer *index_entry_ptr) {
  if (CheckConstraints(tuple) == false) {
    //LOG_TRACE("InsertVersion(): Constraint violated");
    return false;
  }

  // Index checks and updates
  if (InsertInSecondaryIndexes(tuple, targets_ptr, transaction,
                               index_entry_ptr) == false) {
    //LOG_TRACE("Index constraint violated");
    return false;
  }
  return true;
}

ItemPointer DataTable::InsertTuple(const Tuple *tuple,
                                   TransactionContext *transaction,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  auto result =
      InsertTuple(tuple, location, transaction, index_entry_ptr, check_fk);
  if (result == false) {
    return INVALID_ITEMPOINTER;
  }
  return location;
}

//with tid 传入mysql线程id 物理插入
ItemPointer DataTable::InsertTuple(const Tuple *tuple,
                                   TransactionContext *transaction,
                                   uint curr_thd_id,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  auto result =
      InsertTuple(tuple, location, transaction, index_entry_ptr, curr_thd_id, check_fk);
  if (result == false) {
    return INVALID_ITEMPOINTER;
  }
  return location;
}

//with tid 传入mysql线程id 索引插入
bool DataTable::InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                            TransactionContext *transaction,
                            ItemPointer **index_entry_ptr,  uint curr_thd_id, bool check_fk) {
  if (CheckConstraints(tuple) == false) {
    //LOG_TRACE("InsertTuple(): Constraint violated");
    return false;
  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
    if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
      //LOG_TRACE("ForeignKey constraint violated");
      return false;
    }
    IncreaseTupleCount(1);
    return true;
  }
  // Index checks and updates
  if (InsertInIndexes(tuple, location, transaction, index_entry_ptr, curr_thd_id) == false) {
    //LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
  if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
    //LOG_TRACE("ForeignKey constraint violated");
    return false;
  }

  PELOTON_ASSERT((*index_entry_ptr)->block == location.block &&
                 (*index_entry_ptr)->offset == location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return true;
}


ItemPointer DataTable::InsertTupleNew(Tuple *tuple,
                                   TransactionContext *transaction,
                                    uint curr_thd_id,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {
//  ItemPointer location = GetEmptyTupleSlotNew(tuple);

  ItemPointer location = GetEmptyTupleSlotForThd(tuple, curr_thd_id);
  if (location.block == INVALID_OID) {
    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  auto result =
      InsertTupleNew(tuple, location, transaction, index_entry_ptr, curr_thd_id, check_fk);
  if (!result) {
    return INVALID_ITEMPOINTER;
  }
  return location;
}


bool DataTable::InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                            TransactionContext *transaction,
                            ItemPointer **index_entry_ptr, bool check_fk) {
  if (CheckConstraints(tuple) == false) {
    //LOG_TRACE("InsertTuple(): Constraint violated");
    return false;
  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
    if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
      //LOG_TRACE("ForeignKey constraint violated");
      return false;
    }
    IncreaseTupleCount(1);
    return true;
  }
  // Index checks and updates
  if (InsertInIndexes(tuple, location, transaction, index_entry_ptr) == false) {
    //LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
  if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
    //LOG_TRACE("ForeignKey constraint violated");
    return false;
  }

  PELOTON_ASSERT((*index_entry_ptr)->block == location.block &&
                 (*index_entry_ptr)->offset == location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return true;
}

bool DataTable::InsertTupleNew(Tuple *tuple, ItemPointer location,
                            TransactionContext *transaction,
                            ItemPointer **index_entry_ptr, uint curr_thd_id, bool check_fk) {
  (void)check_fk;
//  if (CheckConstraints(tuple) == false) {
//    //LOG_TRACE("InsertTuple(): Constraint violated");
//    return false;
//  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

//  //LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
//    if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
//      //LOG_TRACE("ForeignKey constraint violated");
//      return false;
//    }
    IncreaseTupleCount(1);
    IncreaseRealTupleCount(1);

    return true;
  }
  // Index checks and updates
  if (!InsertInIndexesNew(tuple, location, transaction, index_entry_ptr, curr_thd_id)) {
    //LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
//  if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
//    //LOG_TRACE("ForeignKey constraint violated");
//    return false;
//  }

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  IncreaseRealTupleCount(1);
  return true;
}

// insert tuple into a table that is without index.
ItemPointer DataTable::InsertTuple(const Tuple *tuple) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    //LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  //LOG_TRACE("Location: %u, %u", location.block, location.offset);
//  std::shared_ptr<Index> a;
//  UNUSED_ATTRIBUTE auto index_count = GetIndexCount();
//  PELOTON_ASSERT(index_count == 0);
  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
//  (void)a;
  return location;
//  (void)tuple;
//  return INVALID_ITEMPOINTER;
}

/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
bool DataTable::InsertInIndexes(const AbstractTuple *tuple,
                                ItemPointer location,
                                TransactionContext *transaction,
                                ItemPointer **index_entry_ptr) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id =
      number_of_tuples_ % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr =
          active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<Tuple> key(new Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
//    key->SetFromTupleNew((const Tuple*)tuple, indexed_columns);

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        // get unique tuple from primary/unique index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        //jy
        //res = index->InsertEntry(key.get(), *index_entry_ptr);
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), *index_entry_ptr);
        break;
    }

    // Handle failure
    if (res == false) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      *index_entry_ptr = nullptr;
      return false;
    } else {
      success_count += 1;
    }
    //LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
//  (void)tuple;
//  (void)location;
//  (void)transaction;
//  (void)index_entry_ptr;
  return true;
}

/**
 * //with tid 传入mysql线程id.
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
bool DataTable::InsertInIndexes(const AbstractTuple *tuple,
                                ItemPointer location,
                                TransactionContext *transaction,
                                ItemPointer **index_entry_ptr,
                                uint curr_thd_id) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id =
      number_of_tuples_ % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr =
          active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<Tuple> key(new Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
//    key->SetFromTupleNew((const Tuple*)tuple, indexed_columns);

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        // get unique tuple from primary/unique index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        //jy
        //res = index->InsertEntry(key.get(), *index_entry_ptr);
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn, curr_thd_id);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        index->InsertEntry(key.get(), *index_entry_ptr);
        break;
    }

    // Handle failure
    if (res == false) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      *index_entry_ptr = nullptr;
      return false;
    } else {
      success_count += 1;
    }
    //LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
//  (void)tuple;
//  (void)location;
//  (void)transaction;
//  (void)index_entry_ptr;
  return true;
}


bool DataTable::InsertInIndexesNew(const Tuple *tuple,
                                ItemPointer location,
                                TransactionContext *transaction,
                                ItemPointer **index_entry_ptr,
                               uint curr_thd_id) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id =
      number_of_tuples_ % active_indirection_array_count_;
//  size_t active_indirection_array_id =
//      curr_thd_id % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr =
          active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<Tuple> key(new Tuple(index_schema, true));
//    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
    key->SetFromTupleNew( (const Tuple* )tuple, indexed_columns);

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        // get unique tuple from primary/unique index.
        // if in this index there has been a visible or uncommitted
        // <key, location> pair, this constraint is violated
        //jy
        //res = index->InsertEntry(key.get(), *index_entry_ptr);
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn, curr_thd_id);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn, curr_thd_id);
//        index->InsertEntry(key.get(), *index_entry_ptr);
        break;
    }

    key = nullptr;
    // Handle failure
    if (!res) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      *index_entry_ptr = nullptr;
      return false;
    } else {
      success_count += 1;
    }
    // //LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
//  (void)tuple;
//  (void)location;
//  (void)transaction;
//  (void)index_entry_ptr;
  return true;
}


// FIXME: 索引的源码，没有搞懂 threadinfo * masstree_threadinfo_process 是干啥用的
// 也就是说 MassTreeIndex::CondInsertEntry() 的 curr_thd__id 没搞懂是啥用，我暂时传入自己的 thd_id
ItemPointer *DataTable::InsertIndexFromRecovery(const Tuple *tuple, ItemPointer location, uint curr_thd_id) {
  int index_count = GetIndexCount();
  ItemPointer *index_entry_ptr = nullptr;

  size_t active_indirection_array_id =
      number_of_indexs_.fetch_add(1) % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array =
        active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      index_entry_ptr = active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

  (index_entry_ptr)->block = location.block;
  (index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  // 用的是索引的条件插入，但由于不会有重复的元素，所以设定为返回 false
  std::function<bool(const void *)> fn =
      std::bind([](const void *){return false;}, nullptr);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr) continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<Tuple> key(new Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
    key->SetKeyLength(index->GetKeyLength());

    switch (index->GetIndexType()) {
      case IndexConstraintType::PRIMARY_KEY:
      case IndexConstraintType::UNIQUE: {
        res = index->CondInsertEntry(key.get(), index_entry_ptr, fn, curr_thd_id);
      } break;

      case IndexConstraintType::DEFAULT:
      default:
        res = index->CondInsertEntry(key.get(), index_entry_ptr, fn, curr_thd_id);
        break;
    }

    // 不允许插入失败
    assert (res);
  }

  return index_entry_ptr;
}


bool DataTable::InsertInSecondaryIndexes(
//    const AbstractTuple *tuple, const TargetList *targets_ptr,
    const AbstractTuple *tuple, const uint *targets_ptr,
    TransactionContext *transaction,
    ItemPointer *index_entry_ptr) {
//  int index_count = GetIndexCount();
//  // Transform the target list into a hash set
//  // when attempting to perform insertion to a secondary index,
//  // we must check whether the updated column is a secondary index column.
//  // insertion happens only if the updated column is a secondary index column.
//  std::unordered_set<uint> targets_set;
//  for (auto target : *targets_ptr) {
//    targets_set.insert(target.first);
//  }
//
//  bool res = true;
//
//  auto &transaction_manager =
//      TransactionManagerFactory::GetInstance();
//
//  std::function<bool(const void *)> fn =
//      std::bind(&TransactionManager::IsOccupied,
//                &transaction_manager, transaction, std::placeholders::_1);
//
//  // Check existence for primary/unique indexes
//  // Since this is NOT protected by a lock, concurrent insert may happen.
//  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
//    auto index = GetIndex(index_itr);
//    if (index == nullptr) continue;
//    auto index_schema = index->GetKeySchema();
//    auto indexed_columns = index_schema->GetIndexedColumns();
//
//    if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
//      continue;
//    }
//
//    // Check if we need to update the secondary index
//    bool updated = false;
//    for (auto col : indexed_columns) {
//      if (targets_set.find(col) != targets_set.end()) {
//        updated = true;
//        break;
//      }
//    }
//
//    // If attributes on key are not updated, skip the index update
//    if (updated == false) {
//      continue;
//    }
//
//    // Key attributes are updated, insert a new entry in all secondary index
//    std::unique_ptr<Tuple> key(new Tuple(index_schema, true));
//
//    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
//
//    switch (index->GetIndexType()) {
//      case IndexConstraintType::PRIMARY_KEY:
//      case IndexConstraintType::UNIQUE: {
//        res = index->CondInsertEntry(key.get(), index_entry_ptr, fn);
//      } break;
//      case IndexConstraintType::DEFAULT:
//      default:
//        index->InsertEntry(key.get(), index_entry_ptr);
//        break;
//    }
//    //LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
//  }
//  return res;
  (void)tuple;
  (void)targets_ptr;
  (void)transaction;
  (void)index_entry_ptr;
    return true;

}

/**
 * @brief This function checks any other table which has a foreign key
 *constraint
 * referencing the current table, where a tuple is updated/deleted. The final
 * result depends on the type of cascade action.
 *
 * @param prev_tuple: The tuple which will be updated/deleted in the current
 * table
 * @param new_tuple: The new tuple after update. This parameter is ignored
 * if is_update is false.
 * @param current_txn: The current transaction context
 * @param context: The executor context passed from upper level
 * @param is_update: whether this is a update action (false means delete)
 *
 * @return True if the check is successful (nothing happens) or the cascade
 *operation
 * is done properly. Otherwise returns false. Note that the transaction result
 * is not set in this function.
 */
//bool DataTable::CheckForeignKeySrcAndCascade(
//    Tuple *prev_tuple, Tuple *new_tuple,
//    TransactionContext *current_txn,
//    ExecutorContext *context, bool is_update) {
//  if (!schema->HasForeignKeySources()) return true;
//
//  auto &transaction_manager =
//      TransactionManagerFactory::GetInstance();
//
//  for (auto cons : schema->GetForeignKeySources()) {
//    // Check if any row in the source table references the current tuple
//    uint source_table_id = cons->GetTableOid();
//    DataTable *src_table = nullptr;
//    try {
//      src_table = (DataTable *)StorageManager::GetInstance()
//          ->GetTableWithOid(GetDatabaseOid(), source_table_id);
//    } catch (CatalogException &e) {
//      //LOG_TRACE("Can't find table %d! Return false", source_table_id);
//      return false;
//    }
//
//    int src_table_index_count = src_table->GetIndexCount();
//    for (int iter = 0; iter < src_table_index_count; iter++) {
//      auto index = src_table->GetIndex(iter);
//      if (index == nullptr) continue;
//
//      // Make sure this is the right index to search in
//      if (index->GetOid() == cons->GetIndexOid() &&
//          index->GetMetadata()->GetKeyAttrs() == cons->GetColumnIds()) {
//        LOG_DEBUG("Searching in source tables's fk index...\n");
//
//        std::vector<uint> key_attrs = cons->GetColumnIds();
//        std::unique_ptr<Schema> fk_schema(
//            Schema::CopySchema(src_table->GetSchema(), key_attrs));
//        std::unique_ptr<Tuple> key(
//            new Tuple(fk_schema.get(), true));
//
//        key->SetFromTuple(prev_tuple, cons->GetFKSinkColumnIds(),
//                          index->GetPool());
//
//        std::vector<ItemPointer *> location_ptrs;
//        index->ScanKey(key.get(), location_ptrs);
//
//        if (location_ptrs.size() > 0) {
//          LOG_DEBUG("Something found in the source table!\n");
//
//          for (ItemPointer *ptr : location_ptrs) {
//            auto src_tile_group = src_table->GetTileGroupById(ptr->block);
//            auto src_tile_group_header = src_tile_group->GetHeader();
//
//            auto visibility = transaction_manager.IsVisible(
//                current_txn, src_tile_group_header, ptr->offset,
//                VisibilityIdType::COMMIT_ID);
//
//            if (visibility != VisibilityType::OK) continue;
//
//            switch (cons->GetFKUpdateAction()) {
//              // Currently NOACTION is the same as RESTRICT
//              case FKConstrActionType::NOACTION:
//              case FKConstrActionType::RESTRICT: {
//                return false;
//              }
//              case FKConstrActionType::CASCADE:
//              default: {
//                // Update
//                bool src_is_owner = transaction_manager.IsOwner(
//                    current_txn, src_tile_group_header, ptr->offset);
//
//                // Read the referencing tuple, update the read timestamp so that
//                // we can
//                // delete it later
//                bool ret = transaction_manager.PerformRead(
//                    current_txn, *ptr, src_tile_group_header, true);
//
//                if (ret == false) {
//                  if (src_is_owner) {
//                    transaction_manager.YieldOwnership(
//                        current_txn, src_tile_group_header, ptr->offset);
//                  }
//                  return false;
//                }
//
//                ContainerTuple<TileGroup> src_old_tuple(
//                    src_tile_group.get(), ptr->offset);
//                Tuple src_new_tuple(src_table->GetSchema(), true);
//
//                if (is_update) {
//                  for (uint col_itr = 0;
//                       col_itr < src_table->GetSchema()->GetColumnCount();
//                       col_itr++) {
//                    type::Value val = src_old_tuple.GetValue(col_itr);
//                    src_new_tuple.SetValue(col_itr, val, context->GetPool());
//                  }
//
//                  // Set the primary key fields
//                  for (uint k = 0; k < key_attrs.size(); k++) {
//                    auto src_col_index = key_attrs[k];
//                    auto sink_col_index = cons->GetFKSinkColumnIds()[k];
//                    src_new_tuple.SetValue(src_col_index,
//                                           new_tuple->GetValue(sink_col_index),
//                                           context->GetPool());
//                  }
//                }
//
//                ItemPointer new_loc = src_table->InsertEmptyVersion();
//
//                if (new_loc.IsNull()) {
//                  if (src_is_owner == false) {
//                    transaction_manager.YieldOwnership(
//                        current_txn, src_tile_group_header, ptr->offset);
//                  }
//                  return false;
//                }
//
//                transaction_manager.PerformDelete(current_txn, *ptr, new_loc);
//
//                // For delete cascade, just stop here
//                if (is_update == false) {
//                  break;
//                }
//
//                ItemPointer *index_entry_ptr = nullptr;
//                ItemPointer location = src_table->InsertTuple(
//                    &src_new_tuple, current_txn, &index_entry_ptr, false);
//
//                if (location.block == INVALID_OID) {
//                  return false;
//                }
//
//                transaction_manager.PerformInsert(current_txn, location,
//                                                  index_entry_ptr);
//
//                break;
//              }
//            }
//          }
//        }
//
//        break;
//      }
//    }
//  }
//  return true;
//}

// PA - looks like the FIXME has been done. We check to see if the key
// is visible
/**
 * @brief Check if all the foreign key constraints on this table
 * is satisfied by checking whether the key exist in the referred table
 *
 * FIXME: this still does not guarantee correctness under concurrent transaction
 *   because it only check if the key exists the referred table's index
 *   -- however this key might be a uncommitted key that is not visible to
 *   and it might be deleted if that txn abort.
 *   We should modify this function and add logic to check
 *   if the result of the ScanKey is visible.
 *
 * @returns True on success, false if any foreign key constraints fail
 */
bool DataTable::CheckForeignKeyConstraints(
    const AbstractTuple *tuple, TransactionContext *transaction) {
//  for (auto foreign_key : schema->GetForeignKeyConstraints()) {
//    uint sink_table_id = foreign_key->GetFKSinkTableOid();
//    DataTable *ref_table = nullptr;
//    try {
//      ref_table = (DataTable *)StorageManager::GetInstance()
//          ->GetTableWithOid(database_oid, sink_table_id);
//    } catch (CatalogException &e) {
//      LOG_ERROR("Can't find table %d! Return false", sink_table_id);
//      return false;
//    }
//    int ref_table_index_count = ref_table->GetIndexCount();
//
//    for (int index_itr = ref_table_index_count - 1; index_itr >= 0;
//         --index_itr) {
//      auto index = ref_table->GetIndex(index_itr);
//      if (index == nullptr) continue;
//
//      // The foreign key constraints only refer to the primary key
//      if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
//        std::vector<uint> key_attrs = foreign_key->GetFKSinkColumnIds();
//        std::unique_ptr<Schema> foreign_key_schema(
//            Schema::CopySchema(ref_table->schema, key_attrs));
//        std::unique_ptr<Tuple> key(
//            new Tuple(foreign_key_schema.get(), true));
//        key->SetFromTuple(tuple, foreign_key->GetColumnIds(), index->GetPool());
//
//        //LOG_TRACE("check key: %s", key->GetInfo().c_str());
//        std::vector<ItemPointer *> location_ptrs;
//        index->ScanKey(key.get(), location_ptrs);
//
//        // if this key doesn't exist in the referred column
//        if (location_ptrs.size() == 0) {
//          LOG_DEBUG("The key: %s does not exist in table %s\n",
//                    key->GetInfo().c_str(), ref_table->GetName().c_str());
//          return false;
//        }
//
//        // Check the visibility of the result
//        auto tile_group = ref_table->GetTileGroupById(location_ptrs[0]->block);
//        auto tile_group_header = tile_group->GetHeader();
//
//        auto &transaction_manager =
//            TransactionManagerFactory::GetInstance();
//        auto visibility = transaction_manager.IsVisible(
//            transaction, tile_group_header, location_ptrs[0]->offset,
//            VisibilityIdType::READ_ID);
//
//        if (visibility != VisibilityType::OK) {
//          LOG_DEBUG(
//              "The key: %s is not yet visible in table %s, visibility "
//              "type: %s.\n",
//              key->GetInfo().c_str(), ref_table->GetName().c_str(),
//              VisibilityTypeToString(visibility).c_str());
//          return false;
//        }
//
//        break;
//      }
//    }
//  }
  (void)tuple;
  (void)transaction;
  return true;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseTupleCount(const size_t &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

void DataTable::IncreaseRealTupleCount(const size_t &amount) {
  real_count += amount;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseTupleCount(const size_t &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetTupleCount(const size_t &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t DataTable::GetTupleCount() const { return number_of_tuples_; }

size_t DataTable::GetRealTupleCount() const { return real_count; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *DataTable::GetTileGroupWithLayout(
    std::shared_ptr<const Layout> layout) {
  uint tile_group_id =
      StorageManager::GetInstance()->GetNextTileGroupId();
  return (AbstractTable::GetTileGroupWithLayout(database_oid, tile_group_id,
                                                layout, tuples_per_tilegroup_));
}
//
uint DataTable::AddDefaultIndirectionArray(
    const size_t &active_indirection_array_id) {
  auto &manager = Manager::GetInstance();
  uint indirection_array_id = manager.GetNextIndirectionArrayId();

  std::shared_ptr<IndirectionArray> indirection_array(
      new IndirectionArray(indirection_array_id));
  manager.AddIndirectionArray(indirection_array_id, indirection_array);

//  COMPILER_MEMORY_FENCE;

  active_indirection_arrays_[active_indirection_array_id] = indirection_array;

  return indirection_array_id;
}
//
uint DataTable::AddDefaultTileGroup() {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  return AddDefaultTileGroup(active_tile_group_id);
}
//
uint DataTable::AddDefaultTileGroup(const size_t &active_tile_group_id) {
  uint tile_group_id = INVALID_OID;

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(
      GetTileGroupWithLayout(default_layout_));
//  PELOTON_ASSERT(tile_group.get());

  tile_group_id = tile_group->GetTileGroupId();

//  //LOG_TRACE("Added a tile group ");
  tile_groups_.Append(tile_group_id);

  // add tile group metadata in locator
  StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                       tile_group);

//  COMPILER_MEMORY_FENCE;

  active_tile_groups_[active_tile_group_id] = tile_group;

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
//  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

//  //LOG_TRACE("Recording tile group : %u ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroupWithOidForRecovery(const uint &tile_group_id) {
PELOTON_ASSERT(tile_group_id);

 std::vector<Schema> schemas;
 schemas.push_back(*schema);
 std::shared_ptr<const Layout> layout = nullptr;

 // The TileGroup for recovery is always added in ROW layout,
 // This was a part of the previous design. If you are planning
 // to change this, make sure the layout is added to the catalog
 if (default_layout_->IsRowStore()) {
   layout = default_layout_;
 } else {
   layout = std::shared_ptr<const Layout>(
       new const Layout(schema->GetColumnCount()));
 }

 std::shared_ptr<TileGroup> tile_group(TileGroupFactory::GetTileGroup(
     database_oid, table_oid, tile_group_id, this, schemas, layout,
     tuples_per_tilegroup_));

 auto tile_groups_exists = tile_groups_.Contains(tile_group_id);

 if (tile_groups_exists == false) {
   tile_groups_.Append(tile_group_id);

   //LOG_TRACE("Added a tile group ");

   // add tile group metadata in locator
   StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                        tile_group);

   // we must guarantee that the compiler always add tile group before adding
   // tile_group_count_.
   COMPILER_MEMORY_FENCE;

   tile_group_count_++;

   //LOG_TRACE("Recording tile group : %u ", tile_group_id);
 }
}

// NOTE: This function is only used in test cases.
void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
//  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
//
//  active_tile_groups_[active_tile_group_id] = tile_group;
//
//  uint tile_group_id = tile_group->GetTileGroupId();
//
//  tile_groups_.Append(tile_group_id);
//
//  // add tile group in catalog
//  StorageManager::GetInstance()->AddTileGroup(tile_group_id,
//                                                       tile_group);
//
//  // we must guarantee that the compiler always add tile group before adding
//  // tile_group_count_.
//  COMPILER_MEMORY_FENCE;
//
//  tile_group_count_++;
//
//  //LOG_TRACE("Recording tile group : %u ", tile_group_id);
  (void)tile_group;
}

size_t DataTable::GetTileGroupCount() const { return tile_group_count_; }

std::shared_ptr<TileGroup> DataTable::GetTileGroup(
    const std::size_t &tile_group_offset) const {
//  PELOTON_ASSERT(tile_group_offset < GetTileGroupCount());

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  return GetTileGroupById(tile_group_id);
}

std::shared_ptr<TileGroup> DataTable::GetTileGroupById(
    const uint &tile_group_id) const {
  auto storage_manager = StorageManager::GetInstance();
  return storage_manager->GetTileGroup(tile_group_id);
}

void DataTable::DropTileGroups() {
  auto storage_manager = StorageManager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      // drop tile group in catalog
      storage_manager->DropTileGroup(tile_group_id);
    }
  }

  // Clear array
  tile_groups_.Clear();

  tile_group_count_ = 0;
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//
void DataTable::AddIndex(Index* index) {
  // Add index
  indexes_.push_back(index);

  // Add index column info
  auto index_columns_ = index->GetMetadata()->GetKeyAttrs();
  std::set<uint> index_columns_set(index_columns_.begin(),
                                    index_columns_.end());

  indexes_columns_.push_back(index_columns_set);
//  (void)index;

}

Index* DataTable::GetIndexWithOid(
    const uint &index_oid) {
  Index* ret_index;
  auto index_count = indexes_.size();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    ret_index = indexes_[index_itr];
    if (ret_index != nullptr && ret_index->GetOid() == index_oid) {
      break;
    }
  }
  if (ret_index == nullptr) {
    throw CatalogException("No index with oid = " + std::to_string(index_oid) +
                           " is found");
  }
  return ret_index;
}

Index* DataTable::GetIndexWithName(std::string index_name) {
  Index* ret_index;
  auto index_count = indexes_.size();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    ret_index = indexes_[index_itr];
    if (ret_index != nullptr && ret_index->GetName() == index_name) {
      break;
    }
    ret_index = nullptr; // 如果不加这一行，找不到对应的索引时就会返回最后一次Find得到的索引，逻辑错误。
  }
  if (ret_index == nullptr) {
    throw CatalogException("No index with oid = " + index_name +
                           " is found");
  }
  return ret_index;
}


void DataTable::DropIndexWithOid(const uint &index_oid) {
  uint index_offset = 0;
  Index* index;
  auto index_count = indexes_.size();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_[index_itr];
    if (index != nullptr && index->GetOid() == index_oid) {
      break;
    }
  }

  PELOTON_ASSERT(index_offset < indexes_.GetSize());

  // Drop the index FIXME: should delete ptr
  indexes_[index_offset] = nullptr;

  // Drop index column info
  indexes_columns_[index_offset].clear();
}


//
//void DataTable::DropIndexes() {
//  // TODO: iterate over all indexes, and actually drop them
//
//  indexes_.Clear();
//
//  indexes_columns_.clear();
//}

// This is a dangerous function, use GetIndexWithOid() instead. Note
// that the returned index could be a nullptr once we can drop index
// with oid (due to a limitation of LockFreeArray).
Index* DataTable::GetIndex(const uint &index_offset) {
  if (index_offset >= indexes_.size()) {
    return nullptr;
  }
  auto ret_index = indexes_[index_offset];

  return ret_index;
}

//
std::set<uint> DataTable::GetIndexAttrs(const uint &index_offset) const {
  PELOTON_ASSERT(index_offset < GetIndexCount());

  auto index_attrs = indexes_columns_.at(index_offset);

  return index_attrs;
}

uint DataTable::GetIndexCount() const {
  size_t index_count = indexes_.size();

  return index_count;
}
//
//uint DataTable::GetValidIndexCount() const {
//  std::shared_ptr<Index> index;
//  auto index_count = indexes_.GetSize();
//  uint valid_index_count = 0;
//
//  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
//    index = indexes_.Find(index_itr);
//    if (index == nullptr) {
//      continue;
//    }
//
//    valid_index_count++;
//  }
//
//  return valid_index_count;
//}
//
//// Get the schema for the new transformed tile group
//std::vector<Schema> TransformTileGroupSchema(
//    TileGroup *tile_group, const Layout &layout) {
//  std::vector<Schema> new_schema;
//  uint orig_tile_offset, orig_tile_column_offset;
//  uint new_tile_offset, new_tile_column_offset;
//  auto tile_group_layout = tile_group->GetLayout();
//
//  // First, get info from the original tile group's schema
//  std::map<uint, std::map<uint, Column>> schemas;
//
//  uint32_t column_count = layout.GetColumnCount();
//  for (uint col_id = 0; col_id < column_count; col_id++) {
//    // Get TileGroup layout's tile and offset for col_id.
//    tile_group_layout.LocateTileAndColumn(col_id, orig_tile_offset,
//                                          orig_tile_column_offset);
//    // Get new layout's tile and offset for col_id.
//    layout.LocateTileAndColumn(col_id, new_tile_offset, new_tile_column_offset);
//
//    // Get the column info from original tile
//    auto tile = tile_group->GetTile(orig_tile_offset);
//    PELOTON_ASSERT(tile != nullptr);
//    auto orig_schema = tile->GetSchema();
//    auto column_info = orig_schema->GetColumn(orig_tile_column_offset);
//    schemas[new_tile_offset][new_tile_column_offset] = column_info;
//  }
//
//  // Then, build the new schema
//  for (auto schemas_tile_entry : schemas) {
//    std::vector<Column> columns;
//    for (auto schemas_column_entry : schemas_tile_entry.second)
//      columns.push_back(schemas_column_entry.second);
//
//    Schema tile_schema(columns);
//    new_schema.push_back(tile_schema);
//  }
//
//  return new_schema;
//}
//
//// Set the transformed tile group column-at-a-time
//void SetTransformedTileGroup(TileGroup *orig_tile_group,
//                             TileGroup *new_tile_group) {
//  auto new_layout = new_tile_group->GetLayout();
//  auto orig_layout = orig_tile_group->GetLayout();
//
//  // Check that both tile groups have the same schema
//  // Currently done by checking that the number of columns are equal
//  // TODO Pooja: Handle schema equality for multiple schema versions.
//  UNUSED_ATTRIBUTE auto new_column_count = new_layout.GetColumnCount();
//  UNUSED_ATTRIBUTE auto orig_column_count = orig_layout.GetColumnCount();
//  PELOTON_ASSERT(new_column_count == orig_column_count);
//
//  uint orig_tile_offset, orig_tile_column_offset;
//  uint new_tile_offset, new_tile_column_offset;
//
//  auto column_count = new_column_count;
//  auto tuple_count = orig_tile_group->GetAllocatedTupleCount();
//  // Go over each column copying onto the new tile group
//  for (uint column_itr = 0; column_itr < column_count; column_itr++) {
//    // Locate the original base tile and tile column offset
//    orig_layout.LocateTileAndColumn(column_itr, orig_tile_offset,
//                                    orig_tile_column_offset);
//
//    new_layout.LocateTileAndColumn(column_itr, new_tile_offset,
//                                   new_tile_column_offset);
//
//    auto orig_tile = orig_tile_group->GetTile(orig_tile_offset);
//    auto new_tile = new_tile_group->GetTile(new_tile_offset);
//
//    // Copy the column over to the new tile group
//    for (uint tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
//      type::Value val =
//          (orig_tile->GetValue(tuple_itr, orig_tile_column_offset));
//      new_tile->SetValue(val, tuple_itr, new_tile_column_offset);
//    }
//  }
//
//  // Finally, copy over the tile header
//  auto header = orig_tile_group->GetHeader();
//  auto new_header = new_tile_group->GetHeader();
//  *new_header = *header;
//}
//
//TileGroup *DataTable::TransformTileGroup(
//    const uint &tile_group_offset, const double &theta) {
//  // First, check if the tile group is in this table
//  if (tile_group_offset >= tile_groups_.GetSize()) {
//    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
//    return nullptr;
//  }
//
//  auto tile_group_id =
//      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);
//
//  // Get orig tile group from catalog
//  auto storage_tilegroup = StorageManager::GetInstance();
//  auto tile_group = storage_tilegroup->GetTileGroup(tile_group_id);
//  auto diff = tile_group->GetLayout().GetLayoutDifference(*default_layout_);
//
//  // Check threshold for transformation
//  if (diff < theta) {
//    return nullptr;
//  }
//
//  //LOG_TRACE("Transforming tile group : %u", tile_group_offset);
//
//  // Get the schema for the new transformed tile group
//  auto new_schema =
//      TransformTileGroupSchema(tile_group.get(), *default_layout_);
//
//  // Allocate space for the transformed tile group
//  std::shared_ptr<TileGroup> new_tile_group(
//      TileGroupFactory::GetTileGroup(
//          tile_group->GetDatabaseId(), tile_group->GetTableId(),
//          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
//          new_schema, default_layout_, tile_group->GetAllocatedTupleCount()));
//
//  // Set the transformed tile group column-at-a-time
//  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());
//
//  // Set the location of the new tile group
//  // and clean up the orig tile group
//  storage_tilegroup->AddTileGroup(tile_group_id, new_tile_group);
//
//  return new_tile_group.get();
//}
//
//void DataTable::RecordLayoutSample(const tuning::Sample &sample) {
//  // Add layout sample
//  {
//    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
//    layout_samples_.push_back(sample);
//  }
//}
//
//std::vector<tuning::Sample> DataTable::GetLayoutSamples() {
//  {
//    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
//    return layout_samples_;
//  }
//}
//
//void DataTable::ClearLayoutSamples() {
//  // Clear layout samples list
//  {
//    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
//    layout_samples_.clear();
//  }
//}
//
//void DataTable::RecordIndexSample(const tuning::Sample &sample) {
//  // Add index sample
//  {
//    std::lock_guard<std::mutex> lock(index_samples_mutex_);
//    index_samples_.push_back(sample);
//  }
//}
//
//std::vector<tuning::Sample> DataTable::GetIndexSamples() {
//  {
//    std::lock_guard<std::mutex> lock(index_samples_mutex_);
//    return index_samples_;
//  }
//}
//
//void DataTable::ClearIndexSamples() {
//  // Clear index samples list
//  {
//    std::lock_guard<std::mutex> lock(index_samples_mutex_);
//    index_samples_.clear();
//  }
//}
//
//void DataTable::AddTrigger(trigger::Trigger new_trigger) {
//  trigger_list_->AddTrigger(new_trigger);
//}
//
//int DataTable::GetTriggerNumber() {
//  return trigger_list_->GetTriggerListSize();
//}
//
//trigger::Trigger *DataTable::GetTriggerByIndex(int n) {
//  if (trigger_list_->GetTriggerListSize() <= n) return nullptr;
//  return trigger_list_->Get(n);
//}
//
//trigger::TriggerList *DataTable::GetTriggerList() {
//  if (trigger_list_->GetTriggerListSize() <= 0) return nullptr;
//  return trigger_list_.get();
//}
//
//void DataTable::UpdateTriggerListFromCatalog(
//    TransactionContext *txn) {
//  trigger_list_ = Catalog::GetInstance()
//      ->GetSystemCatalogs(database_oid)
//      ->GetTriggerCatalog()
//      ->GetTriggers(txn, table_oid);
//}
//
//hash_t DataTable::Hash() const {
//  auto oid = GetOid();
//  hash_t hash = HashUtil::Hash(&oid);
//  hash = HashUtil::CombineHashes(
//      hash, HashUtil::HashBytes(GetName().c_str(), GetName().length()));
//  auto db_oid = GetOid();
//  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&db_oid));
//  return hash;
//}
//
//bool DataTable::Equals(const DataTable &other) const {
//  return (*this == other);
//}
//
//bool DataTable::operator==(const DataTable &rhs) const {
//  if (GetName() != rhs.GetName()) return false;
//  if (GetDatabaseOid() != rhs.GetDatabaseOid()) return false;
//  if (GetOid() != rhs.GetOid()) return false;
//  return true;
//}
//
bool DataTable::SetCurrentLayoutOid(uint new_layout_oid) {
  uint old_oid = current_layout_oid_;
  while (old_oid <= new_layout_oid) {
    if (current_layout_oid_.compare_exchange_strong(old_oid, new_layout_oid)) {
      return true;
    }
    old_oid = current_layout_oid_;
  }
  return false;
}

void DataTable::ResetActiveTileGroupsAfterRecovery() {
  auto storage_manager = StorageManager::GetInstance();
  uint tile_group_index = 0;
  uint active_tile_group_index;

  for (active_tile_group_index = 0; active_tile_group_index < active_tilegroup_count_; active_tile_group_index++) {
    while (tile_group_index < tile_group_count_) {
      auto tile_group = storage_manager->GetTileGroup(tile_groups_.Find(tile_group_index));
      if (tile_group->GetNextTupleSlot() < tile_group->GetAllocatedTupleCount()) {
        active_tile_groups_[active_tile_group_index] = tile_group;
        tile_group_index++;
        break;
      }
      tile_group_index++;
    }
  }

  assert(active_tile_group_index == active_tilegroup_count_);
}