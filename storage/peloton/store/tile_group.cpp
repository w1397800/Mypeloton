//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.cpp
//
// Identification: src/storage/tile_group.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iostream>
//#include <numeric>
//#include <sstream>
#include "storage_manager.h"
#include "abstract_table.h"
//#include "layout.h"
#include "tile.h"
//#include "tile_group_header.h"
//#include "tile_group.h"
#include "tuple.h"

//#include "common/container_tuple.h"
//#include "common/logger.h"
//#include "common/platform.h"
#include "storage/peloton/util/stringbox_util.h"


TileGroup::TileGroup(BackendType backend_type,
                     TileGroupHeader *tile_group_header, AbstractTable *table,
                     const std::vector<Schema> &schemas,
                     std::shared_ptr<const Layout> layout, int tuple_count)
    : database_id(INVALID_OID),
      table_id(INVALID_OID),
      tile_group_id(INVALID_OID),
      backend_type(backend_type),
      tile_group_header(tile_group_header),
      table(table),
      num_tuple_slots_(tuple_count),
      tile_group_layout_(layout) {
  tile_count_ = schemas.size();
  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    StorageManager *storage_manager = StorageManager::GetInstance();
    uint tile_id = storage_manager->GetNextTileId();

    std::shared_ptr<Tile> tile(TileFactory::GetTile(
        backend_type, database_id, table_id, tile_group_id, tile_id,
        tile_group_header, schemas[tile_itr], this, tuple_count));

    // Add a reference to the tile in the tile group
    tiles.push_back(tile);
  }
}

TileGroup::~TileGroup() {
  // Drop references on all tiles

  // clean up tile group header
  delete tile_group_header;
}

uint TileGroup::GetTileId(const uint tile_id) const {
  PELOTON_ASSERT(tiles[tile_id]);
  return tiles[tile_id]->GetTileId();
}

AbstractPool *TileGroup::GetTilePool(const uint tile_id) const {
  Tile *tile = GetTile(tile_id);

  if (tile != nullptr) {
    return tile->GetPool();
  }

  return nullptr;
}

uint TileGroup::GetTileGroupId() const {
  return tile_group_id;
}

// TODO: check when this function is called. --Yingjun
uint TileGroup::GetNextTupleSlot() const {
  return tile_group_header->GetCurrentNextTupleSlot();
}

// this function is called only when building tile groups for aggregation
// operations.
uint TileGroup::GetActiveTupleCount() const {
  return tile_group_header->GetActiveTupleCount();
}


//===--------------------------------------------------------------------===//
// Operations
//===--------------------------------------------------------------------===//

/**
 * Copy from tuple.
 */
void TileGroup::CopyTuple(const Tuple *tuple, const uint &tuple_slot_id) {
//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);

  uint tile_column_count;
  uint column_itr = 0;

  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    tile_column_count = schema->GetColumnCount();
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PELOTON_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    Tuple tile_tuple(schema, tile_tuple_location);
    (void)tile_tuple;
    (void)tile_column_count;
    (void)column_itr;
    (void)tuple;



    for (uint tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      Value val = (tuple->GetValue(column_itr));
      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());
      column_itr++;
    }
  }
}

// Copy from a peloton tuple rather than mysql tuple format.
void TileGroup::CopyTupleFromRecovery(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id) {
  (void)curr_thd_id;
  uint column_count;
  std::vector<uint> buf_offset;
  AbstractPool* pool =
      StorageManager::GetPoolByThdId(curr_thd_id);

  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    column_count = schema->GetColumnCount();
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PELOTON_ASSERT(tile_tuple_location);

    Tuple tuple_warper(schema, tile_tuple_location);
    char *tuple_pos = tuple->GetData();

    for (uint column_id = 0; column_id < column_count; column_id++) {
      const Column &column = schema->GetColumn(column_id);
      auto typeId = column.GetType();
      const auto pos = tuple_pos + schema->GetOffset(column_id);

      if (TypeId::VARCHAR == typeId) {
        char *actual_pos =
            *reinterpret_cast<char *const *>(pos);
        uint32_t var_len =
            *reinterpret_cast<const uint32_t *>(actual_pos);
        tuple_warper.SetValueMemAndNVM(column_id, actual_pos + sizeof(uint32_t), var_len, pool);

      }else if(TypeId::CHAR == typeId){
        auto var_len = column.GetLength();
        tuple_warper.SetValueMemAndNVM(column_id, pos, var_len, pool);

      }else {
        tuple_warper.SetValueMemAndNVM(column_id, pos, -1, pool);
      }
    }
  }
}


void TileGroup::CopyTupleNew(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id) {
//void TileGroup::CopyTupleNew(const Tuple *tuple, uint &tuple_slot_id) {
//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);
  (void)curr_thd_id;
  uint column_count;
  std::vector<uint> buf_offset;
//  tuple = const_cast<Tuple*> (tuple);
  AbstractPool* pool =
//      StorageManager::GetInstance()->GetPoolByThdId(curr_thd_id);
      StorageManager::GetPoolByThdId(curr_thd_id);//目前在用的
//        nullptr;
//        &IntelPool::GetInstance();

  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    column_count = schema->GetColumnCount();
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    char *tile_tuple_location_nvm;



    PELOTON_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    Tuple tile_tuple(schema, tile_tuple_location);

    if(FOR_NVM){
      tile_tuple_location_nvm = tile->GetTupleLocation_nvm(tuple_slot_id);
      tile_tuple.SetData_nvm(tile_tuple_location_nvm);
    }

    (void)tile_tuple;
    (void)tuple;

//    const char *data_dist = tuple->GetData();
    char *data_buf = tuple->GetDataAddress();
//    memcpy(tile_tuple.GetData(), data_dist, schema->GetLength());

//    tuple->GetBuf_offset().push_back(0);//最开始的offset为0
    buf_offset.push_back(0);//最开始的offset为0 主要是给索引插入定位数据用的.
    uint offset = 0;
    for (uint column_id = 0; column_id < column_count; column_id++) {
//      Value val = (tuple->GetValue(column_itr));
//      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());

      const Column &column = schema->GetColumn(column_id);
      auto typeId = column.GetType();
      //src是buf那边根据偏移量得到的值. int和float类型是4, double, char是8.
      //varchar现有1位或2位, 然后是具体的值. src只需要传入具体的值即可

      if (TypeId::VARCHAR == typeId) {
        uint var_len = 0;
        if(column.GetLength()<86){
          var_len = *(int8*)(data_buf+offset);
          offset+=1;
//          data_buf += 1;
        }else {
          var_len = *(int16*)(data_buf+offset);
          offset+=2;
//          data_buf += 2;
        }
        tile_tuple.SetValueMemAndNVM(column_id, data_buf+offset, var_len, pool);
//        data_buf += column.GetLength()*3;

        offset += column.GetLength()*3;
        buf_offset.push_back(offset);
//        tuple->GetBuf_offset().push_back(offset);

      }else if(TypeId::CHAR == typeId){
        uint var_len = column.GetLength();
        tile_tuple.SetValueMemAndNVM(column_id, data_buf+offset, var_len, pool);
//        data_buf += column.GetLength()*3;

        offset += column.GetLength()*3;
        buf_offset.push_back(offset);
      }else {
        tile_tuple.SetValueMemAndNVM(column_id, data_buf+offset, -1, pool);
//        data_buf += column.GetLength();

        offset += column.GetLength();
        buf_offset.push_back(offset);
//        tuple->GetBuf_offset().push_back(offset);
      }
    }
    tuple->SetBuf_offset(buf_offset);
  }
}


void TileGroup::CopyTupleNewForUpdate(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id, ItemPointer old_location) {
//void TileGroup::CopyTupleNew(const Tuple *tuple, uint &tuple_slot_id) {
//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);
  (void)curr_thd_id;
  uint column_count;
  std::vector<uint> buf_offset;
//  tuple = const_cast<Tuple*> (tuple);

  AbstractPool* pool =
//      StorageManager::GetInstance()->GetPoolByThdId(curr_thd_id);
      StorageManager::GetPoolByThdId(curr_thd_id);
//        &IntelPool::GetInstance();
  auto old_tile_group = StorageManager::GetInstance()->GetTileGroup(old_location.block).get();
  Tile *old_tile = old_tile_group->GetTile(0);
  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    column_count = schema->GetColumnCount();

    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    const char *old_tuple_location = old_tile->GetTupleLocation(old_location.offset);

    const char *old_tuple_location_nvm;
    const char *old_field_location_nvm;


    PELOTON_ASSERT(tile_tuple_location);
    PELOTON_ASSERT(old_tuple_location);

    // NOTE:: Only a tuple wrapper
    Tuple tile_tuple(schema, tile_tuple_location);
    (void)tile_tuple;
    (void)tuple;
    if(FOR_NVM){
      char *tile_tuple_location_nvm = tile->GetTupleLocation_nvm(tuple_slot_id);
      old_tuple_location_nvm = old_tile->GetTupleLocation_nvm(old_location.offset);
      tile_tuple.SetData_nvm(tile_tuple_location_nvm);
    }

//    const char *data_dist = tuple->GetData();
    char *data_buf = tuple->GetDataAddress();
//    memcpy(tile_tuple.GetData(), data_dist, schema->GetLength());

//    tuple->GetBuf_offset().push_back(0);//最开始的offset为0
    buf_offset.push_back(0);//最开始的offset为0
    uint offset = 0;
    for (uint column_id = 0; column_id < column_count; column_id++) {
//      Value val = (tuple->GetValue(column_itr));
//      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());

      const char *old_field_location = old_tuple_location + schema->GetOffset(column_id);
      if(FOR_NVM){
        old_field_location_nvm = old_tuple_location_nvm + schema->GetOffset(column_id);
      }


      const Column &column = schema->GetColumn(column_id);
      auto typeId = column.GetType();
      //src是buf那边根据偏移量得到的值. int和float类型是4, double, char是8.
      //varchar现有1位或2位, 然后是具体的值. src只需要传入具体的值即可

      if (TypeId::VARCHAR == typeId) {

//        char *ptr = *reinterpret_cast<char *const *>(old_field_location);
//
//        char *value_location = tile_tuple.GetDataPtr(column_id);
//
//        *reinterpret_cast<const char **>(value_location) = ptr;

        if(FOR_NVM){
          char *ptr_nvm = *reinterpret_cast<char *const *>(old_field_location_nvm);
          char *value_location_nvm = tile_tuple.GetDataPtr_nvm(column_id);
          *reinterpret_cast<const char **>(value_location_nvm) = ptr_nvm;
        }

        uint var_len = 0;
        if(column.GetLength()<86){
          var_len = *(int8*)data_buf;
          offset+=1;
          data_buf += 1;
        }else {
          var_len = *(int16*)data_buf;
          offset+=2;
          data_buf += 2;
        }
        tile_tuple.SetValueMemAndNVM(column_id, data_buf, var_len, pool);
        data_buf += column.GetLength()*3;

        offset += column.GetLength()*3;
        buf_offset.push_back(offset);
//        tuple->GetBuf_offset().push_back(offset);

      }else if(TypeId::CHAR == typeId){
        uint var_len = column.GetLength();
        tile_tuple.SetValueMemAndNVM(column_id, data_buf, var_len, pool);
        data_buf += column.GetLength()*3;

        offset += column.GetLength()*3;
        buf_offset.push_back(offset);
      }else {
        tile_tuple.SetValueMemAndNVM(column_id, data_buf, -1, pool);
        data_buf += column.GetLength();

        offset += column.GetLength();
        buf_offset.push_back(offset);
//        tuple->GetBuf_offset().push_back(offset);
      }
    }
    tuple->SetBuf_offset(buf_offset);
  }
}

/**
 * Grab next slot (thread-safe) and fill in the tuple if tuple != nullptr
 *
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
uint TileGroup::InsertTuple(const Tuple *tuple) {
//uint TileGroup::InsertTuple(const Tuple *tuple) {
  uint tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);

  // No more slots
  if (tuple_slot_id == INVALID_OID) {
//    LOG_TRACE("Failed to get next empty tuple slot within tile group.");
    return INVALID_OID;
  }

  // if the input tuple is nullptr, then it means that the tuple with be filled
  // in
  // outside the function. directly return the empty slot.
  if (tuple == nullptr) {
    return tuple_slot_id;
  }

  // copy tuple.
  CopyTuple(tuple, tuple_slot_id);
//  CopyTupleNew(tuple, tuple_slot_id);//1119

  // Set MVCC info
  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_slot_id) ==
            INVALID_TXN_ID);
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
  PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);
  return tuple_slot_id;
}

uint TileGroup::InsertTupleNew(Tuple *tuple, uint curr_thd_id) {
//uint TileGroup::InsertTuple(const Tuple *tuple) {
  uint tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();

//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);

  // No more slots
  if (tuple_slot_id == INVALID_OID) {
//    LOG_TRACE("Failed to get next empty tuple slot within tile group.");
    return INVALID_OID;
  }

  // if the input tuple is nullptr, then it means that the tuple with be filled
  // in
  // outside the function. directly return the empty slot.
  if (tuple == nullptr) {
    return tuple_slot_id;
  }

  // copy tuple.
//  CopyTuple(tuple, tuple_slot_id);

  CopyTupleNew(tuple, tuple_slot_id, curr_thd_id);//1119

  // Set MVCC info
  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_slot_id) ==
                 INVALID_TXN_ID);
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
  PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);
  return tuple_slot_id;
}

uint TileGroup::InsertTupleFromRecovery(Tuple *tuple, uint curr_thd_id) {
  uint tuple_slot_id = tile_group_header->GetNextEmptyTupleSlot();
  // No more slots
  if (tuple_slot_id == INVALID_OID) {
    return INVALID_OID;
  }

  // if the input tuple is nullptr, then it means that the tuple with be filled
  // in
  // outside the function. directly return the empty slot.
  if (tuple == nullptr) {
    return tuple_slot_id;
  }

  CopyTupleFromRecovery(tuple, tuple_slot_id, curr_thd_id);//1119

  PELOTON_ASSERT(tile_group_header->GetTransactionId(tuple_slot_id) ==
                 INVALID_TXN_ID);
  PELOTON_ASSERT(tile_group_header->GetBeginCommitId(tuple_slot_id) == MAX_CID);
  PELOTON_ASSERT(tile_group_header->GetEndCommitId(tuple_slot_id) == MAX_CID);
  return tuple_slot_id;
}

/**
 * Grab specific slot and fill in the tuple
 * Used by recovery
 * Returns slot where inserted (INVALID_ID if not inserted)
 */
uint TileGroup::InsertTupleFromRecovery(cid_t commit_id, uint tuple_slot_id, const Tuple *tuple) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  // No more slots
  if (status == false) return INVALID_OID;

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }

//  LOG_TRACE("Tile Group Id :: %u status :: %u out of %u slots ", tile_group_id,
//            tuple_slot_id, num_tuple_slots_);

  uint tile_column_count;
  uint column_itr = 0;

  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    tile_column_count = schema->GetColumnCount();
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PELOTON_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    Tuple tile_tuple(schema, tile_tuple_location);

    for (uint tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      Value val = (tuple->GetValue(column_itr));
      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INITIAL_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

uint TileGroup::DeleteTupleFromRecovery(cid_t commit_id, uint tuple_slot_id) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }
  // No more slots
  if (status == false) return INVALID_OID;
  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);
  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

uint TileGroup::UpdateTupleFromRecovery(cid_t commit_id, uint tuple_slot_id,
                                         ItemPointer new_location) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  tile_group_header->GetHeaderLock().Lock();

  cid_t current_begin_cid = tile_group_header->GetBeginCommitId(tuple_slot_id);
  if (current_begin_cid != MAX_CID && current_begin_cid > commit_id) {
    tile_group_header->GetHeaderLock().Unlock();
    return tuple_slot_id;
  }

  // No more slots
  if (status == false) return INVALID_OID;

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INVALID_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetNextItemPointer(tuple_slot_id, new_location);
  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

// the same as InsertTupleFromRecovery, but not need to compare cid
uint TileGroup::InsertTupleFromCheckpoint(uint tuple_slot_id, const Tuple *tuple, cid_t commit_id) {
  auto status = tile_group_header->GetEmptyTupleSlot(tuple_slot_id);

  // No more slots
  if (status == false) return INVALID_OID;

  tile_group_header->GetHeaderLock().Lock();

  uint tile_column_count;
  uint column_itr = 0;

  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
    Tile *tile = GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    tile_column_count = schema->GetColumnCount();
    char *tile_tuple_location = tile->GetTupleLocation(tuple_slot_id);
    PELOTON_ASSERT(tile_tuple_location);

    // NOTE:: Only a tuple wrapper
    Tuple tile_tuple(schema, tile_tuple_location);

    for (uint tile_column_itr = 0; tile_column_itr < tile_column_count;
         tile_column_itr++) {
      Value val = (tuple->GetValue(column_itr));
      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());
      column_itr++;
    }
  }

  // Set MVCC info
  tile_group_header->SetTransactionId(tuple_slot_id, INITIAL_TXN_ID);
  tile_group_header->SetBeginCommitId(tuple_slot_id, commit_id);
  tile_group_header->SetEndCommitId(tuple_slot_id, MAX_CID);
  tile_group_header->SetNextItemPointer(tuple_slot_id, INVALID_ITEMPOINTER);

  tile_group_header->GetHeaderLock().Unlock();
  return tuple_slot_id;
}

Value TileGroup::GetValue(uint tuple_id, uint column_id) {
//  PELOTON_ASSERT(tuple_id < GetNextTupleSlot());
  uint tile_column_id, tile_offset;
  tile_group_layout_->LocateTileAndColumn(column_id, tile_offset,
                                          tile_column_id);
  return GetTile(tile_offset)->GetValue(tuple_id, tile_column_id);
}


void TileGroup::SetValue(Value &value, uint tuple_id,
                         uint column_id) {
//  PELOTON_ASSERT(tuple_id < GetNextTupleSlot());
  uint tile_column_id, tile_offset;
  tile_group_layout_->LocateTileAndColumn(column_id, tile_offset,
                                          tile_column_id);
  GetTile(tile_offset)->SetValue(value, tuple_id, tile_column_id);
}


std::shared_ptr<Tile> TileGroup::GetTileReference(
    const uint tile_offset) const {
  PELOTON_ASSERT(tile_offset < tile_count_);
  return tiles[tile_offset];
}

void TileGroup::Sync() {
  // Sync the tile group data by syncing all the underlying tiles
  for (auto tile : tiles) {
    tile->Sync();
  }
}

//===--------------------------------------------------------------------===//
// Utilities
//===--------------------------------------------------------------------===//

const std::string TileGroup::GetInfo() const {
  std::ostringstream os;
//
//  os << GETINFO_DOUBLE_STAR << " TILE GROUP[#" << tile_group_id << "] "
//     << GETINFO_DOUBLE_STAR << std::endl;
//  os << "Database[" << database_id << "] // ";
//  os << "Table[" << table_id << "] " << std::endl;
//  os << "Layout[" << *tile_group_layout_ << std::endl;
//  os << (*tile_group_header) << std::endl;

//  for (uint tile_itr = 0; tile_itr < tile_count_; tile_itr++) {
//    Tile *tile = GetTile(tile_itr);
//    if (tile != nullptr) {
//      os << std::endl << (*tile);
//    }
//  }

  // auto header = GetHeader();
  // if (header != nullptr) os << (*header);
  return StringUtil::Prefix(StringBoxUtil::Box(os.str()),
                                     GETINFO_SPACER);
}

