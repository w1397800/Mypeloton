//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.h
//
// Identification: src/include/storage/data_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef DATA_TABLE
//#define DATA_TABLE

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>

#include "storage/peloton/common/item_pointer.h"
//#include "common/platform.h"
#include "storage/peloton/index/index.h"
//#include "storage/peloton/util/lock_free_array.h"
#include "storage/peloton/util/lock_free_array.h"
#include "abstract_table.h"
//#include "indirection_array.h"
#include "layout.h"
#include "storage/peloton/common/abstract_tuple.h"
//#include "trigger/trigger.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//
#include "storage/peloton/catalog/manager.h"

extern std::vector<uint> sdbench_column_ids;


//namespace tuning {
//class Sample;
//}  // namespace indextuner
//
//namespace catalog {
//class ForeignKey;
//class Catalog;
//}  // namespace catalog
//
//namespace executor {
//class ExecutorContext;
//}  // namespace executor
//
//namespace index {
//class Index;
//}  // namespace index
//
//namespace logging {
//class LogManager;
//}  // namespace logging
//
//namespace concurrency {
//class TransactionContext;
//}  // namespace concurrency


class Tuple;
class TileGroup;
class IndirectionArray;

//===--------------------------------------------------------------------===//
// DataTable
//===--------------------------------------------------------------------===//

/**
 * Represents a group of tile groups logically vertically contiguous.
 *
 * <Tile Group 1>
 * <Tile Group 2>
 * ...
 * <Tile Group n>
 *
 */
class DataTable : public AbstractTable {
  friend class TileGroup;
  friend class TileGroupFactory;
//  friend class TableFactory;
  friend class Catalog;
//  friend class logging::LogManager;

  DataTable() = delete;
  DataTable(DataTable const &) = delete;

 public:
  // Table constructor
  DataTable(Schema *schema, const std::string &table_name,
            const uint &database_oid, const uint &table_oid,
            const size_t &tuples_per_tilegroup, const bool own_schema,
            const bool adapt_table, const bool is_catalog = false,
            const LayoutType layout_type = LayoutType::ROW);

  ~DataTable();

  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//
  // insert an empty version in table. designed for delete operation.
  ItemPointer InsertEmptyVersion();

  // these two functions are designed for reducing memory allocation by
  // performing in-place update.
  // in the update executor, we first acquire a version slot from the data
  // table, and then
  // copy the content into the version. after that, we need to check constraints
  // and then install the version
  // into all the corresponding indexes.
  ItemPointer AcquireVersion();
  ItemPointer AcquireVersion(uint curr_thd_id);

  // install an version in table. designed for update operation.
  // as we implement logical-pointer indexing mechanism, targets_ptr is
  // required.//update相关
  bool InstallVersion(const AbstractTuple *tuple,
//                      const TargetList *targets_ptr,
                      const uint *targets_ptr,
                      TransactionContext *transaction,
                      ItemPointer *index_entry_ptr);

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  ItemPointer InsertTuple(const Tuple *tuple,
                          TransactionContext *transaction,
                          ItemPointer **index_entry_ptr = nullptr,
                          bool check_fk = true);


  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  ItemPointer InsertTuple(const Tuple *tuple);
//
//  // Insert tuple with ItemPointer provided explicitly
  bool InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                   TransactionContext *transaction,
                   ItemPointer **index_entry_ptr,
                   bool check_fk = true);

  bool InsertTupleNew(Tuple *tuple, ItemPointer location,
                   TransactionContext *transaction,
                   ItemPointer **index_entry_ptr,
                      uint curr_thd_id,
                      bool check_fk = true);

  ItemPointer InsertTupleNew(Tuple *tuple,
                             TransactionContext *transaction,
                             uint curr_thd_id,
                             ItemPointer **index_entry_ptr = nullptr,
                             bool check_fk = true);

  bool InsertInIndexesNew(const Tuple *tuple, ItemPointer location,
                          TransactionContext *transaction,
                          ItemPointer **index_entry_ptr,
                          uint curr_thd_id);


  ItemPointer *InsertIndexFromRecovery(const Tuple *tuple, ItemPointer location, uint curr_thd_id);

//with tid
  ItemPointer InsertTuple(const Tuple *tuple,
                          TransactionContext *transaction,
                          uint curr_thd_id,
                          ItemPointer **index_entry_ptr = nullptr,
                          bool check_fk = true);

//with tid
  bool InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                   TransactionContext *transaction,
                   ItemPointer **index_entry_ptr,
                   uint curr_thd_id,
                   bool check_fk = true);

//with tid
  bool InsertInIndexes(const AbstractTuple *tuple, ItemPointer location,
                       TransactionContext *transaction,
                       ItemPointer **index_entry_ptr,
                       uint curr_thd_id);

//
//  //===--------------------------------------------------------------------===//
//  // TILE GROUP
//  //===--------------------------------------------------------------------===//
//
//  // coerce into adding a new tile group with a tile group id
  void AddTileGroupWithOidForRecovery(const uint &tile_group_id);
//
  void AddTileGroup(const std::shared_ptr<TileGroup> &tile_group);
//
//  // Offset is a 0-based number local to the table
  std::shared_ptr<TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const;
//
//  // ID is the global identifier in the entire DBMS
  std::shared_ptr<TileGroup> GetTileGroupById(
      const uint &tile_group_id) const;
//
  size_t GetTileGroupCount() const;
//
//  // Get a tile group with given layout
  TileGroup *GetTileGroupWithLayout(std::shared_ptr<const Layout> layout);
//
//  //===--------------------------------------------------------------------===//
//  // TRIGGER
//  //===--------------------------------------------------------------------===//
//
//  void AddTrigger(trigger::Trigger new_trigger);
//
//  int GetTriggerNumber();
//
//  trigger::Trigger *GetTriggerByIndex(int n);
//
//  trigger::TriggerList *GetTriggerList();
//
//  void UpdateTriggerListFromCatalog(TransactionContext *txn);
//
//  //===--------------------------------------------------------------------===//
//  // INDEX
//  //===--------------------------------------------------------------------===//
//
  void AddIndex(Index* index);
//
//  // Throw CatalogException if not such index is found
  Index* GetIndexWithOid(const uint &index_oid);
  Index* GetIndexWithName(std::string index_name);
//
  void DropIndexWithOid(const uint &index_oid);
//
//  void DropIndexes();
//
  Index* GetIndex(const uint &index_offset);
//
  std::set<uint> GetIndexAttrs(const uint &index_offset) const;
//
  uint GetIndexCount() const;
//
//  uint GetValidIndexCount() const;
//
//  const std::vector<std::set<uint>> &GetIndexColumns() const {
//    return indexes_columns_;
//  }
//
//  //===--------------------------------------------------------------------===//
//  // FOREIGN KEYS
//  //===--------------------------------------------------------------------===//
//
//  bool CheckForeignKeySrcAndCascade(
//      Tuple *prev_tuple, Tuple *new_tuple,
//      TransactionContext *transaction,
//      executor::ExecutorContext *context, bool is_update);
//
//  //===--------------------------------------------------------------------===//
//  // TRANSFORMERS
//  //===--------------------------------------------------------------------===//
//
//  TileGroup *TransformTileGroup(const uint &tile_group_offset,
//                                         const double &theta);
//
//  //===--------------------------------------------------------------------===//
//  // STATS
//  //===--------------------------------------------------------------------===//
//
  void IncreaseTupleCount(const size_t &amount);

  void IncreaseRealTupleCount(const size_t &amount);
//
  void DecreaseTupleCount(const size_t &amount);
//
  void SetTupleCount(const size_t &num_tuples);
//
  size_t GetTupleCount() const;
  size_t GetRealTupleCount() const;
//
  bool IsDirty() const;
//
  void ResetDirty();
//
//  //===--------------------------------------------------------------------===//
//  // LAYOUT TUNER
//  //===--------------------------------------------------------------------===//
//
//  void RecordLayoutSample(const tuning::Sample &sample);
//
//  std::vector<tuning::Sample> GetLayoutSamples();
//
//  void ClearLayoutSamples();
//
//  void SetDefaultLayout(std::shared_ptr<const Layout> new_layout) {
//    PELOTON_ASSERT(new_layout->GetColumnCount() == schema->GetColumnCount());
//    default_layout_ = new_layout;
//  }
//
//  void ResetDefaultLayout(LayoutType type = LayoutType::ROW) {
//    PELOTON_ASSERT((type == LayoutType::ROW) || (type == LayoutType::COLUMN));
//    default_layout_ = std::shared_ptr<const Layout>(
//        new const Layout(schema->GetColumnCount(), type));
//  }
//
//  const std::shared_ptr<const Layout> GetDefaultLayout() const {
//    return default_layout_;
//  }
//
//  //===--------------------------------------------------------------------===//
//  // INDEX TUNER
//  //===--------------------------------------------------------------------===//
//
//  void RecordIndexSample(const tuning::Sample &sample);
//
//  std::vector<tuning::Sample> GetIndexSamples();
//
//  void ClearIndexSamples();
//
//  //===--------------------------------------------------------------------===//
//  // UTILITIES
//  //===--------------------------------------------------------------------===//
//
//  // deprecated, use TableCatalog::GetInstance()->GetTableName()
  inline std::string GetName() const { return (table_name); }
//
//  // deprecated, use TableCatalog::GetInstance()->GetDatabaseOid()
  inline uint GetDatabaseOid() const { return (database_oid); }
//
//  // try to insert into all indexes.
//  // the last argument is the index entry in primary index holding the new
//  // tuple.
  bool InsertInIndexes(const AbstractTuple *tuple, ItemPointer location,
                       TransactionContext *transaction,
                       ItemPointer **index_entry_ptr);
//
  inline static size_t GetActiveTileGroupCount() {
    return default_active_tilegroup_count_;
  }
//
  static void SetActiveTileGroupCount(const size_t active_tile_group_count) {
    default_active_tilegroup_count_ = active_tile_group_count;
  }
//
  inline static size_t GetActiveIndirectionArrayCount() {
    return default_active_indirection_array_count_;
  }
//
  static void SetActiveIndirectionArrayCount(
      const size_t active_indirection_array_count) {
    default_active_indirection_array_count_ = active_indirection_array_count;
  }
//
//  // Claim a tuple slot in a tile group
  ItemPointer GetEmptyTupleSlot(const Tuple *tuple);
  ItemPointer GetEmptyTupleSlotNew(Tuple *tuple);
  ItemPointer GetEmptyTupleSlotForThd(Tuple *tuple, uint curr_thd_id);
  ItemPointer GetEmptyTupleSlotForRecovery(Tuple *tuple, uint curr_thd_id);
//
//  hash_t Hash() const;
//
//  bool Equals(const DataTable &other) const;
//  bool operator==(const DataTable &rhs) const;
//  bool operator!=(const DataTable &rhs) const { return !(*this == rhs); }
//
// protected:
//  //===--------------------------------------------------------------------===//
//  // INTEGRITY CHECKS
//  //===--------------------------------------------------------------------===//
//
  bool CheckNotNulls(const AbstractTuple *tuple, uint column_idx) const;
  //  bool MultiCheckNotNulls(const Tuple *tuple,
  //                          std::vector<uint> cols) const;

  // bool CheckExp(const Tuple *tuple, uint column_idx,
  //              std::pair<ExpressionType, type::Value> exp) const;
  // bool CheckUnique(const Tuple *tuple, uint column_idx) const;

  // bool CheckExp(const Tuple *tuple, uint column_idx) const;

  bool CheckConstraints(const AbstractTuple *tuple) const;

  // add a tile group to the table
  uint AddDefaultTileGroup();
  // add a tile group to the table. replace the active_tile_group_id-th active
  // tile group.
  uint AddDefaultTileGroup(const size_t &active_tile_group_id);
//
  uint AddDefaultIndirectionArray(const size_t &active_indirection_array_id);

  // Drop all tile groups of the table. Used by recovery
  void DropTileGroups();

  //===--------------------------------------------------------------------===//
  // INDEX HELPERS
  //===--------------------------------------------------------------------===//

  bool InsertInSecondaryIndexes(const AbstractTuple *tuple,
                                const uint *targets_ptr,
//                                const TargetList *targets_ptr,
                                TransactionContext *transaction,
                                ItemPointer *index_entry_ptr);

  // check the foreign key constraints
  bool CheckForeignKeyConstraints(const AbstractTuple *tuple,
                                  TransactionContext *transaction);

  //===--------------------------------------------------------------------===//
  // LAYOUT HELPERS
  //===--------------------------------------------------------------------===//

  // Set the current_layout_oid_ to the given value if the current value
  // is less than new_layout_oid. Return true on success.
  // To be used for recovery.
  bool SetCurrentLayoutOid(uint new_layout_oid);

  // Performs an atomic increment on the current_layout_oid_
  // and returns the incremented value.
  uint GetNextLayoutOid() { return ++current_layout_oid_; }

  void SetPk_cols(std::vector<uint> pk_cols){pk_cols_ = pk_cols;}

  std::vector<uint> GetPk_cols(){return pk_cols_;}

  void SetInit_buf(uint buf){init_buf = buf;}
  uint GetInit_buf() { return init_buf; }

  void SetMove_pos(uint16 p){move_pos = p;}
  uint16 GetMove_pos() { return move_pos; }

  void SetPrimaryIndexName(std::string name) {primary_index_name_ = name;}

  void AddUniqueIndexInfo(std::string index_name, vector<uint> cols) {
    unique_index_info_.emplace_back(index_name, cols);
  }

  std::string GetPrimaryIndexName() {return primary_index_name_;}

  std::vector<
      std::pair<std::string, std::vector<uint>>> GetUniqueIndexInfo() {return unique_index_info_;}

  void SetUniqueIndexInfo(std::vector<std::pair<std::string, std::vector<uint>>> info) {
    unique_index_info_ = info;
  }

  void ResetActiveTileGroupsAfterRecovery();
 private:
  //===--------------------------------------------------------------------===//
  // STATIC MEMBERS
  //===--------------------------------------------------------------------===//
  static size_t default_active_tilegroup_count_;
  static size_t default_active_indirection_array_count_;

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  size_t active_tilegroup_count_;
  size_t active_indirection_array_count_;

  const uint database_oid;

  // deprecated, use TableCatalog::GetInstance()->GetTableName()
  std::string table_name;

  // number of tuples allocated per tilegroup
  size_t tuples_per_tilegroup_;

  // TILE GROUPS
  LockFreeArray<uint> tile_groups_;
//
  std::vector<std::shared_ptr<TileGroup>> active_tile_groups_;
//
  std::atomic<size_t> tile_group_count_ = ATOMIC_VAR_INIT(0);
//
//  // INDIRECTIONS
  std::vector<std::shared_ptr<IndirectionArray>>
      active_indirection_arrays_;

  // data table mutex
  std::mutex data_table_mutex_;

  // INDEXES
  std::vector<Index*> indexes_;
//  LockFreeArray<std::shared_ptr<uint>> indexes_;

  // columns present in the indexes
  std::vector<std::set<uint>> indexes_columns_;

  // # of tuples. must be atomic as multiple transactions can perform insert
  // concurrently.
  std::atomic<size_t> number_of_tuples_ = ATOMIC_VAR_INIT(0);

  std::atomic<size_t> real_count = ATOMIC_VAR_INIT(0);


  // 用于索引恢复时，DataTable::InsertIndexFromRecovery 中分配 indirection_offset 时对 cache 友好
  // 按道理它的值不重要，不影响索引的正确性
  std::atomic<size_t> number_of_indexs_ = ATOMIC_VAR_INIT(0);

//
//  // dirty flag. for detecting whether the tile group has been used.
  bool dirty_ = false;
//
//  // Last used layout_oid. Used while creating new layouts
//  // Initialized to COLUMN_STORE_OID since its the highest predefined value.
  std::atomic<uint> current_layout_oid_;
//
//  //===--------------------------------------------------------------------===//
//  // TUNING MEMBERS
//  //===--------------------------------------------------------------------===//
//
//  // adapt table
  bool adapt_table_ = true;
//
//  // samples for layout tuning
//  std::vector<tuning::Sample> layout_samples_;
//
//  // layout samples mutex
//  std::mutex layout_samples_mutex_;
//
//  // samples for layout tuning
//  std::vector<tuning::Sample> index_samples_;
//
//  // index samples mutex
//  std::mutex index_samples_mutex_;
//
  static uint invalid_tile_group_id;


  //当tuple的长度小于某一个值的时候,
  //需要给buf一个初始值, 在该"初始值后+数据" 才能正确的显示 .
  //当tuple的长度大于某值时, rnd_next传进来的buf初始值为"".
  uint init_buf = 9999;//
  uint16 move_pos = 0;
  //
//  // trigger list
//  std::unique_ptr<trigger::TriggerList> trigger_list_;
  int trigger_list_;


//===--------------------------------------------------------------------===//
// 索引相关
//===--------------------------------------------------------------------===//

  std::string primary_index_name_;
  std::vector<uint> pk_cols_;//主键列们

  // 第一个为索引名，第二个为索引列们
  std::vector<
      std::pair<std::string, std::vector<uint>>> unique_index_info_;

};

//#endif