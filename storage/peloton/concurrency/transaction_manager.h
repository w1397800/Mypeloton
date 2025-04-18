//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/include/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <atomic>
#include <unordered_map>
#include <list>
#include <utility>

#include "storage/peloton/store/tile_group_header.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/concurrency/epoch_manager_factory.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/catalog/manager.h"
#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/traffic_cop/traffic_cop.h"
/**
 * @brief      Class for item pointer.
 */
//class ItemPointer;

//class DataTable;
//class TileGroupHeader;

//class Manager;


/**
 * @brief      Class for transaction context.
 */
//class TransactionContext;

/**
 * @brief      Class for transaction manager.
 */
class TransactionManager {
 public:
  TransactionManager() {}

  /**
   * @brief      Destroys the object.
   */
  virtual ~TransactionManager() {}

  enum bool_ex{
    True = 0,
    False = 1,
    Retry = 2
  };
  void Init(const ProtocolType protocol,
            const IsolationLevelType isolation, 
            const ConflictAvoidanceType conflict) {
    protocol_ = protocol;
    isolation_level_ = isolation;
    conflict_avoidance_ = conflict;
  }

  /**
   * Used for avoiding concurrent inserts.
   *
   * @param      current_txn        The current transaction
   * @param[in]  position_ptr  The position pointer
   *
   * @return     True if occupied, False otherwise.
   */
  bool IsOccupied(
      TransactionContext *const current_txn,
      const void *position_ptr);

  /**
   * @brief      Determines if visible.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   * @param[in]  type               The type
   *
   * @return     True if visible, False otherwise.
   */
  VisibilityType IsVisible(
      TransactionContext *const current_txn,
      const TileGroupHeader *const tile_group_header,
      const uint &tuple_id,
      const VisibilityIdType type = VisibilityIdType::READ_ID);

  /**
   * Test whether the current transaction is the owner of this tuple.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if owner, False otherwise.
   */
  virtual bool IsOwner(
      TransactionContext *const current_txn,
      const TileGroupHeader *const tile_group_header,
      const uint &tuple_id) = 0;

  /**
   * This method tests whether any other transaction has owned this version.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if owned, False otherwise.
   */
  virtual bool IsOwned(
      TransactionContext *const current_txn,
      const TileGroupHeader *const tile_group_header,
      const uint &tuple_id) = 0;

  /**
   * Test whether the current transaction has created this version of the tuple.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if written, False otherwise.
   */
  virtual bool IsWritten(
    TransactionContext *const current_txn,
    const TileGroupHeader *const tile_group_header,
    const uint &tuple_id) = 0;

  /**
   * Test whether it can obtain ownership.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if ownable, False otherwise.
   */
  virtual bool IsOwnable(
      TransactionContext *const current_txn,
      const TileGroupHeader *const tile_group_header,
      const uint &tuple_id) = 0;

  /**
   * Used to acquire ownership of a tuple for a transaction.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   *
   * @return     True if success, False otherwise.
   */
  virtual bool AcquireOwnership(
      TransactionContext *const current_txn,
      const TileGroupHeader *const tile_group_header, 
      const uint &tuple_id) = 0;

  /**
   * Used by executor to yield ownership after the acquired it.
   *
   * @param      current_txn        The current transaction
   * @param[in]  tile_group_header  The tile group header
   * @param[in]  tuple_id           The tuple identifier
   */
  virtual void YieldOwnership(
      TransactionContext *const current_txn,
      // const uint &tile_group_id, 
      const TileGroupHeader *const tile_group_header, 
      const uint &tuple_id) = 0;

  /**
   * The index_entry_ptr is the address of the head node of the version chain,
   * which is directly pointed by the primary index.
   *
   * @param      current_txn        The current transaction
   * @param[in]  location         The location
   * @param      index_entry_ptr  The index entry pointer
   */
  virtual void PerformInsert(TransactionContext *const current_txn,
                             const ItemPointer &location, 
                             ItemPointer *index_entry_ptr = nullptr) = 0;

//  virtual bool PerformRead(TransactionContext *const current_txn,
//                             const ItemPointer &location,
//                             TileGroupHeader *tile_group_header,
//                             bool acquire_ownership) = 0;

  virtual bool_ex PerformRead(TransactionContext *const current_txn,
                              const ItemPointer &location,
                              TileGroupHeader *tile_group_header,
                              bool acquire_ownership) = 0;


  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &old_location,
                             const ItemPointer &new_location) = 0;

  virtual void PerformUpdate(TransactionContext *const current_txn,
                             const ItemPointer &location) = 0;

  virtual void PerformDelete(TransactionContext *const current_txn,
                             const ItemPointer &location) = 0;

  /**
   * @brief      Sets the transaction result.
   *
   * @param      current_txn  The current transaction
   * @param[in]  result       The result
   */
  void SetTransactionResult(TransactionContext *const current_txn, const ResultType result) {
    current_txn->SetResult(result);
  }

  TransactionContext *BeginTransaction(TrafficCop *cop, const IsolationLevelType type) {
    return BeginTransaction(cop, 0, type, false);
  }

  TransactionContext *BeginTransaction(TrafficCop *cop, const size_t thread_id = 0,
                                const IsolationLevelType type = isolation_level_,
                                bool read_only = false);

  /**
   * @brief      Ends a transaction.
   *
   * @param      current_txn  The current transaction
   */
  void EndTransaction(TransactionContext *current_txn);

  /**
   * @brief     Record transaction results
   * @param[in] current_txn     The current transaction
   * @warning   Assumes stats_mode != INVALID
   */
  void RecordTransactionStats(
      const TransactionContext *const current_txn) const;

  // virtual void PrepareTransaction(TransactionContext * const current_txn) = 0;
  virtual ResultType CommitTransaction(TransactionContext *const current_txn) = 0;

  virtual ResultType AbortTransaction(TransactionContext *const current_txn) = 0;
  virtual ResultType AbortTransaction(TransactionContext *const current_txn,
                                      bool is_entire, bool is_fail_aft_del) = 0;

  /**
   * This function generates the maximum commit id of committed transactions.
   * please note that this function only returns a "safe" value instead of a
   * precise value.
   *
   * @return     The expired cid.
   */
  cid_t GetExpiredCid() {
    return EpochManagerFactory::GetInstance().GetExpiredCid();
  }

  /**
   * @brief      Gets the isolation level.
   *
   * @return     The isolation level.
   */
  IsolationLevelType GetIsolationLevel() {
    return isolation_level_;
  }

 protected:
  static ProtocolType protocol_;
  static IsolationLevelType isolation_level_;
  static ConflictAvoidanceType conflict_avoidance_;

};
