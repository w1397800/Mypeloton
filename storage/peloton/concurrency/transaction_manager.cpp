//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/concurrency/transaction_manager.h"
#include "storage/peloton/gc/gc_manager_factory.h"

#include "storage/peloton/catalog/manager.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/function/date_functions.h"
//#include "storage/peloton/gc/gc_manager_factory.h"
#include "storage/peloton/settings/settings_manager.h"
//#include "storage/peloton/statistics/stats_aggregator.h"
#include "storage/peloton/store/tile_group.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/logging/log_manager.h"


ProtocolType TransactionManager::protocol_ = ProtocolType::TIMESTAMP_ORDERING;
IsolationLevelType TransactionManager::isolation_level_ =
    IsolationLevelType::SERIALIZABLE;
ConflictAvoidanceType TransactionManager::conflict_avoidance_ =
    ConflictAvoidanceType::ABORT;

TransactionContext *TransactionManager::BeginTransaction(
    TrafficCop *cop, const size_t thread_id,
    const IsolationLevelType type, bool read_only) {

  TransactionContext *txn = nullptr;

  if (type == IsolationLevelType::SNAPSHOT) {
    // transaction processing with decentralized epoch manager
    // the DBMS must acquire
    cid_t read_id = EpochManagerFactory::GetInstance().EnterEpoch(
        thread_id, TimestampType1::SNAPSHOT_READ);

    if (protocol_ == ProtocolType::TIMESTAMP_ORDERING) {
      cid_t commit_id = EpochManagerFactory::GetInstance().EnterEpoch(
          thread_id, TimestampType1::COMMIT);

      txn = new TransactionContext(thread_id, type, read_id, commit_id);
    } else {
      txn = new TransactionContext(thread_id, type, read_id);
    }

  } else {
    // if the isolation level is set to:
    // - SERIALIZABLE, or
    // - REPEATABLE_READS, or
    // - READ_COMMITTED.
    // transaction processing with decentralized epoch manager
    cid_t read_id = EpochManagerFactory::GetInstance().EnterEpoch(
        thread_id, TimestampType1::READ);
    txn = new TransactionContext(thread_id, type, read_id);
  }

  if (read_only) {
    txn->SetReadOnly();
  }

  txn->SetTimestamp(DateFunctions::Now());


  txn->SetTrafficCop(cop);

  //LOGGING
  auto &log_manager = LogManager::GetInstance();
  log_manager.PrepareLogging(txn);

  return txn;
}

void TransactionManager::EndTransaction(TransactionContext *current_txn) {
  // LOGGING
  auto &log_manager = LogManager::GetInstance();
  if (current_txn->GetResult() == ResultType::SUCCESS && !current_txn->GetReadWriteSet().empty()) {
    log_manager.LogCommitTransaction(current_txn);
  } else {
    log_manager.DoneLogging(current_txn);
  }


    // pass transaction context to garbage collector
    if (GCManagerFactory::GetGCType() == GarbageCollectionType::ON) {
      GCManagerFactory::GetInstance().RecycleTransaction(current_txn);
    } else {
      delete current_txn;
    }
    //(void)current_txn;
    current_txn = nullptr;
    //delete current_txn;
}

// this function checks whether a concurrent transaction is inserting the same
// tuple
// that is to-be-inserted by the current transaction.
bool TransactionManager::IsOccupied(TransactionContext *const current_txn,
                                    const void *position_ptr) {
  ItemPointer &position = *((ItemPointer *)position_ptr);

  auto tile_group_header =
      StorageManager::GetInstance()->GetTileGroup(position.block)->GetHeader();
  auto tuple_id = position.offset;

  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);
  // the tuple has already been committed.
  bool activated = (current_txn->GetReadId() >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (current_txn->GetReadId() >= tuple_end_cid);

  // there are exactly two versions that can be owned by a transaction.
  // unless it is an insertion/select for update.
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      PELOTON_ASSERT(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted one.
      return true;
    } else if (current_txn->GetRWType(position) == RWType::READ_OWN) {
      // the ownership is from a select-for-update read operation
      return true;
    } else {
      // the older version is not visible.
      return false;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // uncommitted version.
        if (tuple_end_cid == INVALID_CID) {
          // dirty delete is invisible
          return false;
        } else {
          // dirty update or insert is visible
          return true;
        }
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return true;
        } else {
          return false;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  }
}

// this function checks whether a version is visible to current transaction.
VisibilityType TransactionManager::IsVisible(
    TransactionContext *const current_txn,
    const TileGroupHeader *const tile_group_header,
    const uint &tuple_id, const VisibilityIdType type) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  uint tile_group_id = tile_group_header->GetTileGroup()->GetTileGroupId();

  // the tuple has already been owned by the current transaction.
  bool own = (current_txn->GetTransactionId() == tuple_txn_id);

  cid_t txn_vis_id;

//  return VisibilityType::OK;

  if (type == VisibilityIdType::READ_ID) {
    txn_vis_id = current_txn->GetReadId();
  } else {
    PELOTON_ASSERT(type == VisibilityIdType::COMMIT_ID);
    txn_vis_id = current_txn->GetCommitId();
  }

  // the tuple has already been committed.
  bool activated = (txn_vis_id >= tuple_begin_cid);
  // the tuple is not visible.
  bool invalidated = (txn_vis_id >= tuple_end_cid);

  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    if (activated && !invalidated) {
      // deleted tuple
      return VisibilityType::DELETED;
    } else {
      // aborted tuple
      return VisibilityType::INVISIBLE;
    }
  }

  // there are exactly two versions that can be owned by a transaction,
  // unless it is an insertion/select-for-update
  if (own == true) {
    if (tuple_begin_cid == MAX_CID && tuple_end_cid != INVALID_CID) {
      PELOTON_ASSERT(tuple_end_cid == MAX_CID);
      // the only version that is visible is the newly inserted/updated one.
      return VisibilityType::OK;
    } else if (current_txn->GetRWType(ItemPointer(tile_group_id, tuple_id)) ==
               RWType::READ_OWN) {
      // the ownership is from a select-for-up`date read operation
      return VisibilityType::OK;
    } else if (tuple_end_cid == INVALID_CID) {
      // tuple being deleted by current txn
      return VisibilityType::DELETED;
    } else {
      // old version of the tuple that is being updated by current txn
      return VisibilityType::INVISIBLE;
    }
  } else {
    if (tuple_txn_id != INITIAL_TXN_ID) {
      // if the tuple is owned by other transactions.
      if (tuple_begin_cid == MAX_CID) {
        // in this protocol, we do not allow cascading abort. so never read an
        // uncommitted version.
        return VisibilityType::INVISIBLE;
      } else {
        // the older version may be visible.
        if (activated && !invalidated) {
          return VisibilityType::OK;
        } else {
          return VisibilityType::INVISIBLE;
        }
      }
    } else {
      // if the tuple is not owned by any transaction.
      if (activated && !invalidated) {
        return VisibilityType::OK;
      } else {
        return VisibilityType::INVISIBLE;
      }
    }
  }
}
