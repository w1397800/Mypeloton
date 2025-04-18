//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.cpp
//
// Identification: src/concurrency/transaction_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/concurrency/transaction_context.h"

#include <sstream>

#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/common/platform.h"
//#include "storage/peloton/trigger/trigger.h"

#include <chrono>
#include <iomanip>
#include <thread>


/*
 * TransactionContext state transition:
 *                r           r/ro            u/r/ro
 *              +--<--+     +---<--+        +---<--+
 *           r  |     |     |      |        |      |     d
 *  (init)-->-- +-> Read  --+-> Read Own ---+--> Update ---> Delete (final)
 *                    |   ro             u  |
 *                    |                     |
 *                    +----->--------->-----+
 *                              u
 *              r/ro/u
 *            +---<---+
 *         i  |       |     d
 *  (init)-->-+---> Insert ---> Ins_Del (final)
 *
 *    r : read
 *    ro: read_own
 *    u : update
 *    d : delete
 *    i : insert
 */

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id) {
  Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                                       const IsolationLevelType isolation,
                                       const cid_t &read_id,
                                       const cid_t &commit_id) {
  Init(thread_id, isolation, read_id, commit_id);
}

void TransactionContext::Init(const size_t thread_id,
                              const IsolationLevelType isolation,
                              const cid_t &read_id, const cid_t &commit_id) {
  read_id_ = read_id;

  // commit id can be set at a transaction's commit phase.
  commit_id_ = commit_id;

  // set txn_id to commit_id.
  txn_id_ = commit_id_;

  epoch_id_ = read_id_ >> 32;

  thread_id_ = thread_id;

  is_written_ = false;

  isolation_level_ = isolation;

  gc_set_ = std::make_shared<GCSet>();
  gc_object_set_ = std::make_shared<GCObjectSet>();

//  on_commit_triggers_.reset();
}

RWType TransactionContext::GetRWType(const ItemPointer &location) {
  const auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end()) {
    return rw_set_it->second;
  }
  return RWType::INVALID;
}

void TransactionContext::RecordReadOwn(const ItemPointer &location) {
  PELOTON_ASSERT(rw_set_.find(location) == rw_set_.end() ||
                 (rw_set_[location] != RWType::DELETE &&
                  rw_set_[location] != RWType::INS_DEL));
  rw_set_[location] = RWType::READ_OWN;
  is_written_ = true;
}

void TransactionContext::RecordUpdate(const ItemPointer &location) {
  PELOTON_ASSERT(rw_set_.find(location) == rw_set_.end() ||
                 (rw_set_[location] != RWType::DELETE &&
                  rw_set_[location] != RWType::INS_DEL));
  rw_set_[location] = RWType::UPDATE;
  is_written_ = true;
}

void TransactionContext::RecordInsert(const ItemPointer &location) {
  PELOTON_ASSERT(rw_set_.find(location) == rw_set_.end());
  rw_set_[location] = RWType::INSERT;
  is_written_ = true;
}

bool TransactionContext::RecordDelete(const ItemPointer &location) {
  PELOTON_ASSERT(rw_set_.find(location) == rw_set_.end() ||
                 (rw_set_[location] != RWType::DELETE &&
                  rw_set_[location] != RWType::INS_DEL));
  auto rw_set_it = rw_set_.find(location);
  if (rw_set_it != rw_set_.end() && rw_set_it->second == RWType::INSERT) {
    PELOTON_ASSERT(is_written_);
    rw_set_it->second = RWType::INS_DEL;
    return true;
  } else {
    rw_set_[location] = RWType::DELETE;
    is_written_ = true;
    return false;
  }
}

const std::string TransactionContext::GetInfo() const {
  std::ostringstream os;

  os << " Txn :: @" << this << " ID : " << std::setw(4) << txn_id_
     << " Read ID : " << std::setw(4) << read_id_
     << " Commit ID : " << std::setw(4) << commit_id_
     << " Result : " << result_;

  return os.str();
}

BackendLogger* TransactionContext::GetBackendLogger() const {
  return traffic_cop->GetBackendLogger();
}

void TransactionContext::SetBackendLogger(BackendLogger* logger) {
  assert(logger);
  traffic_cop->SetBackendLogger(logger);
}

void TransactionContext::SetTrafficCop(TrafficCop* cop) {
  traffic_cop = cop;
}

