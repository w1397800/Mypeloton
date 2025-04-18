//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.cpp
//
// Identification: src/traffic_cop/traffic_cop.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/traffic_cop/traffic_cop.h"

#include <utility>

#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/concurrency/transaction_manager_factory.h"
#include "storage/peloton/settings/settings_manager.h"


TrafficCop::TrafficCop()
    : is_queuing_(false),
      rows_affected_(0),
      single_statement_txn_(true) {}

TrafficCop::TrafficCop(void (*task_callback)(void *), void *task_callback_arg)
    : single_statement_txn_(true),
      task_callback_(task_callback),
      task_callback_arg_(task_callback_arg) {}

void TrafficCop::Reset() {
  std::stack<TcopTxnState> new_tcop_txn_state;
  // clear out the stack
  swap(tcop_txn_state_, new_tcop_txn_state);
  param_values_.clear();
  setRowsAffected(0);
}

TrafficCop::~TrafficCop() {
  // Abort all running transactions
  while (!tcop_txn_state_.empty()) {
    AbortQueryHelper();
  }

}

/* Singleton accessor
 * NOTE: Used by in unit tests ONLY
 */
TrafficCop &TrafficCop::GetInstance() {
  static TrafficCop tcop;
  tcop.Reset();
  return tcop;
}

TrafficCop::TcopTxnState &TrafficCop::GetDefaultTxnState() {
  static TcopTxnState default_state;
  default_state = std::make_pair(nullptr, ResultType::INVALID);
  return default_state;
}

TrafficCop::TcopTxnState &TrafficCop::GetCurrentTxnState() {
  if (tcop_txn_state_.empty()) {
    return GetDefaultTxnState();
  }
  return tcop_txn_state_.top();
}

ResultType TrafficCop::BeginQueryHelper(size_t thread_id) {
//  if (tcop_txn_state_.empty()) {
//    auto &txn_manager = TransactionManagerFactory::GetInstance();
//    auto txn = txn_manager.BeginTransaction(thread_id);
//    // this shouldn't happen
//    if (txn == nullptr) {
//      return ResultType::FAILURE;
//    }
//    // initialize the current result as success
//    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
//  }
//  return ResultType::SUCCESS;
  auto &curr_state = GetCurrentTxnState();
  TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {//不为空就代表事务已经存在.
    txn = curr_state.first;
  } else {
    // No active txn, single-statement txn
    auto &txn_manager = TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(this, thread_id);
    if (txn == nullptr) {
      return ResultType::FAILURE;
    }
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  return ResultType::SUCCESS;
}

ResultType TrafficCop::CommitQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  auto txn = curr_state.first;
  auto &txn_manager = TransactionManagerFactory::GetInstance();
  // I catch the exception (ex. table not found) explicitly,
  // If this exception is caused by a query in a transaction,
  // I will block following queries in that transaction until 'COMMIT' or
  // 'ROLLBACK' After receive 'COMMIT', see if it is rollback or really commit.
  if (curr_state.second != ResultType::ABORTED) {
    // txn committed
    return txn_manager.CommitTransaction(txn);
  } else {
    // otherwise, rollback
    return txn_manager.AbortTransaction(txn);
  }
}


ResultType TrafficCop::AbortQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = TransactionManagerFactory::GetInstance();
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    delete curr_state.first;
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

ResultType TrafficCop::AbortQueryHelper(bool is_entire) {
  (void)is_entire;
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty()) {
//    sql_print_warning("nothing abort");
    return ResultType::NOOP;
  }
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = TransactionManagerFactory::GetInstance();
//    auto result = txn_manager.AbortTransaction(txn, is_entire,
//          (curr_state.first->GetResult() == ResultType::FAILURE_AFTER_DELETE)?true:false);
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    delete curr_state.first;
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

/*
 * Do nothing if there is no active transaction;
 * If single-stmt transaction, abort it;
 * If multi-stmt transaction, just set transaction state to 'ABORTED'.
 * The multi-stmt txn will be explicitly aborted when receiving 'Commit' or
 * 'Rollback'.
 */
void TrafficCop::ProcessInvalidStatement() {
  if (single_statement_txn_) {
    LOG_TRACE("SINGLE ABORT!");
    AbortQueryHelper();
  } else {  // multi-statment txn
    if (tcop_txn_state_.top().second != ResultType::ABORTED) {
      tcop_txn_state_.top().second = ResultType::ABORTED;
    }
  }
}



