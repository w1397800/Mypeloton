//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.h
//
// Identification: src/include/traffic_cop/traffic_cop.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <stack>
#include <vector>

// Libevent 2.0

#include "storage/peloton/catalog/column.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/type/type.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/logging/backend_logger.h"

class TransactionContext;


//===--------------------------------------------------------------------===//
// TRAFFIC COP
// Helpers for executing statements.
//
// Usage in unit tests:
//   auto &traffic_cop = tcop::TrafficCop::GetInstance();
//   traffic_cop.SetTaskCallback(<callback>, <arg>);
//   txn = txn_manager.BeginTransaction();
//   traffic_cop.SetTcopTxnState(txn);
//   std::shared_ptr<AbstractPlan> plan = <set up a plan>;
//   traffic_cop.ExecuteHelper(plan, <params>, <result>, <result_format>);
//   <wait>
//   traffic_cop.CommitQueryHelper();
//===--------------------------------------------------------------------===//

class TrafficCop {
 public:
  TrafficCop();
  TrafficCop(void (*task_callback)(void *), void *task_callback_arg);
  ~TrafficCop();
  DISALLOW_COPY_AND_MOVE(TrafficCop);

  // Static singleton used by unit tests.
  static TrafficCop &GetInstance();

  // Reset this object.
  void Reset();

  bool get_is_first_stmt(){return is_first_stmt;}
  void set_is_first_stmt(bool flag){is_first_stmt = flag;}

  std::string get_curr_table_name(){return curr_table_name;}
  void set_curr_table_name(std::string t_name){curr_table_name = t_name;}


    void SetTcopTxnState(TransactionContext *txn) {
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  using TcopTxnState = std::pair<TransactionContext *, ResultType>;
  std::stack<TcopTxnState> GetTcopTxnState() {
    return tcop_txn_state_;
  }

  ResultType BeginQueryHelper(size_t thread_id);

  ResultType CommitQueryHelper();

  ResultType AbortQueryHelper();
  ResultType AbortQueryHelper(bool is_entire);


  void SetTaskCallback(void (*task_callback)(void *), void *task_callback_arg) {
    task_callback_ = task_callback;
    task_callback_arg_ = task_callback_arg;
  }

  void setRowsAffected(int rows_affected) { rows_affected_ = rows_affected; }

  void ProcessInvalidStatement();

  int getRowsAffected() { return rows_affected_; }

  void SetParamVal(std::vector<Value> param_values) {
    param_values_ = std::move(param_values);
  }

  std::vector<Value> &GetParamVal() { return param_values_; }

  std::string &GetErrorMessage() { return error_message_; }

  void SetQueuing(bool is_queuing) { is_queuing_ = is_queuing; }

  bool GetQueuing() { return is_queuing_; }


  void SetDefaultDatabaseName(std::string default_database_name) {
    default_database_name_ = std::move(default_database_name);
  }

  // TODO: this member variable should be in statement_ after parser part
  // finished
  std::string query_;

  void SetBackendLogger(BackendLogger* logger) {
    assert(logger);
    backend_logger.reset(logger);
  }

  BackendLogger* GetBackendLogger(void) {
    return backend_logger.get();
  }

 private:
  bool is_queuing_;

  std::string error_message_;

  std::vector<Value> param_values_;

  // Default database name
  std::string default_database_name_ = DEFAULT_DB_NAME;

  int rows_affected_;


  // flag of single statement txn
  bool single_statement_txn_;


  // The current callback to be invoked after execution completes.
  void (*task_callback_)(void *);
  void *task_callback_arg_;

  // pair of txn ptr and the result so-far for that txn
  // use a stack to support nested-txns
//  using TcopTxnState = std::pair<TransactionContext *, ResultType>;
  std::stack<TcopTxnState> tcop_txn_state_;

  static TcopTxnState &GetDefaultTxnState();

  TcopTxnState &GetCurrentTxnState();
  bool is_first_stmt = true;
  std::string curr_table_name;
//  ResultType BeginQueryHelper(size_t thread_id);


  std::unique_ptr<BackendLogger> backend_logger = nullptr;
};

