//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.h
//
// Identification: src/include/concurrency/transaction_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TRANSACTION_CONTEXT
//#define TRANSACTION_CONTEXT
#include <atomic>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

//#include "storage/peloton/catalog/catalog_cache.h"
//#include "storage/peloton/common/exception.h"
#include "storage/peloton/common/item_pointer.h"
//#include "storage/peloton/common/printable.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/logging/backend_logger.h"
#include "storage/peloton/traffic_cop/traffic_cop.h"

class TrafficCop;
//
//namespace trigger {
//class TriggerSet;
//class TriggerData;
//}  // namespace trigger


//===--------------------------------------------------------------------===//
// TransactionContext
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for transaction context.
 */
class TransactionContext  {
  TransactionContext(TransactionContext const &) = delete;

 public:
  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id);

  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
              const cid_t &read_id, const cid_t &commit_id);

  /**
   * @brief      Destroys the object.
   */
  ~TransactionContext() = default;

 private:
  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id) {
    Init(thread_id, isolation, read_id, read_id);
  }

  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id, const cid_t &commit_id);

 public:
  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  /**
   * @brief      Gets the thread identifier.
   *
   * @return     The thread identifier.
   */
  inline size_t GetThreadId() const { return thread_id_; }

  /**
   * @brief      Gets the transaction identifier.
   *
   * @return     The transaction identifier.
   */
  inline txn_id_t GetTransactionId() const { return txn_id_; }

  /**
   * @brief      Gets the read identifier.
   *
   * @return     The read identifier.
   */
  inline cid_t GetReadId() const { return read_id_; }

  /**
   * @brief      Gets the commit identifier.
   *
   * @return     The commit identifier.
   */
  inline cid_t GetCommitId() const { return commit_id_; }

  /**
   * @brief      Gets the epoch identifier.
   *
   * @return     The epoch identifier.
   */
  inline eid_t GetEpochId() const { return epoch_id_; }

  /**
   * @brief      Gets the timestamp.
   *
   * @return     The timestamp.
   */
  inline uint64_t GetTimestamp() const { return timestamp_; }

  /**
   * @brief      Gets the query strings.
   *
   * @return     The query strings.
   */
  inline const std::vector<std::string>& GetQueryStrings() const {
                                                      return query_strings_; }

  /**
   * @brief      Sets the commit identifier.
   *
   * @param[in]  commit_id  The commit identifier
   */
  inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

  /**
   * @brief      Sets the epoch identifier.
   *
   * @param[in]  epoch_id  The epoch identifier
   */
  inline void SetEpochId(const eid_t epoch_id) { epoch_id_ = epoch_id; }

  /**
   * @brief      Sets the timestamp.
   *
   * @param[in]  timestamp  The timestamp
   */
  inline void SetTimestamp(const uint64_t timestamp) { timestamp_ = timestamp; }

  /**
   * @brief      Adds a query string.
   *
   * @param[in]  query_string  The query string
   */
  inline void AddQueryString(const char* query_string) {
    query_strings_.push_back(std::string(query_string));
  }

  void RecordCreate(uint database_oid, uint table_oid, uint index_oid) {
    rw_object_set_.push_back(std::make_tuple(database_oid, table_oid,
                                index_oid, DDLType::CREATE));
  }

  void RecordDrop(uint database_oid, uint table_oid, uint index_oid) {
    rw_object_set_.push_back(std::make_tuple(database_oid, table_oid,
                                index_oid, DDLType::DROP));
  }

  void RecordReadOwn(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  /**
   * @brief      Delete the record.
   *
   * @param[in]  <unnamed>  The logical physical location of the record
   * @return    true if INS_DEL, false if DELETE
   */
  bool RecordDelete(const ItemPointer &);

  RWType GetRWType(const ItemPointer &);

  /**
   * @brief      Adds on commit trigger.
   *
   * @param      trigger_data  The trigger data
   */
//  void AddOnCommitTrigger(trigger::TriggerData &trigger_data);

//  void ExecOnCommitTriggers();

  /**
   * @brief      Determines if in rw set.
   *
   * @param[in]  location  The location
   *
   * @return     True if in rw set, False otherwise.
   */
  bool IsInRWSet(const ItemPointer &location) {
    return (rw_set_.find(location) != rw_set_.end());
  }

  /**
   * @brief      Gets the read write set.
   *
   * @return     The read write set.
   */
  inline const ReadWriteSet &GetReadWriteSet() const { return rw_set_; }
  inline const CreateDropSet &GetCreateDropSet() { return rw_object_set_; }

  /**
   * @brief      Gets the gc set pointer.
   *
   * @return     The gc set pointer.
   */
  inline std::shared_ptr<GCSet> GetGCSetPtr() { return gc_set_; }

  /**
   * @brief      Gets the gc object set pointer.
   *
   * @return     The gc object set pointer.
   */
  inline std::shared_ptr<GCObjectSet> GetGCObjectSetPtr() {
    return gc_object_set_;
  }

  /**
   * @brief      Determines if gc set empty.
   *
   * @return     True if gc set empty, False otherwise.
   */
  inline bool IsGCSetEmpty() { return gc_set_->size() == 0; }

  /**
   * @brief      Determines if gc object set empty.
   *
   * @return     True if gc object set empty, False otherwise.
   */
  inline bool IsGCObjectSetEmpty() { return gc_object_set_->size() == 0; }

  /**
   * @brief      Get a string representation for debugging.
   *
   * @return     The information.
   */
  const std::string GetInfo() const;

  /**
   * Set result and status.
   *
   * @param[in]  result  The result
   */
  inline void SetResult(ResultType result) { result_ = result; }

  /**
   * Get result and status.
   *
   * @return     The result.
   */
  inline ResultType GetResult() const { return result_; }

  /**
   * @brief      Determines if read only.
   *
   * @return     True if read only, False otherwise.
   */
  bool IsReadOnly() const {
    return read_only_;
  }

  /**
   * @brief      mark this context as read only
   *
   */
  void SetReadOnly() {
    read_only_ = true;
  }

  void SetDelLocation(ItemPointer location){
    del_location = location;
  }

  ItemPointer GetDelLocation(){
    return del_location;
  }

  /**
   * @brief      Gets the isolation level.
   *
   * @return     The isolation level.
   */
  inline IsolationLevelType GetIsolationLevel() const {
    return isolation_level_;
  }



  BackendLogger* GetBackendLogger() const;

  void SetBackendLogger(BackendLogger* logger = nullptr);

  void SetTrafficCop(TrafficCop* cop);

  void SetXid(shared_ptr<PXID> xid) { this->xid = xid; }

  std::shared_ptr<PXID> GetXid() {return xid;}

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  /** transaction id */
  txn_id_t txn_id_;

  /** id of thread creating this transaction */
  size_t thread_id_;

  /**
   * read id
   * this id determines which tuple versions the transaction can access.
   */
  cid_t read_id_;

  /**
   * commit id
   * this id determines the id attached to the tuple version written by the
   * transaction.
   */
  cid_t commit_id_;

  /**
   * epoch id can be extracted from read id.
   * GC manager uses this id to check whether a version is still visible.
   */
  eid_t epoch_id_;

  /**
   * vector of strings to log at the end of the transaction
   * populated only if the indextuner is running
   */
  std::vector<std::string> query_strings_;

  /** timestamp when the transaction began */
  uint64_t timestamp_;

  ReadWriteSet rw_set_;
  CreateDropSet rw_object_set_;

  /**
   * this set contains data location that needs to be gc'd in the transaction.
   */
  std::shared_ptr<GCSet> gc_set_;
  std::shared_ptr<GCObjectSet> gc_object_set_;

  /** result of the transaction */
  ResultType result_ = ResultType::SUCCESS;

  IsolationLevelType isolation_level_;

  bool is_written_;

//  std::unique_ptr<trigger::TriggerSet> on_commit_triggers_;

  /** one default transaction is NOT 'read only' unless it is marked 'read only' explicitly*/
  bool read_only_ = false;

  ItemPointer del_location;//在主键更新中, 先删除再插入, 失败了就记录该location

  TrafficCop* traffic_cop = nullptr;

  std::shared_ptr<PXID> xid = nullptr;
};

//#endif