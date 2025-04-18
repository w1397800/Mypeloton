//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.h
//
// Identification: src/include/logging/log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sys/stat.h>

#include <map>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

#include "backend_logger.h"
#include "frontend_logger.h"
#include "my_inttypes.h"
#include "replica/raft_node.h"
#include "replica/state_machine.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/logging/logging_util.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/table_factory.h"
#include "storage/peloton/type/tuple_value_pool.h"
#define NO_XA_RECOVERY true

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//
extern LoggingType peloton_logging_mode;

//===--------------------------------------------------------------------===//
// Log Manager
//===--------------------------------------------------------------------===//

// Logging basically refers to the PROTOCOL -- like aries or peloton
// Logger refers to the implementation -- like frontend or backend
// Transition diagram :: standby -> recovery -> logging -> terminate -> sleep

#define LOG_FILE_LEN (1024 * UINT64_C(128))  // 128 MB

class StateMachineImpl : public mraft::StateMachine {
   public:
    // Invoked once when the raft node was shut down.
    // Default do nothing
    void on_shutdown() override;

    // Invoked when the belonging node becomes the leader of the group at |term|
    // Default: Do nothing
    void on_leader_start(const mraft::LeaderStartContext &ctx) override;

    // Invoked when this node steps down from the leader of the replication
    // group and |status| describes detailed information
    void on_leader_stop(const butil::Status &status) override;

    // this method is called when a follower stops following a leader and its
    // leader_id becomes NULL, situations including:
    // 1. handle lease_timeout
    // 2.
    void on_stop_following(const mraft::LeaderChangeContext &ctx) override;

    // this method is called when a follower or candidate starts following a
    // leader and its leader_id (should be NULL before the method is called) is
    // set to the leader's id, situations including:
    // 1. a candidate receives append_entries from a leader
    // 2. election chooses a leader
    // 3.
    void on_start_following(const mraft::LeaderChangeContext &ctx) override;
};

/**
 * Global Log Manager
 */
#define kDEFAULT_LOG_FILE_DIR "./"
class LogManagerOptions {
   public:
    LogManagerOptions() = default;

    // If use mraft，this dir is the common dir for all streams
    std::string log_file_dir = kDEFAULT_LOG_FILE_DIR;

    vector<std::string> log_file_dirs{kDEFAULT_LOG_FILE_DIR};

    bool enable_logging = true;

    bool enable_chkpt = false;
};

class LogManager {
   public:
    void ScanOneTable(DataTable *target_table, cid_t commit_id);

    int Init(LogManagerOptions &options);

    void RecoveryIndex();

    void RecoveryIndexMultiThread(int thread_num);

    // 将数据库根据 tile_group 尽量均分成 part_num 组并返回这些分好组的
    // tile_group_id
    std::vector<std::vector<uint>> DataScanner(int part_num);

    static void IndexBuilder(const std::vector<uint> &tile_group_ids,
                             cid_t commit_id, uint thread_id);
    //===--------------------------------------------------------------------===//
    // Index recovery
    //===--------------------------------------------------------------------===//

    LogManager(const LogManager &) = delete;
    LogManager &operator=(const LogManager &) = delete;
    LogManager(LogManager &&) = delete;
    LogManager &operator=(LogManager &&) = delete;

    // global singleton
    static LogManager &GetInstance(void);

    // configuration
    void Configure(LoggingType logging_type, bool test_mode = false,
                   LoggerMappingStrategyType logger_mapping_strategy =
                       LoggerMappingStrategyType::ROUND_ROBIN) {
        logging_type_ = logging_type;
        test_mode_ = test_mode;
        logger_mapping_strategy_ = logger_mapping_strategy;
    }

    // reset log status to invalid
    void ResetLogStatus() {
        this->recovery_to_logging_counter = 0;
        SetLoggingStatus(LoggingStatusType::INVALID);
    }

    // Wait for the system to begin
    void StartStandbyMode();

    // Start recovery
    void StartRecoveryMode();

    void StartLoggingMode() {
        WaitForModeTransition(LoggingStatusType::STANDBY, true);
        LOG(INFO) << "Starting logging mode";
        SetLoggingStatus(LoggingStatusType::LOGGING);
        for (auto &logger : frontend_loggers) {
            logger->start_logging_mode();
        }
    }

    void StartApplyMode() {
        SetLoggingStatus(LoggingStatusType::APPLYING);
        LOG(INFO) << "Starting apply mode";
        for (auto &logger : frontend_loggers) {
            logger->start_apply_mode();
        }
    }

    // Check whether the frontend logger is in logging mode
    inline bool IsInLoggingMode() {
        auto is_in_logging_mode =
            (logging_status == LoggingStatusType::LOGGING);
        return is_in_logging_mode;
    }

    inline bool IsInApplyingMode() {
        return logging_status == LoggingStatusType::APPLYING;
    }

    // Used to terminate current logging and wait for sleep mode
    void TerminateLoggingMode();

    // Used to wait for a certain mode (or not certain mode if is_equal is
    // false)
    void WaitForModeTransition(LoggingStatusType logging_status, bool is_equal);

    // get the current persistent flushed commit
    cid_t GetPersistentFlushedCommitId();

    // called by frontends when recovery is complete.(for a particular frontend)
    void NotifyRecoveryDone();

    //===--------------------------------------------------------------------===//
    // DDL data logger
    //===--------------------------------------------------------------------===//

    void InitDDLLogDirectory(void);

    // every table call once
    void FlushTableRecord(const shared_ptr<LogRecord> &record);

    // called before checkpoint recovery and log recovery
    void RecoveryDDL();

    void ReCreateIndex(TableRecord *table_record, DataTable *data_table);

    LogRecordType GetNextDDLRecordType();
    //===--------------------------------------------------------------------===//
    // Accessors
    //===--------------------------------------------------------------------===//

    // Logging status associated with the front end logger of given type
    void SetLoggingStatus(LoggingStatusType logging_status);

    LoggingStatusType GetLoggingStatus();

    // Whether to enable or disable synchronous commit ?
    void SetSyncCommit(bool sync_commit) {
        syncronization_commit = sync_commit;
    }

    // get the status of sychronus commit
    bool GetSyncCommit(void) const { return syncronization_commit; }

    // returns true if a frontend logger is active
    bool ContainsFrontendLogger(void);

    // create a backend logger for transaction context
    BackendLogger *CreateBackendLogger();

    // set the name of the log file (only used in wbl)
    void SetLogFileName(std::string log_file);

    // get the log file name (used only in wbl)
    std::string GetLogFileName(void);

    void SetLogDirectoryName(std::vector<std::string> log_dir);

    std::vector<std::string> GetLogDirectoryNames(void);

    std::string GetFirstLogDirectoryName(void);

    bool HasPelotonFrontendLogger() const {
        return (peloton_logging_mode == LoggingType::NVM_WBL);
    }

    // Drop all default tiles for tables before recovery
    void PrepareRecovery();

    // Add default tiles for tables if necessary
    void DoneRecovery();

    //===--------------------------------------------------------------------===//
    // Utility Functions
    //===--------------------------------------------------------------------===//

    // get a frontend logger at given index
    FrontendLogger *GetFrontendLogger(unsigned int logger_idx);

    // remove all frontend loggers (used for testing)
    void DropFrontendLoggers();

    // perpare to log must be called before a commit id is generated for a
    // transaction
    void PrepareLogging(TransactionContext *txn_ctx);

    // log the beginning of a commited transaction
    void LogBeginTransaction(TransactionContext *txn_ctx);
    // log a table log create
    void TableLogInsert(DataTable *table);

    void TableLogDrop(TransactionContext *txn_ctx, DataTable *table);

    // log an update
    void LogUpdate(TransactionContext *txn_ctx,
                ItemPointer new_version);

    // log an insert
    void LogInsert(TransactionContext *txn_ctx,
                ItemPointer new_location);

    // log a delete
    void LogDelete(TransactionContext *txn_ctx,
                ItemPointer delete_location);

    // commit a transaction and wait until stable
    void LogPrepareTransaction(TransactionContext *txn_ctx);

    // commit a transaction and wait until stable
    void LogCommitTransaction(TransactionContext *txn_ctx);

    // called if a transaction aborts before starting a commit
    void DoneLogging(TransactionContext *current_txn);

    // the maximum flushed commit id for all frontend loggers
    cid_t GetGlobalMaxFlushedIdForRecovery() {
        return global_max_flushed_id_for_recovery;
    }

    // set the max flushed id for recovery
    void SetGlobalMaxFlushedIdForRecovery(cid_t new_max) {
        global_max_flushed_id_for_recovery = new_max;
    }

    // updates the catalog and transaction managers to the correct oid and cid
    // after recovery
    void UpdateCatalogAndTxnManagers(uint new_oid, eid_t new_epoch_id,
                                     uint32_t new_txn_id);

    // set the maximum commit id which has been persisted to disk
    void SetGlobalMaxFlushedCommitId(cid_t);

    // set the maximum commit id which has been persisted to disk
    cid_t GetGlobalMaxFlushedCommitId() const;

    // get the list of frontend loggers
    std::vector<std::unique_ptr<FrontendLogger>> &GetFrontendLoggersList() {
        return frontend_loggers;
    }

    // get the threshold for creating a new log file
    inline unsigned int GetLogFileSizeLimit() { return log_file_size_limit_; }

    // set the threshold for creating a new log file
    inline void SetLogFileSizeLimit(unsigned int file_size_limit) {
        log_file_size_limit_ = file_size_limit;
    }

    // get the beginning capacity of a log buffer
    inline unsigned int GetLogBufferCapacity() { return log_buffer_capacity_; }

    // set the initial capacity of log buffers passed between frontend and
    // backend loggers
    inline void SetLogBufferCapacity(unsigned int log_buffer_capacity) {
        log_buffer_capacity_ = log_buffer_capacity;
    }

    inline void SetNoWrite(bool no_write) { no_write_ = no_write; }

    inline bool GetNoWrite() const { return no_write_; }

    void SetIsRecoveryFromSnapshoot(bool is);

    bool GetIsRecoveryFromSnapshoot(void) const;

    cid_t GetGlobalLowerBoundFlushedCid() const {
        return global_lower_bound_flushed_cid;
    }

    void SetGlobalLowerBoundFlushedCid(cid_t new_max) {
        global_lower_bound_flushed_cid = new_max;
    }

   private:
    LogManager();
    ~LogManager();

    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    // static configurations for logging
    LoggingType logging_type_ = LoggingType::INVALID;

    // test mode will not log to disk
    bool test_mode_ = false;

    // set the strategy for mapping frontend loggers to worker threads
    LoggerMappingStrategyType logger_mapping_strategy_ =
        LoggerMappingStrategyType::INVALID;

    // default log file size
    size_t log_file_size_limit_ = LOG_FILE_LEN;

    // default capacity for log buffer
    size_t log_buffer_capacity_ = LOG_FILE_LEN;

    // There is only one frontend_logger of some type
    // either write ahead or write behind logging
    std::vector<std::unique_ptr<FrontendLogger>> frontend_loggers;

    LoggingStatusType logging_status = LoggingStatusType::INVALID;

    bool prepared_recovery_ = false;

    // To synch the status
    std::mutex logging_status_mutex;
    std::condition_variable logging_status_cv;

    // To update catalog and txn managers
    std::mutex update_managers_mutex;

    // count of loggers who moved from the recovery to loggin state
    unsigned int recovery_to_logging_counter = 0;

    bool syncronization_commit =
        true;  // default should be true because it is safest

    // name of log file (for wbl)
    std::string log_file_name;

    std::vector<std::string> log_directory_names;

    // round robin counter for frontend logger assignment
    int frontend_logger_assign_counter{0};

    cid_t global_max_flushed_id_for_recovery = UINT64_MAX;

    bool replicating_ = false;

    bool no_write_ = false;

    // max oid after recovery
    uint max_oid_recovery_ = 0;

    // set in logger_configureation.cpp
    bool is_recovery_from_snapshoot;

    //  cid_t checkpoint_recoverd_max_cid = 0;
    cid_t global_lower_bound_flushed_cid = INVALID_CID;

    eid_t max_epoch_id_recovery_ = INVALID_EID;
    uint32_t max_txn_id_recovery_ = INVALID_TXN_ID;

    //===--------------------------------------------------------------------===//
    // DDL recovery
    //===--------------------------------------------------------------------===//
    std::mutex flush_ddl_lock_;

    static constexpr auto ddl_file_dir_ = "peloton_ddl_log";
    static constexpr auto ddl_file_name_ = "ddl.log";

    FileHandle ddl_file_handle_;

    CopySerializeOutput ddl_output_buffer_;

    std::unique_ptr<mraft::RaftNode> raft_node;

    // the common directory of all streams
    std::string _log_file_dir;

    bool _enable_logging = false;
    bool _enable_chkpt = false;
};
