//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/logging/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <condition_variable>
#include <memory>
#include <vector>

#include "common/raft.h"
#include "replica/raft_node.h"
#include "storage/peloton/catalog/catalog.h"
#include "storage/peloton/catalog/manager.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/concurrency/transaction_manager_factory.h"

// #include "storage/peloton/executor/executor_context.h"
#include <sql/log.h>

#include "log_manager.h"
#include "logging_util.h"
#include "records/transaction_record.h"
#include "storage/peloton/index/index_factory.h"
#include "storage/peloton/logging/backend_logger.h"
#include "storage/peloton/logging/frontend_logger.h"
#include "storage/peloton/logging/records/tuple_record.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/tile_group.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/type/value.h"

#define USE_MRAFT true

inline void ParseFlags(std::vector<std::string> flags) {
    int argc = flags.size() + 1;  // +1 是为了包括程序名
    char **argv = new char *[argc];
    argv[0] = const_cast<char *>("main");
    for (size_t i = 1; i < static_cast<size_t>(argc); i++) {
        argv[i] = const_cast<char *>(flags[i - 1].c_str());
    }

    google::ParseCommandLineFlags(&argc, &argv, true);
}

int LogManager::Init(LogManagerOptions &options) {
    _enable_logging = options.enable_logging;

    if (!_enable_logging) {
        return 0;
    }

    log_directory_names = options.log_file_dirs;
    _log_file_dir = options.log_file_dir;

    ParseFlags({"--minloglevel=1", "--raft_max_segment_size=1073741824",
                "--max_body_size=1073741824"});

    // Mraft
    raft_node.reset(new mraft::RaftNode());
    mraft::RaftNode::RaftNodeOptions raft_options;
    raft_options.log_file_path = _log_file_dir;
    // TODO: set in my.cnf
    raft_options.confg_file_path = "/home/zky/MyPeloton/cluster_cnf.json";
    raft_options.state_machine.reset(new StateMachineImpl());

    auto s = raft_node->init(raft_options);
    if (!s.ok()) {
        LOG(FATAL) << "Failed to init raft node: ";
        return 1;
    }

    // FrontendLogger
    CHECK_NE(raft_node->stream_nums(), 0L);
    for (unsigned int i = 0; i < raft_node->stream_nums(); i++) {
        FrontendLoggerOptions logger_options;
        logger_options.stream = raft_node->get_stream(i);
        CHECK(logger_options.stream != nullptr);
        std::unique_ptr<FrontendLogger> logger(new FrontendLogger());
        logger->init(logger_options);
        logger_options.stream->reset_commit_at_func(FrontendLogger::apply_at,
                                                    logger.get());
        CHECK(logger != nullptr);
        frontend_loggers.push_back(std::move(logger));
    }

    // assign a distinguished logger thread only if we have more than 1 loggers
    frontend_loggers[0].get()->set_is_distinguished_logger(true);
    return 0;
}

LogManager::LogManager() {
    Configure(peloton_logging_mode, false,
              LoggerMappingStrategyType::ROUND_ROBIN);
}

LogManager::~LogManager() {
    if (!this->IsInLoggingMode()) {
        return;
    }
}

/**
 * @brief Return the singleton log manager instance
 */
LogManager &LogManager::GetInstance() {
    static LogManager log_manager;
    return log_manager;
}

/**
 * @brief Standby logging based on logging type
 *  and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
void LogManager::StartStandbyMode() {
    InitDDLLogDirectory();
    SetLoggingStatus(LoggingStatusType::STANDBY);
}

void LogManager::StartRecoveryMode() {
    SetLoggingStatus(LoggingStatusType::RECOVERY);
    if (!_enable_logging) {
        return;
    }

    CHECK(!frontend_loggers.empty());
    for (unsigned int i = 0; i < frontend_loggers.size(); i++) {
        std::thread(&FrontendLogger::do_recovery, frontend_loggers[i].get())
            .detach();
    }
}

void LogManager::NotifyRecoveryDone() {
    unsigned int i =
        __sync_add_and_fetch(&this->recovery_to_logging_counter, 1);
    LOG(INFO) << recovery_to_logging_counter << "/" << frontend_loggers.size()
              << " loggers have done recovery.";

    if (i == frontend_loggers.size()) {
        LOG(INFO) << "All loggers have done recovery.";
        raft_node->start();
        DoneRecovery();
        SetLoggingStatus(LoggingStatusType::STANDBY);
    }
}

void LogManager::TerminateLoggingMode() {
    //  LOG_TRACE("TRACKING: LogManager::TerminateLoggingMode()");
    SetLoggingStatus(LoggingStatusType::TERMINATE);

    // We set the frontend logger status to Terminate
    // And, then we wait for the transition to sleep mode
    WaitForModeTransition(LoggingStatusType::SLEEP, true);
}

void LogManager::WaitForModeTransition(LoggingStatusType logging_status_,
                                       bool is_equal) {
    // Wait for mode transition
    {
        std::unique_lock<std::mutex> wait_lock(logging_status_mutex);

        while ((!is_equal && logging_status == logging_status_) ||
               (is_equal && logging_status != logging_status_)) {
            logging_status_cv.wait(wait_lock);
        }
    }
}

//===--------------------------------------------------------------------===//
// Index Rebuild
//===--------------------------------------------------------------------===//

void LogManager::ScanOneTable(DataTable *target_table, cid_t commit_id) {
    CopySerializeOutput output_buffer;

    //  std::vector<ItemPointer> visible_tuple_locations;
    std::unique_ptr<LogRecord> record;
    std::unique_ptr<Tuple> tuple;
    std::unique_ptr<TupleValuePoll> pool(new TupleValuePoll());
    uint insert_cnt_for_debug = 0;
    //    std::unique_ptr<ItemPointer> index_entry_ptr;

    auto schema = target_table->GetSchema();
    auto table_tile_group_count = target_table->GetTileGroupCount();

    // loop all tile_group
    for (unsigned int current_tile_group_offset = 0;
         current_tile_group_offset < table_tile_group_count;
         current_tile_group_offset++) {
        auto tile_group = target_table->GetTileGroup(current_tile_group_offset);
        auto active_tuple_count = tile_group->GetNextTupleSlot();
        auto tile_group_header = tile_group->GetHeader();

        // loop all slot of tile_group
        for (uint tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
            auto visibility =
                LoggingUtil::IsVisible(tile_group_header, tuple_id, commit_id);
            if (!visibility) {
                continue;
            }

            tuple.reset(new Tuple(schema, true));
            for (uint col = 0; col < schema->GetColumnCount(); col++) {
                Value val = (tile_group->GetValue(tuple_id, col));
                tuple->SetValue(col, val, pool.get());
            }

            ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
            //        index_entry_ptr.reset(new ItemPointer(location.block,
            //        location.offset));
            auto index_entry_ptr =
                target_table->InsertIndexFromRecovery(tuple.get(), location, 0);
            tile_group_header->SetIndirection(tuple_id, index_entry_ptr);
        }
        // 定期释放内存
        pool->Free(nullptr);
    }
}

void LogManager::RecoveryIndex() {
    uint database_count = 1;  // catalog->GetDatabaseCount();
    auto storageManager = StorageManager::GetInstance();
    cid_t max_cid_recovery =
        (max_epoch_id_recovery_ << 32) + max_txn_id_recovery_;

    // loop all databases
    for (uint database_idx = 1; database_idx <= database_count;
         database_idx++) {
        auto database = storageManager->GetDatabaseWithOid(
            database_idx);  // catalog->GetDatabaseWithOffset(database_idx);
        auto table_count = database->GetTableCount();

        // loop all tables
        for (uint table_idx = 0; table_idx < table_count; table_idx++) {
            // Get the target table
            DataTable *target_table = database->GetTable(table_idx);
            assert(target_table);

            // skip tables without indexs
            if (target_table->GetIndexCount() == 0) {
                continue;
            }

            ScanOneTable(target_table, max_cid_recovery);
        }
    }
}

void LogManager::RecoveryIndexMultiThread(int thread_num) {
    cid_t max_cid_recovery =
        (max_epoch_id_recovery_ << 32) + max_txn_id_recovery_;
    auto partitions = DataScanner(thread_num);

    std::vector<std::thread> recovery_threads;
    for (uint i = 0; i < thread_num; i++) {
        recovery_threads.emplace_back(IndexBuilder, partitions[i],
                                      max_cid_recovery, i);
    }

    for (auto &trd : recovery_threads) {
        trd.join();
    }
}

// 将数据库根据 tile_group 尽量均分成 part_num 组并返回这些分好组的
// tile_group_id
std::vector<std::vector<uint>> LogManager::DataScanner(int part_num) {
    // 预留分组位置
    std::vector<std::vector<uint>> partitions;
    partitions.reserve(part_num);
    std::vector<uint> empty_vector;
    for (int i = 0; i < part_num; i++) {
        partitions.push_back(empty_vector);
    }

    uint database_count = 1;  // catalog->GetDatabaseCount();
    auto storageManager = StorageManager::GetInstance();

    uint not_empty_tile_group_cnt = 0;
    // loop all databases
    for (uint database_idx = 0; database_idx < database_count; database_idx++) {
        auto database = storageManager->GetDatabaseWithOid(database_idx);
        auto table_count = database->GetTableCount();

        // loop all tables
        for (uint table_idx = 0; table_idx < table_count; table_idx++) {
            DataTable *target_table = database->GetTable(table_idx);
            assert(target_table);

            // skip tables without indexs
            if (target_table->GetIndexCount() == 0) {
                continue;
            }

            // loop all tile_groups
            auto table_tile_group_count = target_table->GetTileGroupCount();
            for (unsigned int current_tile_group_offset = 0;
                 current_tile_group_offset < table_tile_group_count;
                 current_tile_group_offset++) {
                auto tile_group =
                    target_table->GetTileGroup(current_tile_group_offset);
                auto active_tuple_count = tile_group->GetNextTupleSlot();
                if (active_tuple_count == 0) {
                    continue;
                }

                // add the tile_group id to partition
                partitions[not_empty_tile_group_cnt % part_num].emplace_back(
                    tile_group->GetTileGroupId());
                not_empty_tile_group_cnt++;
            }  // tile_group loop
        }      // table loop
    }          // database loop

    return partitions;
}

void LogManager::IndexBuilder(const std::vector<uint> &tile_group_ids,
                              cid_t commit_id, uint thread_id) {
    auto storage_manager = StorageManager::GetInstance();
    std::shared_ptr<TileGroup> tile_group;

    std::unique_ptr<LogRecord> record;
    std::unique_ptr<Tuple> tuple;
    std::unique_ptr<TupleValuePoll> pool(new TupleValuePoll());

    for (auto &tg_id : tile_group_ids) {
        tile_group = storage_manager->GetTileGroup(tg_id);
        auto data_table =
            reinterpret_cast<DataTable *>(tile_group->GetAbstractTable());

        auto schema = data_table->GetSchema();
        auto active_tuple_count = tile_group->GetNextTupleSlot();
        auto tile_group_header = tile_group->GetHeader();

        // loop all slot of tile_group
        for (uint tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
            auto visibility =
                LoggingUtil::IsVisible(tile_group_header, tuple_id, commit_id);
            if (!visibility) {
                continue;
            }

            tuple.reset(new Tuple(schema, true));
            for (uint col = 0; col < schema->GetColumnCount(); col++) {
                Value val = (tile_group->GetValue(tuple_id, col));
                tuple->SetValue(col, val, pool.get());
            }

            ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

            auto index_entry_ptr = data_table->InsertIndexFromRecovery(
                tuple.get(), location, thread_id);
            tile_group_header->SetIndirection(tuple_id, index_entry_ptr);
        }
        // 定期释放内存
        pool->Free(nullptr);
    }
}
//===--------------------------------------------------------------------===//
// Utility Functions
//===--------------------------------------------------------------------===//

void LogManager::PrepareLogging(TransactionContext *txn_ctx) {
    if (this->IsInLoggingMode()) {
        if (!txn_ctx->GetBackendLogger()) {
            auto bl = CreateBackendLogger();
            CHECK(bl != nullptr);
            txn_ctx->SetBackendLogger(bl);
        }
        auto logger = txn_ctx->GetBackendLogger();
        logger->SetHasTransaction(true);
    }
}

void LogManager::DoneLogging(TransactionContext *current_txn) {
    if (this->IsInLoggingMode()) {
        auto logger = current_txn->GetBackendLogger();
        logger->SetHasTransaction(false);
    }
}

// Current delete records all the tuple, not only the primary key.
void LogManager::LogDelete(TransactionContext *txn_ctx,
                           ItemPointer delete_location) {
    if (this->IsInLoggingMode()) {
        auto logger = txn_ctx->GetBackendLogger();
        std::unique_ptr<LogRecord> record;
        record.reset(new TupleRecord(LOGRECORD_TYPE_TUPLE_DELETE,
                                     txn_ctx->GetCommitId(), delete_location));

        logger->Log(record.get());
    }
}

void LogManager::LogUpdate(TransactionContext *txn_ctx,
                           ItemPointer new_location) {
    if (this->IsInLoggingMode()) {
        auto logger = txn_ctx->GetBackendLogger();
        std::unique_ptr<LogRecord> record;
        record.reset(new TupleRecord(LOGRECORD_TYPE_TUPLE_UPDATE,
                                     txn_ctx->GetCommitId(), new_location));

        logger->Log(record.get());
    }
}

void LogManager::LogInsert(TransactionContext *txn_ctx,
                           ItemPointer new_location) {
    if (this->IsInLoggingMode()) {
        auto logger = txn_ctx->GetBackendLogger();
        std::unique_ptr<LogRecord> record;
        record.reset(new TupleRecord(LOGRECORD_TYPE_TUPLE_INSERT,
                                     txn_ctx->GetCommitId(), new_location));

        logger->Log(record.get());
    }
}

void LogManager::TableLogInsert(DataTable *table) {
    if (this->IsInLoggingMode()) {
        std::shared_ptr<LogRecord> record(
            new TableRecord(LOGRECORD_TYPE_DDL_TABLE_CREATE, INVALID_CID,
                            table->GetDatabaseOid(), table->GetOid(),
                            table->GetName(), table->GetMove_pos(),
                            table->GetPk_cols(), table->GetPrimaryIndexName(),
                            table->GetUniqueIndexInfo(), table->GetSchema()));

        FlushTableRecord(record);
    }
}

void LogManager::TableLogDrop(TransactionContext *txn_ctx, DataTable *table) {
    // TODO
}

void LogManager::LogBeginTransaction(TransactionContext *txn_ctx) {
    //  sql_print_warning("LogBeginTransaction");
    if (this->IsInLoggingMode()) {
        auto logger = txn_ctx->GetBackendLogger();
        // LOG(ERROR) << "Begin: " << logger->GetFrontendLoggerID();

        logger->SetMaxCollectedCid(global_lower_bound_flushed_cid);
        // LOG(ERROR) << global_lower_bound_flushed_cid;

        TransactionRecord record(LOGRECORD_TYPE_TRANSACTION_BEGIN,
                                 txn_ctx->GetCommitId());
        logger->Log(&record);
    }
}

void LogManager::LogCommitTransaction(TransactionContext *txn_ctx) {
    //  sql_print_warning("LogCommitTransaction");
    if (this->IsInLoggingMode()) {
        auto logger = txn_ctx->GetBackendLogger();
        TransactionRecord record(LOGRECORD_TYPE_TRANSACTION_COMMIT,
                                 txn_ctx->GetCommitId());

        // LOG(ERROR) << "Commit: " << logger->GetFrontendLoggerID();
        logger->Log(&record);

        logger->WaitForFlush();

        // LOG(ERROR) << "Finished: " << logger->GetFrontendLoggerID();

        logger->SetHasTransaction(false);

        logger->GetVarlenPool()->Free(nullptr);
    }
}

/**
 * @brief Return the backend logger based on logging type
    and store it into the vector
 * @param logging type can be stdout(debug), aries, peloton
 */
BackendLogger *LogManager::CreateBackendLogger() {
    assert(!frontend_loggers.empty());

    auto logger = new BackendLogger();
    int i = __sync_fetch_and_add(&this->frontend_logger_assign_counter, 1);
    auto index = i % frontend_loggers.size();
    frontend_loggers[index]->add_backend_logger(logger);
    logger->SetFrontendLoggerID(index);

    return logger;
}

/**
 * @brief mark Peloton is ready, so that frontend logger can  logging
 */
LoggingStatusType LogManager::GetLoggingStatus() {
    // Get the logging status
    return logging_status;
}

void LogManager::SetLoggingStatus(LoggingStatusType logging_status_) {
    {
        std::lock_guard<std::mutex> wait_lock(logging_status_mutex);

        // Set the status in the log manager map
        logging_status = logging_status_;

        // notify everyone about the status change
        logging_status_cv.notify_all();
    }
}

void LogManager::SetLogFileName(std::string log_file) {
    log_file_name = log_file;
}

// XXX change to read configuration file
std::string LogManager::GetLogFileName(void) {
    PELOTON_ASSERT(log_file_name.empty() == false);
    return log_file_name;
}

void LogManager::SetLogDirectoryName(std::vector<std::string> log_directorys) {
    log_directory_names = log_directorys;
}

// XXX change to read configuration file
std::vector<std::string> LogManager::GetLogDirectoryNames(void) {
    return log_directory_names;
}

std::string LogManager::GetFirstLogDirectoryName(void) {
    return log_directory_names[0];
}

void LogManager::PrepareRecovery() {
    if (prepared_recovery_) {
        return;
    }
    auto storage_manager = StorageManager::GetInstance();
    // auto catalog = Catalog::GetInstance();
    // for all database
    // auto db_count = catalog->GetDatabaseCount();
    uint db_idx = 0;
    // for (uint db_idx = 1; db_idx < db_count; db_idx++) {
    auto database = storage_manager->GetDatabaseWithOid(db_idx);
    // auto database = catalog->GetDatabaseWithOffset(db_idx);
    // for all tables
    auto table_count = database->GetTableCount();
    for (uint table_idx = 0; table_idx < table_count; table_idx++) {
        auto table = database->GetTable(table_idx);
        // drop existing tile groups
        table->DropTileGroups();
        table->SetTupleCount(0);
    }
    storage_manager->ResetInfo();

    // }
    prepared_recovery_ = true;
}

void LogManager::DoneRecovery() {
    auto storageManager = StorageManager::GetInstance();
    // auto catalog = Catalog::GetInstance();
    // // for all database
    // auto db_count = catalog->GetDatabaseCount();
    uint db_idx = 0;
    // for (uint db_idx = 1; db_idx < db_count; db_idx++) {
    auto database = storageManager->GetDatabaseWithOid(db_idx);
    // auto database = catalog->GetDatabaseWithOffset(db_idx);
    // for all tables
    auto table_count = database->GetTableCount();
    for (uint table_idx = 0; table_idx < table_count; table_idx++) {
        auto table = database->GetTable(table_idx);
        if (table->GetTileGroupCount() == 0) {
            table->AddDefaultTileGroup();
            continue;
        }

        table->ResetActiveTileGroupsAfterRecovery();
    }
    // }
}

void LogManager::UpdateCatalogAndTxnManagers(uint new_oid, eid_t new_epoch_id,
                                             uint32_t new_txn_id) {
    std::unique_lock<std::mutex> wait_lock(update_managers_mutex);

    if (new_epoch_id > max_epoch_id_recovery_) {
        max_epoch_id_recovery_ = new_epoch_id;
        EpochManagerFactory::GetInstance().SetCurrentEpochId(new_epoch_id + 1);
    }

    if (new_txn_id > max_txn_id_recovery_) {
        max_txn_id_recovery_ = new_txn_id;
        EpochManagerFactory::GetInstance().SetCurrentTxnId(new_txn_id);
    }

    if (new_oid > max_oid_recovery_) {
        max_oid_recovery_ = new_oid;
        auto storageManager = StorageManager::GetInstance();
        storageManager->SetNextTileGroupId(new_oid);
    }
}

void LogManager::SetIsRecoveryFromSnapshoot(bool is) {
    this->is_recovery_from_snapshoot = is;
}

bool LogManager::GetIsRecoveryFromSnapshoot() const {
    return this->is_recovery_from_snapshoot;
}

//===--------------------------------------------------------------------===//
// DDL data logger
//===--------------------------------------------------------------------===//

void LogManager::InitDDLLogDirectory(void) {
    auto dir_name = GetFirstLogDirectoryName() + std::string(ddl_file_dir_);

    mkdir(dir_name.c_str(), 0700);
}

void LogManager::FlushTableRecord(const shared_ptr<LogRecord> &record) {
    {
        flush_ddl_lock_.lock();

        if (!ddl_file_handle_.file) {
            // open log file
            std::string full_name = GetFirstLogDirectoryName() +
                                    std::string(ddl_file_dir_) + "/" +
                                    ddl_file_name_;

            ddl_file_handle_.file = fopen(full_name.c_str(), "ab");
            assert(ddl_file_handle_.file);
        }
        record->Serialize(ddl_output_buffer_);
        fwrite(record->GetMessage(), sizeof(char), record->GetMessageLength(),
               ddl_file_handle_.file);
        fflush(ddl_file_handle_.file);
        LOG_TRACE("flushed one table record");

        flush_ddl_lock_.unlock();
    }
}

void LogManager::RecoveryDDL() {
    LOG_TRACE("Start DDL recovery");

    std::string full_name = GetFirstLogDirectoryName() + "/" +
                            std::string(ddl_file_dir_) + "/" + ddl_file_name_;
    ddl_file_handle_.file = fopen(full_name.c_str(), "rb");
    ddl_file_handle_.size =
        LoggingUtil::GetFileSizeFromFileName(full_name.c_str());

    if (!ddl_file_handle_.file) {
        LOG_TRACE("No DDL file found !");
        return;
    }

    TableRecord *table_record;
    std::string table_name;
    cid_t table_id;
    Schema *schema;

    // 用于暂存所有table，实现 drop table，以及创建索引时对重复table的处理。
    std::set<std::string> table_name_set;
    std::vector<TableRecord *> records;

    // 先读取日志文件
    for (auto record_type = GetNextDDLRecordType();
         record_type != LOGRECORD_TYPE_INVALID;
         record_type = GetNextDDLRecordType()) {
        table_record = new TableRecord(record_type);

        if (!LoggingUtil::ReadTableRecordHeader(*table_record,
                                                ddl_file_handle_)) {
            sql_print_warning("MyPeloton: ddl log file are not integrity!");
            break;
        }

        table_name = table_record->GetTableName();

        // 新建表
        if (record_type == LOGRECORD_TYPE_DDL_TABLE_CREATE) {
            auto schema = LoggingUtil::ReadTableRecordBody(ddl_file_handle_);
            if (!schema) {  // 表正在落盘时发生崩溃才会出现这种情况
                sql_print_warning("MyPeloton: ddl log file are not integrity!");
                break;
            }
            table_record->SetSchema(schema);

            // 当表创建索引时，会重新生成一次
            // table_record，所以只需要保留第二条记录就行
            if (table_name_set.find(table_name) != table_name_set.end()) {
                // 替换老的 table_record
                for (auto &record : records) {
                    if (record->GetTableName() == table_name) {
                        auto old_record = record;
                        record = table_record;
                        delete old_record;
                    }
                }
            } else {
                table_name_set.insert(table_name);
                records.emplace_back(table_record);
            }
        }

        // TODO: peloton 里面表的创建是有顺序的, 不然 tile_group
        // 会混乱，所以删表逻辑有待考虑
        if (record_type == LOGRECORD_TYPE_DDL_TABLE_DROP) {
        }
    }

    for (auto &record : records) {
        table_record = record;
        table_name = table_record->GetTableName();
        table_id = table_record->GetTableId();
        schema = table_record->GetSchema();

        auto db_id = table_record->GetDatabaseOid();
        Database *db = StorageManager::GetInstance()->GetDatabaseWithOid(db_id);
        assert(db);

        DataTable *data_table = TableFactory::GetDataTable(
            db_id, table_id, schema, table_name, 1000, true, false, false,
            LayoutType::ROW);

        // 恢复索引
        data_table->SetMove_pos(table_record->GetMovePos());
        ReCreateIndex(table_record, data_table);
        db->AddTable(data_table);
    }

    LOG_TRACE("Recovery one table: %ud", table_id);

    fclose(ddl_file_handle_.file);
    ddl_file_handle_.file = nullptr;
}

// 重新创建索引，但是不恢复内容
void LogManager::ReCreateIndex(TableRecord *table_record,
                               DataTable *data_table) {
    auto schema = data_table->GetSchema();
    bool unique_keys = true;
    uint database_oid = 0;
    uint table_oid = 0;
    uint index_oid = 0;
    IndexType index_type = INDEXTYPE;
    IndexConstraintType index_constraint;

    // 恢复主键索引
    if (!table_record->GetPk_cols().empty()) {
        auto key_attrs = table_record->GetPk_cols();
        auto idx_name = table_record->GetPrimaryIndexName();
        auto key_schema = Schema::CopySchema(schema, key_attrs);
        key_schema->SetIndexedColumns(key_attrs);
        index_constraint = IndexConstraintType::PRIMARY_KEY;

        // set index metadata
        auto index_metadata = new IndexMetadata(
            idx_name, index_oid, table_oid, database_oid, index_type,
            index_constraint, schema, key_schema, key_attrs, unique_keys);

        // get index key length
        uint key_length = 0;
        for (const auto col : key_attrs) {
            if (schema->GetType(col) == TypeId::VARCHAR) {
                key_length += schema->GetColumn(col).GetLength() + 4;
            } else {
                key_length += schema->GetColumn(col).GetLength() + 1;
            }
        }

        Index *key_index = IndexFactory::GetIndex(index_metadata);
        key_index->SetKeyLength(key_length);
        data_table->AddIndex(key_index);
    }

    // 恢复其他索引
    if (!table_record->GetUniqueIndexInfo().empty()) {
        for (const auto &pair : table_record->GetUniqueIndexInfo()) {
            auto key_attrs = pair.second;
            auto idx_name = pair.first;
            auto key_schema = Schema::CopySchema(schema, key_attrs);
            key_schema->SetIndexedColumns(key_attrs);
            index_constraint = IndexConstraintType::PRIMARY_KEY;

            // set index metadata
            auto index_metadata = new IndexMetadata(
                idx_name, index_oid, table_oid, database_oid, index_type,
                index_constraint, schema, key_schema, key_attrs, unique_keys);

            // get index key length
            uint key_length = 0;
            for (const auto col : key_attrs) {
                if (schema->GetType(col) == TypeId::VARCHAR) {
                    key_length += schema->GetColumn(col).GetLength() + 4;
                } else {
                    key_length += schema->GetColumn(col).GetLength() + 1;
                }
            }

            Index *key_index = IndexFactory::GetIndex(index_metadata);
            key_index->SetKeyLength(key_length);
            data_table->AddIndex(key_index);
        }
    }

    data_table->SetPk_cols(table_record->GetPk_cols());
    data_table->SetPrimaryIndexName(table_record->GetPrimaryIndexName());
    data_table->SetUniqueIndexInfo(table_record->GetUniqueIndexInfo());
}

LogRecordType LogManager::GetNextDDLRecordType() {
    char buffer;
    // Check if the log record type is broken
    if (LoggingUtil::IsFileTruncated(ddl_file_handle_, 1)) {
        return LOGRECORD_TYPE_INVALID;
    }

    fread((void *)&buffer, 1, sizeof(char), ddl_file_handle_.file);
    CopySerializeInput input(&buffer, sizeof(char));
    auto log_record_type = (LogRecordType)(input.ReadByte());

    return log_record_type;
}

void StateMachineImpl::on_shutdown() {
    LOG(INFO) << "StateMachineImpl::on_shutdown";
}

void StateMachineImpl::on_leader_start(const mraft::LeaderStartContext &ctx) {
    LOG(INFO) << "StateMachineImpl::on_leader_start";
    (void)ctx;
    auto &log_manager = LogManager::GetInstance();
    log_manager.StartLoggingMode();
}

void StateMachineImpl::on_leader_stop(const butil::Status &status) {
    (void)status;
    auto &log_manager = LogManager::GetInstance();
    LOG(FATAL) << "StateMachineImpl::on_leader_stop Not implemented";
    // FIXME:
    // 1. roll back all uncommitted transactions
    // 2. clear all buffered logs
    // 3. stop frontend loggers' logging loop
}

void StateMachineImpl::on_start_following(
    const mraft::LeaderChangeContext &ctx) {
    (void)ctx;
    auto &log_manager = LogManager::GetInstance();
    log_manager.StartApplyMode();
}

void StateMachineImpl::on_stop_following(
    const mraft::LeaderChangeContext &ctx) {
    (void)ctx;
    auto &log_manager = LogManager::GetInstance();
    log_manager.TerminateLoggingMode();
}