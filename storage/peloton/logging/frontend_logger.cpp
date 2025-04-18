#include "storage/peloton/logging/frontend_logger.h"

#include <butil/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "common/log_entry.h"
#include "common/util.h"
#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/concurrency/epoch_manager_factory.h"
#include "storage/peloton/logging/log_manager.h"
#include "storage/peloton/logging/logging_util.h"
#include "storage/peloton/logging/records/transaction_record.h"

FrontendLogger::~FrontendLogger() {
    _stop_apply_thread();
    backend_loggers_lock.Lock();
    for (auto &bl : backend_loggers) {
        CHECK(bl != nullptr);
        bl->SetShutdown(true);
    }
    backend_loggers_lock.Unlock();
}

void FrontendLogger::init(FrontendLoggerOptions &options) {
    CHECK(options.stream != nullptr);
    _stream = options.stream;
    _logger_id = _stream->id();
    LOG(INFO) << "FrontendLogger init, id: " << _logger_id;
}

void FrontendLogger::do_recovery() {
    LOG(INFO) << "FrontendLogger start recovery, id: " << _logger_id;
    CHECK(_stream != nullptr);
    auto first_log_index = _stream->first_log_index();
    auto last_log_index = _stream->last_log_index();
    if (first_log_index > 1) {
        LOG(FATAL) << "first_log_index is larger than 1, first_log_index: "
                   << first_log_index;
        // TODO: Snapshoot recovery first
    }

    if (first_log_index != 1) {
        // In segment_storage
        // the first_log_index are initialized to 1,
        // last_log_index are 0
        CHECK_GE(last_log_index, first_log_index);
    }

    for (auto idx = first_log_index; idx <= last_log_index; idx++) {
        auto entry = _stream->get_log_entry(idx);
        if (entry->type != mraft::ENTRY_TYPE_DATA) {
            continue;
            LOG(WARNING) << "Skip no-data log entry: " << entry->type;
        }
        CHECK(entry != nullptr);
        _parse_one_log_entry(entry, _txn_map, _buffer);
        update_applied_index(idx);
    }

    LOG(INFO) << last_log_index - first_log_index + 1
              << " log entries recovered, id: " << _logger_id;
    auto &log_manager = LogManager::GetInstance();
    log_manager.NotifyRecoveryDone();
}

void FrontendLogger::add_backend_logger(BackendLogger *bl) {
    // Grant empty buffers
    for (int i = 0; i < BUFFER_POOL_SIZE; i++) {
        std::unique_ptr<LogBuffer> buffer(new LogBuffer(bl));
        bl->GrantEmptyBuffer(std::move(buffer));
    }
    // Add backend logger to the list of backend loggers
    backend_loggers_lock.Lock();
    bl->SetLoggingCidLowerBound(max_collected_committed_cid);
    backend_loggers.push_back(bl);
    backend_loggers_lock.Unlock();
}

void FrontendLogger::remove_backend_logger(BackendLogger *bl) {
    backend_loggers_lock.Lock();

    auto iter = std::find(backend_loggers.begin(), backend_loggers.end(), bl);
    if (iter != backend_loggers.end()) {
        backend_loggers.erase(iter);
    }

    backend_loggers_lock.Unlock();
}

bool FrontendLogger::has_alive_backend_logger() const {
    return _has_alive_backend_logger;
}

void FrontendLogger::_collect_log_records() {
    lower_bound_committed_cid = UINT64_MAX;
    _has_alive_backend_logger = true;
    {
        // Look at the local queues of the backend loggers
        backend_loggers_lock.Lock();
        for (auto backend_logger : backend_loggers) {
            CHECK(backend_logger != nullptr);
            auto back_max_committed_cid = backend_logger->PrepareLogBuffers();
            auto &log_buffers = backend_logger->GetLogBuffers();
            auto log_buffer_size = log_buffers.size();

            /* 当该线程没有事务正在运行的时候，可以不用收集此线程对应的
             * BackendLogger 的信息， 但有可能该 BackendLogger 中还有一个
             * commit_record，但 has_transaction 已经在提交阶段设置成了 false，
             * 所以还需要检查其 buffer 是否为空*/
            if (log_buffer_size <= 0 && !backend_logger->HasTransaction()) {
                continue;
            }

            // 更新下界
            if (back_max_committed_cid < lower_bound_committed_cid) {
                lower_bound_committed_cid = back_max_committed_cid;
            }

            // 更新上界
            if (back_max_committed_cid > max_collected_committed_cid) {
                LOG_TRACE("max_collected_committed_cid updated: %lu",
                          back_max_committed_cid);
                max_collected_committed_cid = back_max_committed_cid;
            }

            // Move the log record from backend_logger to here
            for (uint log_record_itr = 0; log_record_itr < log_buffer_size;
                 log_record_itr++) {
                global_queue.push_back(std::move(log_buffers[log_record_itr]));
            }

            // cleanup the local queue
            log_buffers.clear();
        }

        backend_loggers_lock.Unlock();
    }

    // 情况1：所有线程都在执行第一个事务，且都还没提交，此时
    // lower_bound_flushed_cid 也是初始值 INVALID_CID
    // 情况2：某一个线程正在执行第一个事务，但在这之前已经有事务提交过了，lower_bound_flushed_cid
    // 是有效值。
    if (lower_bound_committed_cid == INVALID_CID) {
        lower_bound_committed_cid = lower_bound_flushed_cid;
    }

    // 当前 FrontendLogger 管辖的 BackendLogger 没有在干活的了
    if (lower_bound_committed_cid == UINT64_MAX) {
        // 也就是说所有的事务目前是执行完了的，lower_bound 可以设置成最大值了
        lower_bound_committed_cid =
            std::max(lower_bound_committed_cid, max_collected_committed_cid);
        if (max_collected_committed_cid > lower_bound_flushed_cid) {
            lower_bound_flushed_cid = max_collected_committed_cid;
        }
        _has_alive_backend_logger = false;
    }
}

// TODO: 抽象出计时器，放到 task 中，细化地对每个步骤计时，可以分成三个步骤
// 1. 从 backend logger 中收集 log record
// 2. 从调用 _do_stabilize() 到发送 rpc / 落盘前
// 3. 落盘和 rpc 的延迟
void FrontendLogger::_logging_loop() {
    LOG(INFO) << "FlushLogger start logging loop, id: " << _logger_id;

    std::chrono::steady_clock::time_point start_time;
    std::chrono::steady_clock::time_point last_print_time =
        std::chrono::steady_clock::now();
    double total_latency_us = 0;
    int flush_count = 0;
    auto &log_manager = LogManager::GetInstance();

    while (log_manager.GetLoggingStatus() == LoggingStatusType::LOGGING) {
        _collect_log_records();

        if (global_queue.empty()) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(_wait_timeout_us));
        } else {
            start_time = std::chrono::steady_clock::now();
            _do_stabelize();
            auto end_time = std::chrono::steady_clock::now();
            auto latency_us =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time)
                    .count();

            // Limit the frequency of flushing
            std::this_thread::sleep_for(std::chrono::microseconds(
                std::max(0L, _wait_timeout_us - latency_us)));

            total_latency_us += latency_us;
            flush_count++;
        }

        auto current_time = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(current_time -
                                                             last_print_time)
                .count() >= 1) {
            if (flush_count > 0) {
                double avg_latency = total_latency_us / flush_count;
                std::cout << "Average latency for the last second: "
                          << avg_latency << " us" << std::endl;
            }
            total_latency_us = 0;
            flush_count = 0;
            last_print_time = current_time;
        }

        _update_global_lowerbound_flushed_cid();
    }

    LOG(INFO) << "FlushLogger stop logging loop, id: " << _logger_id;
}

void FrontendLogger::_do_stabelize() {
    if (global_queue.empty()) {
        return;
    }
    if (!_stream) {
        L_ERROR("log_stat_matchine_ is null");
        LOG(FATAL) << "log_stat_matchine_ is null";
    }

    uint64_t cur_flush_size_bytes = 0;

    entry_create_metric_timer.start();
    std::vector<std::shared_ptr<mraft::Task>> tasks;
    butil::IOBuf log;
    // First, write all the record in the queue
    for (auto &log_buffer : global_queue) {
        log.append(log_buffer->GetData(), log_buffer->GetSize());
        cur_flush_size_bytes += log_buffer->GetSize();
    }

    collected_buffer_cnt++;
    tasks.emplace_back(new mraft::Task);
    tasks.back()->data = &log;
    entry_create_metric_timer.end();

    stablelize_metric_timer.start();
    _stream->append_entries_async(tasks);

    // Second, wait for replication
    while (_stream->commit_index() < collected_buffer_cnt) {
        // FIXME: Any better way ?
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
    stablelize_metric_timer.end();

    _report_flush(cur_flush_size_bytes);

    for (auto &log_buffer : global_queue) {
        // return empty buffer
        auto backend_logger = log_buffer->GetBackendLogger();
        log_buffer->ResetData();
        backend_logger->GrantEmptyBuffer(std::move(log_buffer));
    }

    // update info
    if (lower_bound_committed_cid > lower_bound_flushed_cid) {
        // LOG_TRACE("lower_bound_flushed_cid updated: %lu",
        // lower_bound_committed_cid);
        lower_bound_flushed_cid = lower_bound_committed_cid;
    }

    // notify flush done
    backend_loggers_lock.Lock();
    for (auto logger : backend_loggers) {
        // 只有收集到对应 logger 的 prepare_record 时，刷新结束后才通知它
        if (logger->CommitRecordCollected()) {
            logger->FlushLoggerFlushed();
        }
    }
    backend_loggers_lock.Unlock();

    global_queue.clear();
}

void FrontendLogger::_report_flush(uint64_t cur_flush_size_bytes) {
    _avg_flush_size_bytes =
        (_avg_flush_size_bytes * _flush_count + cur_flush_size_bytes) /
        (_flush_count + 1);
    _totol_flush_size_bytes += cur_flush_size_bytes;
    _flush_count++;

    auto cur_time = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(cur_time -
                                                         _last_flush_time)
            .count() >= 1) {
        _last_flush_time = cur_time;
        auto avg_flush_size_MB = _avg_flush_size_bytes / 1024.0 / 1024.0;
        auto total_flush_size_MB = _totol_flush_size_bytes / 1024.0 / 1024.0;
        std::cout << "--- REPORT ---\n";
        std::cout << "Total flush times for the last second:  " << _flush_count
                  << std::endl;
        std::cout << "Average flush size for the last second:  "
                  << avg_flush_size_MB << " MB" << std::endl;
        std::cout << "Total flush size for the last second:  "
                  << total_flush_size_MB << " MB" << std::endl;
        _totol_flush_size_bytes = 0;
        _avg_flush_size_bytes = 0;
        _flush_count = 0;
    }
}

int FrontendLogger::_start_apply_thread() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    return bthread::execution_queue_start(&_apply_queue, &queue_options,
                                          _apply_thread, this);
}

int FrontendLogger::_stop_apply_thread() {
    bthread::execution_queue_stop(_apply_queue);
    return bthread::execution_queue_join(_apply_queue);
}

void FrontendLogger::apply(std::shared_ptr<mraft::LogEntry> entry) {
    CHECK_EQ(0, bthread::execution_queue_execute(_apply_queue, entry));
}

void FrontendLogger::start_apply_mode() {
    _txn_map.clear();
    LOG(INFO) << "FlushLogger start follower mode, id: " << _logger_id;

    _start_apply_thread();
    CHECK(_stream != nullptr);
    // TODO: tell the stream the _apply_queue。
}

void FrontendLogger::start_logging_mode() {
    LOG(INFO) << "FlushLogger start leader mode, id: " << _logger_id;
    std::thread(&FrontendLogger::_logging_loop, this).detach();
}

int FrontendLogger::_apply_thread(
    void *flush_logger,
    bthread::TaskIterator<std::shared_ptr<mraft::LogEntry>> &iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    FrontendLogger *logger = static_cast<FrontendLogger *>(flush_logger);

    for (; iter; ++iter) {
        shared_ptr<mraft::LogEntry> &entry = *iter;
        logger->_parse_one_log_entry(entry, logger->_txn_map, logger->_buffer);
        logger->update_applied_index(entry->id.index);
    }
    return 0;
}

void FrontendLogger::_update_global_lowerbound_flushed_cid() const {
    if (is_distinguished_logger) {
        cid_t global_lower_bound_flushed_commit_id = UINT64_MAX;

        auto &log_manager = LogManager::GetInstance();
        std::vector<std::unique_ptr<FrontendLogger>> &frontend_loggers =
            log_manager.GetFrontendLoggersList();

        for (auto &logger : frontend_loggers) {
            if (!logger->has_alive_backend_logger()) {
                continue;
            }
            global_lower_bound_flushed_commit_id =
                std::min(global_lower_bound_flushed_commit_id,
                         logger->lower_bound_flushed_cid);
        }

        // no active transaction
        if (global_lower_bound_flushed_commit_id == UINT64_MAX) {
            global_lower_bound_flushed_commit_id = INVALID_CID;
            for (auto &logger : frontend_loggers) {
                global_lower_bound_flushed_commit_id =
                    std::max(global_lower_bound_flushed_commit_id,
                             logger->lower_bound_flushed_cid);
            }
        }

        log_manager.SetGlobalLowerBoundFlushedCid(
            global_lower_bound_flushed_commit_id);
    }
}

void FrontendLogger::set_is_distinguished_logger(bool flag) {
    is_distinguished_logger = flag;
}

cid_t FrontendLogger::get_lower_bound_flushed_cid() const {
    return lower_bound_flushed_cid;
}

void FrontendLogger::apply_at(int64_t committed_index, FrontendLogger *logger) {
    auto &log_manager = LogManager::GetInstance();
    if (!log_manager.IsInApplyingMode()) {
        if (!log_manager.IsInLoggingMode()) {
            LOG(WARNING) << "Try to apply LogEntry, but LogManager is not in "
                            "applying mode and logging mode.";
        }
        return;
    }

    for (int64_t idx = logger->_applied_idx + 1; idx <= committed_index;
         idx++) {
        auto entry = logger->_stream->get_log_entry(idx);
        if (entry->type != mraft::ENTRY_TYPE_DATA) {
            LOG(INFO) << "Skip no-data log entry: " << entry->type;
            continue;
        }
        bthread::execution_queue_execute(logger->_apply_queue, entry);
    }
}

void FrontendLogger::_parse_one_log_entry(
    std::shared_ptr<mraft::LogEntry> log_entry, TransactionMap &txn_map,
    SimpleBuffer &buffer) {
    bool reached_end_of_log = false;
    LOG(INFO) << "size of log_entry: " << log_entry->data.size()
              << ", log_index: " << log_entry->id.index;
    // Go over each log record in the log entry
    while (!reached_end_of_log) {
        // Read the first byte to identify log record type
        // If that is not possible, then wrap up recovery
        auto record_type = LoggingUtil::GetNextLogRecordType(log_entry->data);
        cid_t txn_id = INVALID_CID;

        switch (record_type) {
            case LOGRECORD_TYPE_TRANSACTION_BEGIN: {
                // Check for torn log write
                TransactionRecord txn_rec(record_type);
                if (!LoggingUtil::ReadTransactionRecordHeader(
                        txn_rec, log_entry->data, buffer)) {
                    LOG(FATAL)
                        << "failed to Read Transaction-Begin Record Header ";
                    reached_end_of_log = true;
                    continue;
                }

                txn_id = txn_rec.GetTransactionId();
                txn_map[txn_id].has_start_record = true;
                txn_map[txn_id].log_index = log_entry->id.index;
                max_recovered_commit_id =
                    std::max(max_recovered_commit_id, txn_id);

                LOG(INFO) << "BeginRecord txn_id: " << txn_id
                          << ", log_index: " << log_entry->id.index;
                break;
            }
            case LOGRECORD_TYPE_TRANSACTION_COMMIT: {
                // Check for torn log write
                TransactionRecord txn_rec(record_type);
                if (!LoggingUtil::ReadTransactionRecordHeader(
                        txn_rec, log_entry->data, buffer)) {
                    LOG(FATAL)
                        << "failed to Read Transaction-Commit Record Header ";
                    return;
                }

                txn_id = txn_rec.GetTransactionId();
                CHECK(txn_map[txn_id].has_start_record);
                txn_map[txn_id].has_commit_record = true;

                LOG(INFO) << "CommitRecord txn_id: " << txn_id
                          << ", log_index: " << log_entry->id.index;

                if (txn_map[txn_id].has_start_record) {
                    // TODO: Run in a bthread
                    LOG(INFO) << "RecoveryOneTransaction txn_id: " << txn_id
                              << ", log_index: " << log_entry->id.index;
                    Records *records = txn_map[txn_id].records.release();
                    txn_map.erase(txn_id);
                    LoggingUtil::RecoveryOneTransaction(records);
                }
                break;
            }
            case LOGRECORD_TYPE_TUPLE_INSERT:
            case LOGRECORD_TYPE_TUPLE_UPDATE:
            case LOGRECORD_TYPE_TUPLE_DELETE: {
                std::shared_ptr<TupleRecord> tuple_record(
                    new TupleRecord(record_type));
                // Check for torn log write
                if (!LoggingUtil::ReadTupleRecordHeader(
                        *tuple_record, log_entry->data, buffer)) {
                    LOG_WARN("failed to Read Tuple Record Header");
                    reached_end_of_log = true;
                    continue;
                }

                txn_id = tuple_record->GetTransactionId();

                auto table = LoggingUtil::GetTable(*tuple_record);
                if (!table) {
                    LoggingUtil::SkipRecordBody(log_entry->data, buffer);
                    LOG_TRACE("Skip a tuple cause by no table, log id is %lu",
                              log_id);
                    continue;
                }

                const Schema *schema = nullptr;
                // if (record_type == LOGRECORD_TYPE_TUPLE_DELETE) {
                //     auto primary_index =
                //         table->GetIndexWithName(table->GetPrimaryIndexName());
                //     CHECK(primary_index != nullptr);
                //     schema = primary_index->GetKeySchema();
                // } else {
                    schema = table->GetSchema();
                // }

                CHECK(schema != nullptr)
                    << "record_type: " << record_type << ", txn_id: " << txn_id
                    << ", log_index: " << log_entry->id.index;
                // Read off the tuple record body from the log
                tuple_record->SetTuple(LoggingUtil::ReadTupleRecordBody(
                    schema, log_entry->data, buffer));

                CHECK(txn_map[txn_id].has_start_record);
                txn_map[txn_id].records->emplace_back(tuple_record);

                LOG(INFO) << "TupleRecord txn_id: " << txn_id
                          << ", log_index: " << log_entry->id.index;
                break;
            }
            default:
                reached_end_of_log = true;
                break;
        }
    }

    eid_t max_epoch_id_for_recovery = this->max_recovered_commit_id >> 32;
    uint32_t max_txn_id_for_recovery =
        this->max_recovered_commit_id - (max_epoch_id_for_recovery << 32);
    EpochManagerFactory::GetInstance().SetCurrentEpochId(
        max_epoch_id_for_recovery + 1);
    EpochManagerFactory::GetInstance().SetCurrentTxnId(max_txn_id_for_recovery +
                                                       1);
}