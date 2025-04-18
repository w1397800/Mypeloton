//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_logger.h
//
// Identification: src/include/logging/backend_logger.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>
#include <mutex>
#include <vector>

#include "circular_buffer_pool.h"
#include "log_buffer.h"
#include "log_record.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/common/synchronization/spin_latch.h"
#include "storage/peloton/type/abstract_pool.h"
#include "storage/peloton/type/tuple_value_pool.h"

//===--------------------------------------------------------------------===//
// Backend Logger
//===--------------------------------------------------------------------===//

class BackendLogger {
    friend class FrontendLogger;

   public:
    BackendLogger();

    ~BackendLogger();

    //===--------------------------------------------------------------------===//
    // Virtual Functions
    //===--------------------------------------------------------------------===//

    // Log the given record
    void Log(LogRecord *record);

    void SetLoggingCidLowerBound(cid_t cid) {
        // XXX bad synchronization practice
        log_buffer_lock.Lock();

        logging_cid_lower_bound = cid;

        highest_logged_commit_message = INVALID_CID;

        log_buffer_lock.Unlock();
    }

    // FIXME The following methods should be exposed to FrontendLogger only
    // Collect all log buffers to be persisted
    std::vector<std::unique_ptr<LogBuffer>> &GetLogBuffers() {
        return local_queue;
    }

    // Grant an empty buffer to use
    void GrantEmptyBuffer(std::unique_ptr<LogBuffer>);

    // Set FrontendLoggerID
    void SetFrontendLoggerID(int id) { frontend_logger_id = id; }

    // Get FrontendLoggerID
    int GetFrontendLoggerID() { return frontend_logger_id; }

    // set when the frontend is dying
    void SetShutdown(bool);

    // gets the Varlenpool used for log serialization
    AbstractPool *GetVarlenPool() { return backend_pool.get(); }

    // LogManager 在收到 COMMIT 类型的记录时，调用 WaitForFlush()
    // FlushLogger 每批次刷新时， 在 GetLogBuffers() 中设置
    // max_flushed_commit_id 为 max_seen_txn_id（后者在log()中更新） FlushLogger
    // 结束刷新时，调用对应 BufferLogger 的 WaitForFlush()
    // 这样的设置理论上比让所有 BufferLogger 等待 global_max_cid 效率高一些
    void FlushLoggerFlushed();
    void WaitForFlush();

    // 如果当前写入记录的事务还未将 commit_record
    // 写入，则返回的是上一个提交的事务 cid 如果当前事务已经写入了
    // commit_record, 则返回的时当前事务的 cid
    cid_t PrepareLogBuffers();

    // void SetMaxFlushedCid(cid_t cid) { max_flushed_commit_id = cid; }
    //  cid_t GetMaxFlushedCid() const {
    //      return max_flushed_commit_id;
    //  }

    // 在 LogManager->PrepareLogging() 中设置为 true
    // 在 LogManager→logCommit() 以及 LogManager→DoneLogging() 中设置为 false
    void SetHasTransaction(bool have);

    void SetMaxCollectedCid(cid_t cid) { max_collected_commit_id = cid; };

    bool HasTransaction(void) const;

    bool CommitRecordCollected(void) { return commit_record_collected; }

   protected:
    // the lock for the buffer being used currently
    SpinLatch log_buffer_lock;

    // temporary local_queue used by backend
    std::vector<std::unique_ptr<LogBuffer>> local_queue;

    // commit id of the highest value committed so far
    cid_t highest_logged_commit_message = INVALID_CID;

    // id of the corresponding frontend logger
    int frontend_logger_id = -1;  // default

    // lower bound for values this backend may commit
    cid_t logging_cid_lower_bound = INVALID_CID;

    // max cid for the current log buffer
    cid_t max_log_id_buffer = 0;

    // temporary serialization buffer
    CopySerializeOutput output_buffer;

    // the current buffer
    std::unique_ptr<LogBuffer> log_buffer_;

    // the pool of available buffers
    std::unique_ptr<BufferPool> available_buffer_pool_;

    // the pool of buffers to persist
    std::unique_ptr<BufferPool> persist_buffer_pool_;

    // varlen pool for serialization
    std::unique_ptr<AbstractPool> backend_pool;

    // shutdown flag
    bool shutdown = false;

    // 等待刷新用的锁
    std::mutex flush_notify_mutex;
    std::condition_variable flush_notify_cv;

    // buffers 提交给 FrontendLogger 后更新它
    cid_t max_collected_commit_id = INVALID_CID;

    // 标记当前线程是否有运行的事务（用于区分长事务和空闲线程）
    bool has_transaction = false;

    // commit_record 被装入 buffer 后设置为 true
    bool commit_record_logged = false;

    // commit_record 被 FrontendLogger::CollectLogRecordsFromBackendLoggers()
    // 收集后，设置为 true 只有它为 true 时，FrontendLogger 在 Flush
    // 结束后才会调用BackendLogger::FlushLoggerFlushed()释放信号量
    bool commit_record_collected = false;
};
