//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// backend_logger.cpp
//
// Identification: src/logging/backend_logger.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend_logger.h"

#include "log_manager.h"
#include "log_record.h"
#include "logging_util.h"
#include "sql/log.h"
#include "storage/peloton/common/logger.h"
int logger_counter = 0;

// create a backend logger
BackendLogger::BackendLogger()
    : log_buffer_(std::unique_ptr<LogBuffer>(nullptr)),
      available_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())),
      persist_buffer_pool_(
          std::unique_ptr<BufferPool>(new CircularBufferPool())) {
    backend_pool.reset(new TupleValuePoll());
    frontend_logger_id = -1;
}

// destroy and cleanup
BackendLogger::~BackendLogger() {
    // __sync_fetch_and_sub(&logger_counter, 1);
    // sql_print_warning("Backend Logger Delete: %d", logger_counter);
    auto &log_manager = LogManager::GetInstance();
    if (!shutdown && frontend_logger_id != -1) {
        log_manager.GetFrontendLoggersList()[frontend_logger_id]
            .get()
            ->remove_backend_logger(this);
    }
}

/**
 * @brief log log a log record
 * @param log record
 */
void BackendLogger::Log(LogRecord *record) {
    // Enqueue the serialized log record into the queue
    record->Serialize(output_buffer);

    {
        this->log_buffer_lock.Lock();

        if (!log_buffer_) {
            //  LOG_TRACE("Acquire the first log buffer in backend logger");
            this->log_buffer_lock.Unlock();
            std::unique_ptr<LogBuffer> new_buff =
                std::move(available_buffer_pool_->Get());
            this->log_buffer_lock.Lock();
            log_buffer_ = std::move(new_buff);
        }

        // update the max logged id for the current buffer
        cid_t cur_log_id = record->GetTransactionId();

        // set if this is the max log_id seen so far
        if (cur_log_id > max_log_id_buffer) {
            log_buffer_->SetMaxLogId(cur_log_id);
            max_log_id_buffer = cur_log_id;
        }

        if (!log_buffer_->WriteRecord(record)) {
            //  LOG_TRACE("Log buffer is full - Attempt to acquire a new one");
            // put back a buffer
            max_log_id_buffer = 0;  // reset
            persist_buffer_pool_->Put(std::move(log_buffer_));
            this->log_buffer_lock.Unlock();

            // get a new one
            std::unique_ptr<LogBuffer> new_buff =
                std::move(available_buffer_pool_->Get());
            this->log_buffer_lock.Lock();
            log_buffer_ = std::move(new_buff);

            // write to the new log buffer
            auto success = log_buffer_->WriteRecord(record);
            assert(success);
        }

        // update max logged commit id
        if (record->GetType() == LOGRECORD_TYPE_TRANSACTION_COMMIT ||
            record->GetType() == LOGRECORD_TYPE_DDL_TABLE_CREATE) {
            auto new_log_commit_id = record->GetTransactionId();

            assert(new_log_commit_id > highest_logged_commit_message);
            highest_logged_commit_message = new_log_commit_id;
            commit_record_logged = true;
        }

        this->log_buffer_lock.Unlock();
    }
}

// this method is used by the frontend logger to give back flushed buffers to
// be used by the backend logger
void BackendLogger::GrantEmptyBuffer(std::unique_ptr<LogBuffer> empty_buffer) {
    available_buffer_pool_->Put(std::move(empty_buffer));
}

// set when the frontend logger is shutting down, prevents deadlock between
// frontend and backend
void BackendLogger::SetShutdown(bool val) { shutdown = val; }

cid_t BackendLogger::PrepareLogBuffers() {
    // 先将 log_buffer_ 里面剩余的内容写入 persist_buffer_pool_
    // 这里是 FrontendLogger 线程在调用，所以需要互斥访问
    {
        this->log_buffer_lock.Lock();

        // 获取 buffer 中剩余的记录
        if (log_buffer_ && log_buffer_->GetSize() > 0) {
            persist_buffer_pool_->Put(std::move(log_buffer_));
        }

        // update
        max_collected_commit_id = highest_logged_commit_message;

        // 当前事务的 prepare_record 正在被 FrontendLogger 收集，稍后就会被刷盘
        if (commit_record_logged) {
            commit_record_collected = true;

            // reset for next txn
            commit_record_logged = false;
        }

        // buffers 装车
        auto num_log_buffer = persist_buffer_pool_->GetSize();
        while (num_log_buffer > 0) {
            local_queue.push_back(persist_buffer_pool_->Get());
            num_log_buffer--;
        }

        this->log_buffer_lock.Unlock();
    }

    return max_collected_commit_id;
}

void BackendLogger::FlushLoggerFlushed() {
    {
        std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);
        flush_notify_cv.notify_all();
    }
}

void BackendLogger::WaitForFlush() {
    {
        std::unique_lock<std::mutex> wait_lock(flush_notify_mutex);

        // 等待最后一个记录被 FrontendLogger 收集并刷新
        flush_notify_cv.wait(wait_lock);

        // reset for next transaction
        commit_record_collected = false;
    }
}

void BackendLogger::SetHasTransaction(bool have) { has_transaction = have; }

bool BackendLogger::HasTransaction() const { return has_transaction; }
