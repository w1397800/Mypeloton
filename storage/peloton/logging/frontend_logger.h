#pragma once

#include <butil/binary_printer.h>

#include <cstdint>
#include <memory>

#include "my_inttypes.h"
#include "storage/peloton/logging/logging_util.h"
#pragma once

#include <bthread/execution_queue.h>  // bthread::ExecutionQueueId
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <thread>
#include <vector>

#include "backend_logger.h"
#include "buffer_pool.h"
#include "common/log_entry.h"
#include "log_buffer.h"
#include "replica/stream.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/common/synchronization/spin_latch.h"
#include "storage/peloton/logging/records/tuple_record.h"

#define kDEFAULT_MIN_WAIT_TIME_US 100

/*
 *     Tuple Record :
 *       - LogRecordType         : enum+
 *     -HEADER
 *       - Header length         : int
 *       - Database Oid          : uint
 *       - Table Oid             : uint
 *       - Transaction Id        : txn_id_t
 *     -BODY
 *       - Body length           : int
 *       - Data                  : void*
 */

//===--------------------------------------------------------------------===//
// Frontend Logger
//===--------------------------------------------------------------------===//

typedef std::vector<shared_ptr<TupleRecord>> Records;
struct FakeTransaction {
    FakeTransaction() { records.reset(new Records); }
    int64_t log_index = -1;
    bool has_start_record = false;
    bool has_commit_record = false;

    std::unique_ptr<Records> records;
};

using TransactionMap = std::unordered_map<txn_id_t, FakeTransaction>;

struct FrontendLoggerOptions {
    mraft::LogStream *stream = nullptr;
};

class FrontendLogger {
   public:
    //    mraft::MetricTimer timer0("LogEntry Create latancy");
    // mraft::MetricTimer timer1("Stabelize latancy");
    FrontendLogger()
        : entry_create_metric_timer("LogEntry Create latancy"),
          stablelize_metric_timer("Stabelize latancy") {}

    ~FrontendLogger();

    void init(FrontendLoggerOptions &options);

    void do_recovery();

    void apply(std::shared_ptr<mraft::LogEntry> entry);

    // Called by LogManager, to get ready for the comming of LogEntries.
    void start_apply_mode();
    void start_logging_mode();

    //===--------------------------------------------------------------------===//
    // For flush
    //===--------------------------------------------------------------------===//
   public:
    void add_backend_logger(BackendLogger *backend_logger);
    void set_logger_id(int);
    void remove_backend_logger(BackendLogger *bl);
    bool has_alive_backend_logger() const;

   private:
    void _collect_log_records(void);
    void _logging_loop(void);
    void _do_stabelize(void);
    void _report_flush(uint64_t cur_flush_size_bytes);

    //===--------------------------------------------------------------------===//
    // For recovery
    //===--------------------------------------------------------------------===//
   public:
    void set_is_distinguished_logger(bool flag);
    cid_t get_lower_bound_flushed_cid() const;
    static void apply_at(int64_t committed_index,
                         FrontendLogger *logger = nullptr);

   private:
    void _update_global_lowerbound_flushed_cid() const;

    void _parse_one_log_entry(std::shared_ptr<mraft::LogEntry> entry,
                              TransactionMap &txn_map, SimpleBuffer &buffer);
    // static void _log_entry_parser(std::shared_ptr<mraft::LogEntry> entry,
    //                               TransactionMap &txn_map);
    static int _apply_thread(
        void *frontend_logger,
        bthread::TaskIterator<std::shared_ptr<mraft::LogEntry>> &iter);
    int _start_apply_thread();
    int _stop_apply_thread();
    void update_applied_index(int64_t index) {
        CHECK_GT(index, _applied_idx);
        _applied_idx = index;
    }

   private:
    // Associated backend loggers
    std::vector<BackendLogger *> backend_loggers;
    SpinLatch backend_loggers_lock;

    // Buffer queue
    std::vector<std::unique_ptr<LogBuffer>> global_queue;

    // When flushLogger do not collect any log records from backend loggers
    // sleep for a while.
    int _wait_timeout_us = kDEFAULT_MIN_WAIT_TIME_US;

    uint32_t _logger_id;

    mraft::LogStream *_stream = nullptr;

    //===--------------------------------------------------------------------===//
    // For flush
    //===--------------------------------------------------------------------===//

    // 所有 BackendLogger 线程中，最小的 max_flushed_cid
    // 在刷盘后更新其为 min_max_committed_cid
    cid_t lower_bound_flushed_cid = INVALID_CID;

    // 在收集 buffer 时候更新为
    // 从所有client线程中得到的最大commit_cid中选取最小的那个，
    // 比它还小的事务肯定都已经提交过了
    cid_t lower_bound_committed_cid = INVALID_CID;

    // 在收集 buffer 时更新，记录最大的已经提交了的 cid
    cid_t max_collected_committed_cid = INVALID_CID;

    // UpdateGlobalLowerBoundFlushedCid() 时，会根据它判断当前 FlushLogger 的
    // lower_bound_flushed_cid 是否有效。 其实只有一个场景会用到：所有
    // BackendLogger
    // 都都没有正在执行的事务，都执行完了之后，Global_lower_bound_flushed_cid
    // 需要设置成所有 FlushLogger->lower_bound_flushed_cid
    // 中的最大值，以便从当前时间点进行检查点。
    bool _has_alive_backend_logger = false;

    cid_t max_recovered_commit_id = INVALID_CID;

    // for log replication，当 replicated_buffer_cnt == collected_buffer_cnt
    // 时， 说明这一批 buffer 都被复制了，可以提交了。
    int64_t collected_buffer_cnt = 0;

    //===--------------------------------------------------------------------===//
    // For recovery
    //===--------------------------------------------------------------------===//

    // Distinguished logger will update TransactionManager's transaction ID
    bool is_distinguished_logger = false;

    TransactionMap _txn_map;
    SimpleBuffer _buffer;  // 暂存从 IOBuf 中复制出来的数据，用于解析 record。
    bthread::ExecutionQueueId<std::shared_ptr<mraft::LogEntry>> _apply_queue;
    // CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, done));
    int64_t _applied_idx = 0;

    //===--------------------------------------------------------------------===//
    // For benchmark test
    //===--------------------------------------------------------------------===//
    uint64_t _avg_flush_size_bytes = 0;
    uint64_t _totol_flush_size_bytes = 0;
    uint64_t _flush_count = 0;
    std::chrono::steady_clock::time_point _last_flush_time =
        std::chrono::steady_clock::now();

    mraft::MetricTimer entry_create_metric_timer;
    mraft::MetricTimer stablelize_metric_timer;
};