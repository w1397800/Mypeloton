#pragma once

#include <bthread/execution_queue.h>    // bthread::ExecutionQueueId
#include <butil/containers/flat_map.h>  // butil::FlatMap
#include <butil/logging.h>
#include <butil/macros.h>  // BAIDU_CACHELINE_ALIGNMENT
#include <butil/synchronization/lock.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <deque>  // std::deque
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/log_entry.h"  // LogEntry
#include "common/raft.h"       // Closure
#include "common/types.h"
#include "common/util.h"  // raft_mutex_t
#include "replica/configuration_manager.h"
#include "storage/storage.h"  // Storage

namespace mraft {

class LogStorage;
class StateMachinCaller;
class RaftNode;
class Replicator;
class LogStream;

struct LogEntryManagerOptions {
    LogEntryManagerOptions() = default;
    // LogStorage* log_storage{nullptr};
    std::string log_storage_path;

    RaftNode* node = nullptr;

    LogStream* stream = nullptr;

    ConfigurationManager* conf_manager = nullptr;
};

enum class StableClosureType {
    LEADER = 1,
    FOLLOWER = 2,
    LAST_LOGID = 3,
    TRUNCATE_SUFFIX = 4,
    TRUNCATE_PREFIX = 5,
    RESET_CLOSURE = 6,
};

// The log info of all the log entries in storage can get
struct LogDraft {
    // All the log entries in storage
    std::vector<LogId> ids;
    int64_t first_log_index = 0;
    int64_t last_log_index = 0;
};

class LogEntryManager {
   public:
    // used for log entry stablelize
    class StableClosure : public Closure {
       public:
        StableClosure() : _first_log_index(0) {}
        StableClosureType type{};

       protected:
        int64_t _first_log_index;
        // used for performance monitoring
        IOMetric metric;

       private:
        friend class LogEntryManager;
        friend class AppendBatcher;
        std::vector<std::shared_ptr<LogEntry>> _entries;
    };

    LogEntryManager() : _metric("Diks Flulsh") {};
    ~LogEntryManager() = default;
    void init(const LogEntryManagerOptions& options);

    void shutdown();

    // 功能：将日志交给 LogEntryManage
    // done：日志写入持久化存储后的回调函数
    // @entries: get ownership of entries
    void append_entries_async(std::vector<std::shared_ptr<LogEntry>>&& entries,
                              StableClosure* done, bool from_collector = false);

    // The detached thread function for disk operations
    // Related to this->_disk_queue
    // 1. Append entries to disk
    // 2. Truncate prefix
    // 3. Truncate suffix
    // 4. Set last log id
    // 5. Reset
    static int disk_thread(void* ln_manager,
                           bthread::TaskIterator<StableClosure*>& iter);
    int start_disk_thread();
    int stop_disk_thread();

    std::shared_ptr<LogEntry> get_entry(const int64_t index);

    // Get the term of the given log entry's index.
    // 1. First try to get the term from memory
    // 2. If not found, try to get the term from storage
    int64_t get_term(const int64_t index);

    // Not thread safe
    int64_t _get_term_nolock(const int64_t index);

    // 功能：第一个 LogEntry 的索引，用于判断快照的位置
    int64_t first_log_index();

    // 功能：最后一个 LogEntry 的索引
    int64_t last_log_index(bool is_flush = false);

    LogId last_log_id(bool is_flush = false);

    // 功能：去掉某个 replicator，在成员变更时使用
    void remove_replicator(Replicator* rep);

    // 功能：注册 replicator，在当选 Leader 时使用
    void register_replicator(Replicator* rep);

    // 功能：设置已经应用的日志索引
    void set_applied_id(const LogId& applied_id);

    void set_flushed_id(const LogId& disk_id);

    LogDraft get_log_draft();

   private:
    friend class AppendBatcher;

    // 功能：将日志写入持久化存储，同步写入，函数返回即刷盘成功
    // @metric: used for performance monitoring
    // @last_flushed_id: Get the updated last_flushed_id
    inline void _append_to_storage(
        std::vector<std::shared_ptr<LogEntry>>& to_append,
        LogId* last_flushed_id, IOMetric* metric = nullptr);

    // 功能：从内存获取缓存的日志文件
    // @return: User do not own the returned LogEntry, can't use delete!
    std::shared_ptr<LogEntry> _get_entry_from_memory(const int64_t index);

    // 功能：Clear the logs in memory whose id <= the given |id|
    // 通常情况下，在日志复制到所有活跃节点上之后，可以删除内存中的部分
    void _clear_memory_logs_lower_than(const LogId& id);

    // 功能：依次调用 Replicator 注册的回调函数
    // - 在新的 bthread 中调用，避免阻塞当前线程
    void _notify_all_replicator(std::unique_lock<raft_mutex_t>& lck);

    // 1. Update the value of _last_log_index
    // 2. Leader: Set the LogEntry's index if it's not set.
    // 3. Follower: Drop the unordered entries.
    // 4. Follower: Drop the out of date entries. (may idle too long in net)
    // 5. Follower: Over lap the recieved entries. (when leader change)
    int _check_and_resolve_conflict(
        std::vector<std::shared_ptr<LogEntry>>& entries, StableClosure* done);

    void _truncate_suffix_nolock(const int64_t last_index_kept);

   private:
    // 日志持久化模块
    std::unique_ptr<LogStorage> _log_storage;
    // ConfigurationManager* _config_manager;

    // 用于获取节点基本信息比如 term
    RaftNode* _node;

    LogStream* _stream;

    // 用于注册 replicator，日志来临时调用其 _try_send_entries_in_new_bthread()
    // add in register_replicator()
    // delete in remove_replicator()
    // No need to use shared_ptr, LogEntryManager do not delete replicator by
    // itself
    std::unordered_set<Replicator*> _replicator_set;

    // applied last log id
    LogId _applied_id;
    // flushed last log id
    LogId _flushed_id;
    // last log received, update in _check_and_resolve_conflict()
    int64_t _last_log_index;
    // First log index of log storage, larger than 0 when snapshoot is installed
    int64_t _first_log_index{0};

    // 缓存在内存中的日志
    std::deque<std::shared_ptr<LogEntry>> _logs_in_memory;

    // 大锁，互斥访问 LogManager，单线程不太需要
    butil::Mutex _mutex;

    // Taskqueue for log storage operations
    // Bind to a detached bthread: disk_thread()
    bthread::ExecutionQueueId<StableClosure*> _disk_queue;

    std::atomic<bool> _has_error{false};

    MetricTimer _metric;
};

class TruncatePrefixClosure : public LogEntryManager::StableClosure {
   public:
    explicit TruncatePrefixClosure(const int64_t first_index_kept)
        : _first_index_kept(first_index_kept) {
        type = StableClosureType::TRUNCATE_PREFIX;
    }
    void Run() { delete this; }
    int64_t first_index_kept() const { return _first_index_kept; }

   private:
    int64_t _first_index_kept;
};

class TruncateSuffixClosure : public LogEntryManager::StableClosure {
   public:
    TruncateSuffixClosure(int64_t last_index_kept, int64_t last_term_kept)
        : _last_index_kept(last_index_kept), _last_term_kept(last_term_kept) {
        type = StableClosureType::TRUNCATE_SUFFIX;
    }
    void Run() { delete this; }
    int64_t last_index_kept() const { return _last_index_kept; }
    int64_t last_term_kept() const { return _last_term_kept; }

   private:
    int64_t _last_index_kept;
    int64_t _last_term_kept;
};

class ResetClosure : public LogEntryManager::StableClosure {
   public:
    explicit ResetClosure(int64_t next_log_index)
        : _next_log_index(next_log_index) {
        type = StableClosureType::RESET_CLOSURE;
    }
    void Run() { delete this; }
    int64_t next_log_index() const { return _next_log_index; }

   private:
    int64_t _next_log_index;
};

class LastLogIdClosure : public LogEntryManager::StableClosure {
   public:
    LastLogIdClosure() { type = StableClosureType::LAST_LOGID; }
    void Run() { _event.signal(); }
    void set_last_log_id(const LogId& log_id) {
        CHECK(log_id.index == 0 || log_id.term != 0)
            << "Invalid log_id=" << log_id;
        _last_log_id = log_id;
    }
    LogId last_log_id() const { return _last_log_id; }

    void wait() { _event.wait(); }

   private:
    bthread::CountdownEvent _event;
    LogId _last_log_id;
};

}  // namespace mraft
