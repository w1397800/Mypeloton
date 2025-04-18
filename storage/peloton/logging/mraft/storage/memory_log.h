#pragma once

#include <butil/atomicops.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

#include <deque>
#include <vector>

#include "common/log_entry.h"
#include "common/util.h"
#include "storage/storage.h"

namespace mraft {

class BAIDU_CACHELINE_ALIGNMENT MemoryLogStorage : public LogStorage {
   public:
    MemoryLogStorage(const std::string& path)
        : _path(path), _first_log_index(1), _last_log_index(0) {}
    MemoryLogStorage() : _first_log_index(1), _last_log_index(0) {}

    ~MemoryLogStorage() { reset(1); }

    int init(ConfigurationManager* configuration_manager);

    // first log index in log
    int64_t first_log_index() {
        return _first_log_index.load(butil::memory_order_acquire);
    }

    // last log index in log
    int64_t last_log_index() {
        return _last_log_index.load(butil::memory_order_acquire);
    }

    // get logentry by index
    std::shared_ptr<LogEntry> get_entry(const int64_t index);

    // get logentry's term by index
    int64_t get_term(const int64_t index);

    // append entries to log
    int append_entry(const std::shared_ptr<LogEntry> entry);

    // append entries to log and update IOMetric, return append success number
    int append_entries(const std::vector<std::shared_ptr<LogEntry>>& entries,
                       IOMetric* metric = nullptr);

    // delete logs from storage's head, [first_log_index, first_index_kept) will
    // be discarded
    int truncate_prefix(const int64_t first_index_kept);

    // delete uncommitted logs from storage's tail, (last_index_kept,
    // last_log_index] will be discarded
    int truncate_suffix(const int64_t last_index_kept);

    // Drop all the existing logs and reset next log index to |next_log_index|.
    // This function is called after installing snapshot from leader
    int reset(const int64_t next_log_index);

    // Create an instance of this kind of LogStorage with the parameters encoded
    // in |uri|
    // Return the address referenced to the instance on success, NULL otherwise.
    LogStorage* new_instance(const std::string& uri) const;

    // GC an instance of this kind of LogStorage with the parameters encoded
    // in |uri|
    butil::Status gc_instance(const std::string& uri) const;

   private:
    std::string _path;
    butil::atomic<int64_t> _first_log_index;
    butil::atomic<int64_t> _last_log_index;
    std::deque<std::shared_ptr<LogEntry>> _log_entry_data;
    raft_mutex_t _mutex;
};

}  //  namespace mraft

