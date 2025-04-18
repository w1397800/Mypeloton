#include "replica/log_entry_manager.h"

#include <butil/logging.h>
#include <butil/status.h>
#include <gflags/gflags.h>

#include <cassert>
#include <memory>
#include <vector>

#include "common/log_entry.h"
#include "common/util.h"
#include "replica/replicator.h"
#include "replica/stream.h"
#include "storage/memory_log.h"
#include "storage/segment_log.h"
#include "storage/storage.h"
// #include "storage/stable_log.h"

namespace mraft {

DEFINE_string(raft_log_storage_type, "segment",
              "the max size of out-of-order append entries cache");

// set false if need not flush log (use for test)
DEFINE_bool(mraft_need_flush, true, "Is the log need to be flush and sync");

void LogEntryManager::init(const LogEntryManagerOptions& options) {
    CHECK(options.node);
    _node = options.node;
    _stream = options.stream;

    if (FLAGS_raft_log_storage_type == "memory") {
        _log_storage.reset(new MemoryLogStorage(options.log_storage_path));
    } else if (FLAGS_raft_log_storage_type == "stable") {
        // _log_storage.reset(new StableLogStorage(options.log_storage_path));
        LOG(FATAL) << "Deleted stable log storage";
    } else if (FLAGS_raft_log_storage_type == "segment")
        _log_storage.reset(new SegmentLogStorage(options.log_storage_path));
    else {
        LOG(FATAL) << "Unknown log storage type: "
                   << FLAGS_raft_log_storage_type;
    }
    // CHECK(options.conf_manager);
    _log_storage->init(options.conf_manager);

    _first_log_index = _log_storage->first_log_index();
    _last_log_index = _log_storage->last_log_index();
    _flushed_id.index = _last_log_index;
    // Term will be 0 if the node has no logs, and we will correct the value
    // after snapshot load finish.
    _flushed_id.term = _log_storage->get_term(_last_log_index);

    start_disk_thread();
}

void LogEntryManager::shutdown() {
    stop_disk_thread();
    _logs_in_memory.clear();
}

void LogEntryManager::append_entries_async(
    std::vector<std::shared_ptr<LogEntry>>&& entries, StableClosure* done,
    bool from_collector) {
    assert(!entries.empty());
    CHECK(done);

    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!entries.empty() && _check_and_resolve_conflict(entries, done) != 0) {
        LOG(INFO) << "Invalied log index, nothing todo";
        return;
    }

    for (const auto& entry : entries) {
        if (entry->type == ENTRY_TYPE_CONFIGURATION) {
            // TODO: configuration change log
        }
    }

    if (!entries.empty()) {
        // The closure from LogCollector are allready set the first_log_index
        if (!from_collector) {
            done->_first_log_index = entries.front()->id.index;
        }

        _logs_in_memory.insert(_logs_in_memory.end(), entries.begin(),
                               entries.end());
    }

    done->_entries.swap(entries);

    // Flush to stable storage.
    // Task are executed in detached bthread, see disk_thread() for details.
    CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, done));

    _notify_all_replicator(lck);
}

std::shared_ptr<LogEntry> LogEntryManager::get_entry(const int64_t index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // out of range, direct return NULL
    if (index > _last_log_index || index < _first_log_index) {
        return NULL;
    }

    auto entry = _get_entry_from_memory(index);
    if (entry) {
        return entry;
    }

    lck.unlock();
    entry = _log_storage->get_entry(index);
    if (!entry) {
        LOG(ERROR) << EIO << "Corrupted entry at index=% " << PRId64 << index;
    }
    return entry;
}

void LogEntryManager::_notify_all_replicator(
    std::unique_lock<raft_mutex_t>& lck) {
    for (const auto& rep : _replicator_set) {
        assert(rep);
        rep->try_send_entries_in_new_bthread(_last_log_index);
    }
    lck.unlock();
}

int LogEntryManager::_check_and_resolve_conflict(
    std::vector<std::shared_ptr<LogEntry>>& entries, StableClosure* done) {
    AsyncClosureGuard done_guard(done);

    // CASE-1: Node is currently the leader and |entries| are from the user who
    // don't know the correct indexes the logs should assign to. So we
    // have to assign indexes to the appending entries
    if (entries.front()->id.index == 0) {
        for (size_t i = 0; i < entries.size(); ++i) {
            entries[i]->id.index = ++_last_log_index;
        }
        (void)done_guard.release();
        return 0;
    } else {
        // Node is currently a follower and |entries| are from the leader. We
        // should check and resolve the confliction between the local logs
        // and |entries|
    }

    // CASE-2: The unordered entries are not allowed in LogEntryManager,
    // They will be processed or cached in the implementation of the
    // append entries RPC in the Stream
    if (entries.front()->id.index > _last_log_index + 1) {
        done->status().set_error(EINVAL,
                                 "There's gap between first_index=%" PRId64
                                 " and last_log_index=%" PRId64,
                                 entries.front()->id.index, _last_log_index);
        LOG(WARNING) << "There's gap between first_index="
                     << entries.front()->id.index
                     << " and last_log_index=" << _last_log_index;
        return -1;
    }

    // CASE-3: The entries allready applied, just ignore
    const int64_t applied_index = _applied_id.index;
    if (entries.back()->id.index <= applied_index) {
        LOG(WARNING) << "Received entries of which the last_log="
                     << entries.back()->id.index
                     << " is not greater than _applied_index=" << applied_index
                     << ", return immediately with nothing changed";
        return 1;
    }

    // CASE-4: Check and solve the confliction.
    if (entries.front()->id.index == _last_log_index + 1) {
        // Fast path
        _last_log_index = entries.back()->id.index;
    } else {
        CHECK_GT(_last_log_index + 1, entries.front()->id.index);
        LOG(WARNING) << "Appending entries overlap the local ones.";
        // Appending entries overlap the local ones. We should find if
        // there is a conflicting index from which we should truncate the
        // local ones.
        // K: This happens when the leader changed. The log with bigger term
        // will replace the local ones.
        size_t conflicting_index = 0;
        for (; conflicting_index < entries.size(); ++conflicting_index) {
            auto local_log_term =
                _get_term_nolock(entries[conflicting_index]->id.index);
            if (local_log_term != entries[conflicting_index]->id.term) {
                CHECK_GT(entries[conflicting_index]->id.term, local_log_term);
                break;
            }
        }

        if (conflicting_index != entries.size()) {
            if (entries[conflicting_index]->id.index <= _last_log_index) {
                // Truncate all the conflicting entries to make local logs
                // consensus with the leader.
                _truncate_suffix_nolock(entries[conflicting_index]->id.index -
                                        1);
            }
            _last_log_index = entries.back()->id.index;
        } else {
            // else this is a duplicated AppendEntriesRequest, we have
            // nothing to do besides releasing all the entries
        }

        // Release all the entries before the conflicting_index and the rest
        // would be append to _logs_in_memory and _log_storage after
        // this function returns
        entries.erase(entries.begin(), entries.begin() + conflicting_index);

        // if (entries.empty()) {
        //     return -1;
        // };
    }
    (void)done_guard.release();
    return 0;
}

int64_t LogEntryManager::_get_term_nolock(const int64_t index) {
    if (index == 0) {
        return 0;
    }
    // TODO: Add snapshoot logic

    // out of range, direct return NULL
    // check this after check last_snapshot_id, because it is likely that
    // last_snapshot_id < first_log_index
    if (index > _last_log_index || index < _first_log_index) {
        return 0;
    }

    auto entry = _get_entry_from_memory(index);
    if (entry) {
        return entry->id.term;
    }

    return _log_storage->get_term(index);
}

int64_t LogEntryManager::first_log_index() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    return _first_log_index;
}

int64_t LogEntryManager::last_log_index(bool is_flush) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (!is_flush) {
        return _last_log_index;
    } else {
        // if (_last_log_index == _last_snapshot_id.index) {
        //     return _last_log_index;
        // }
        LastLogIdClosure c;
        CHECK_EQ(0, bthread::execution_queue_execute(_disk_queue, &c));
        lck.unlock();
        c.wait();
        return c.last_log_id().index;
    }
}

LogId LogEntryManager::last_log_id(bool is_flush) {
    (void)is_flush;
    LOG(FATAL) << "Not implemented";
    return LogId();
}

// 功能：去掉某个 replicator，在成员变更时使用
void LogEntryManager::remove_replicator(Replicator* rep) {
    CHECK(rep != nullptr);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _replicator_set.erase(rep);
}

// 功能：注册 replicator，在当选 Leader 时使用
void LogEntryManager::register_replicator(Replicator* rep) {
    CHECK(rep != nullptr);
    std::unique_lock<raft_mutex_t> lck(_mutex);
    CHECK(_replicator_set.find(rep) == _replicator_set.end());
    _replicator_set.insert(rep);
}

int64_t LogEntryManager::get_term(const int64_t index) {
    if (index == 0) {
        return 0;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);

    return _get_term_nolock(index);
}

void LogEntryManager::_truncate_suffix_nolock(const int64_t last_index_kept) {
    if (last_index_kept < _applied_id.index) {
        LOG(FATAL) << "Can't truncate logs before _applied_id="
                   << _applied_id.index
                   << ", last_log_kept=" << last_index_kept;
        return;
    }

    // 1. Remove the logs in memory
    while (!_logs_in_memory.empty()) {
        auto entry = _logs_in_memory.back();
        if (entry->id.index > last_index_kept) {
            _logs_in_memory.pop_back();
        } else {
            break;
        }
    }
    _last_log_index = last_index_kept;
    const int64_t last_term_kept = _get_term_nolock(last_index_kept);
    CHECK(last_index_kept == 0 || last_term_kept != 0)
        << "last_index_kept=" << last_index_kept;

    // TODO: truncate when configure manager finished
    // _config_manager->truncate_suffix(last_index_kept);

    // 3. Remove the logs in LogStorage async
    CHECK_EQ(0, bthread::execution_queue_execute(
                    _disk_queue, new TruncateSuffixClosure(last_index_kept,
                                                           last_term_kept)));
}

int LogEntryManager::start_disk_thread() {
    bthread::ExecutionQueueOptions queue_options;
    queue_options.bthread_attr = BTHREAD_ATTR_NORMAL;
    return bthread::execution_queue_start(&_disk_queue, &queue_options,
                                          disk_thread, this);
}

int LogEntryManager::stop_disk_thread() {
    bthread::execution_queue_stop(_disk_queue);
    return bthread::execution_queue_join(_disk_queue);
}

LogDraft LogEntryManager::get_log_draft() {
    LogDraft draft;
    draft.first_log_index = _first_log_index;
    draft.last_log_index = _last_log_index;

    for (auto i = draft.first_log_index; i <= draft.last_log_index; ++i) {
        auto entry = get_entry(i);
        if (entry) {
            draft.ids.emplace_back(entry->id);
        }
    }

    return draft;
}

void LogEntryManager::_append_to_storage(
    std::vector<std::shared_ptr<LogEntry>>& to_append, LogId* last_flushed_id,
    IOMetric* metric) {
    if (!_has_error.load(butil::memory_order_relaxed)) {
        butil::Timer timer;
        timer.start();
        int nappent = _log_storage->append_entries(to_append, metric);
        // LOG(INFO) << "APPEND A LOG, size: " << to_append.front()->data.size()
        //           << ", stream " << _stream->get_stream_id();
        timer.stop();
        if (nappent != (int)to_append.size()) {
            // FIXME
            LOG(ERROR) << "Fail to append_entries, "
                       << "nappent=" << nappent
                       << ", to_append=" << to_append.size();
            // report_error(EIO, "Fail to append entries");
        }
        if (nappent > 0) {
            *last_flushed_id = to_append[nappent - 1]->id;
        }
    }
}

std::shared_ptr<LogEntry> LogEntryManager::_get_entry_from_memory(
    const int64_t index) {
    if (!_logs_in_memory.empty()) {
        int64_t first_index = _logs_in_memory.front()->id.index;
        int64_t last_index = _logs_in_memory.back()->id.index;
        CHECK_EQ(last_index - first_index + 1,
                 static_cast<int64_t>(_logs_in_memory.size()));
        if (index >= first_index && index <= last_index) {
            return _logs_in_memory[index - first_index];
        }
    }
    return nullptr;
}

DEFINE_int32(raft_max_append_buffer_size, 256 * 1024,
             "Flush buffer to LogStorage if the buffer size reaches the limit");

// K: The AppendBatcher is used to batch the entries to be appended to the
// LogStorage.
class AppendBatcher {
   public:
    AppendBatcher(LogId* last_id, LogEntryManager* lm, MetricTimer* metric)
        : _last_flushed_id(last_id), _lm(lm), _metric(metric){
        _entries.reserve(8);
        _closures.reserve(8);
    }
    ~AppendBatcher() { flush(); }

    // K:
    // 1. Flush all the entries to the LogStorage
    // 2. Run all the closures
    void flush() {
        if (_entries.empty()) {
            return;
        }
        if (FLAGS_mraft_need_flush) {
            _metric->start();
            _lm->_append_to_storage(_entries, _last_flushed_id);
            _metric->end();
        }
        for (auto& closure : _closures) {
            closure->Run();
        }
        _entries.clear();
        _closures.clear();
    }

    // Once the buffer size reaches the limit, will flush the buffer
    void append(LogEntryManager::StableClosure* done) {
        if (_entries.size() >= (size_t)FLAGS_raft_max_append_buffer_size) {
            flush();
        }
        _closures.push_back(done);
        _entries.insert(_entries.end(), done->_entries.begin(),
                        done->_entries.end());
    }

   private:
    std::vector<LogEntryManager::StableClosure*> _closures;
    std::vector<std::shared_ptr<LogEntry>> _entries;
    LogId* _last_flushed_id;
    LogEntryManager* _lm;
    MetricTimer* _metric;
};

int LogEntryManager::disk_thread(void* ln_manager,
                                 bthread::TaskIterator<StableClosure*>& iter) {
    if (iter.is_queue_stopped()) {
        return 0;
    }

    LogEntryManager* log_entry_manager =
        static_cast<LogEntryManager*>(ln_manager);
    LogId last_flushed_id = log_entry_manager->_flushed_id;

    // Will update last_flushed_id
    AppendBatcher ab(&last_flushed_id, log_entry_manager, &log_entry_manager->_metric);

    for (; iter; ++iter) {
        auto done = *iter;
        // done->metric.bthread_queue_time_us =
        //     butil::cpuwide_time_us() - done->metric.start_time_us;

        // K: LogEntry closure: LeaderStable or FollowerStable
        if (done->type == StableClosureType::LEADER ||
            done->type == StableClosureType::FOLLOWER) {
            if (!done->_entries.empty()) {
                ab.append(done);
            } else {
                done->Run();
            }
            continue;
        }

        // K: Other closure, async run those functions
        ab.flush();
        int ret = 0;
        if (done->type == StableClosureType::LAST_LOGID) {
            // Not used log_manager->get_disk_id() as it might be out of
            // date
            // FIXME: it's buggy
            LastLogIdClosure* llic = dynamic_cast<LastLogIdClosure*>(done);
            CHECK(llic);
            llic->set_last_log_id(last_flushed_id);
        } else if (done->type == StableClosureType::TRUNCATE_SUFFIX) {
            TruncateSuffixClosure* tsc =
                dynamic_cast<TruncateSuffixClosure*>(done);
            CHECK(tsc);
            ret = log_entry_manager->_log_storage->truncate_suffix(
                tsc->last_index_kept());
            if (ret == 0) {
                // update last_id after truncate_suffix
                last_flushed_id.index = tsc->last_index_kept();
                last_flushed_id.term = tsc->last_term_kept();
                CHECK(last_flushed_id.index == 0 || last_flushed_id.term != 0)
                    << "last_id=" << last_flushed_id;
            }
        } else if (done->type == StableClosureType::TRUNCATE_PREFIX) {
            TruncatePrefixClosure* tpc =
                dynamic_cast<TruncatePrefixClosure*>(done);
            CHECK(tpc);
            ret = log_entry_manager->_log_storage->truncate_prefix(
                tpc->first_index_kept());
        } else if (done->type == StableClosureType::RESET_CLOSURE) {
            ResetClosure* rc = dynamic_cast<ResetClosure*>(done);
            CHECK(rc);
            ret = log_entry_manager->_log_storage->reset(rc->next_log_index());
        } else {
            CHECK(false) << "Can't enter here !";
        }

        if (ret != 0) {
            LOG(ERROR) << "Failed operation on LogStorage, ret=" << ret;
        }
        done->Run();
    }
    CHECK(!iter) << "Must iterate to the end";
    ab.flush();
    log_entry_manager->set_flushed_id(last_flushed_id);
    return 0;
}

void LogEntryManager::set_applied_id(const LogId& applied_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);  // Race with set_disk_id
    if (applied_id < _applied_id) {
        return;
    }
    _applied_id = applied_id;
    LogId clear_id = std::min(_flushed_id, _applied_id);
    lck.unlock();
    return _clear_memory_logs_lower_than(clear_id);
}

void LogEntryManager::set_flushed_id(const LogId& disk_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);  // Race with set_applied_id
    if (disk_id < _flushed_id) {
        return;
    }
    _flushed_id = disk_id;
    // FIXME: Any better way to decide whether to clear logs in memory?
    // LogId clear_id = std::min(_flushed_id, _applied_id);
    LogId clear_id = _flushed_id;  // Leader does not call set_applied_id()
    lck.unlock();
    return _clear_memory_logs_lower_than(clear_id);
}

void LogEntryManager::_clear_memory_logs_lower_than(const LogId& id) {
    BAIDU_SCOPED_LOCK(_mutex);
    while (!_logs_in_memory.empty()) {
        auto entry = _logs_in_memory.front();
        if (entry->id > id) {
            break;
        }
        _logs_in_memory.pop_front();
    }
}
}  // namespace mraft