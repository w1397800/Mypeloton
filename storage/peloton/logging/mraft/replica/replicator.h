#pragma once

#include <brpc/channel.h>     // brpc::Channel
#include <bthread/bthread.h>  // bthread_id

#include <cstdint>
#include <memory>
#include <vector>

// #include "braft/storage.h"                       // SnapshotStorage
#include "common/configuration.h"  // Configuration
#include "common/raft.h"           // Closure
#include "common/types.h"          // ReplicatorStatus
#include "common/util.h"
#include "replica/log_collector.h"
#include "replica/log_entry_manager.h"  // LogEntryManager
#include "replica/replication_common.h"
#include "rpc/raft.pb.h"                // AppendEntriesRequest

namespace mraft {

class LogEntryManager;
class BallotBox;
class RaftNode;

class Replicator;
class ReplicatorLockGuard {
   public:
    ReplicatorLockGuard(bthread_id_t id, Replicator*& replicator)
        : _id(id), _succ(true), _locked(false) {
        if (bthread_id_lock(_id, (void**)&replicator) == 0) {
            _locked = true;
            CHECK(replicator != nullptr);
        } else {
            _succ = false;
        }
    }

    ~ReplicatorLockGuard() {
        if (_locked) {
            unlock();
        }
    }

    bool success() const { return _succ; }

    void unlock() {
        if (_locked && _succ) {
            if (bthread_id_unlock(_id) != 0) {
                PLOG(WARNING) << "Fail to unlock bthread_id: " << _id
                              << ", make sure you are in shutdown.";
            }
            _locked = false;
        }
    }

    int destory() {
        if (_locked && _succ) {
            _locked = false;
            return bthread_id_unlock_and_destroy(_id);
        }
        return 0;
    }

   private:
    bthread_id_t _id;
    bool _succ;
    bool _locked;  // Indicate whether the object holds the lock
};

struct ReplicatorOptions {
    ReplicatorOptions() = default;
    // Name of this clustor
    GroupId group_id;

    // Id of this server
    PeerId server_id;

    // Id of this replicator instance
    PeerId peer_id;

    int64_t term = 0;

    // Used to Get LogEntry
    LogEntryManager* ln_manager = nullptr;

    // For vote
    BallotBox* ballot_box = nullptr;

    LogStream* stream = nullptr;

    LogCollector* log_collector = nullptr;

    bool is_learner = false;
};

struct ReplicatorManagerOptions {
    ReplicatorManagerOptions() = default;
    LogEntryManager* ln_manager = nullptr;
    BallotBox* ballot_box = nullptr;
    LogStream* stream = nullptr;
    LogCollector* log_collector = nullptr;

    int64_t term = 0;
    GroupId group_id;
    PeerId server_id;
};

class CatchupClosure : public Closure {
   public:
    virtual void Run() = 0;

   protected:
    CatchupClosure()
        : _max_margin(0), _has_timer(false), _error_was_set(false) {}

   private:
    friend class Replicator;
    int64_t _max_margin;
    bthread_timer_t _timer;
    bool _has_timer;
    bool _error_was_set;
    void _run();
};

class Replicator {
   public:
   Replicator() : mtimer("AppendEntriesRPC laytancy") {}
    // 用于初始化一个 replicator 实例
    static int start(ReplicatorOptions&, ReplicatorId* id);

    static int start_replication(const ReplicatorId& id);

    // Leader 卸任时，用于关闭 replicator 实例
    static int stop(ReplicatorId);

    // 1. LogManager 在每当有新日志到来时，都会调用每个 replicator 的该函数
    // 2. 如果该函数发现当前有新的 LogEntry 可以复制，在新的 bthread 中执行
    // send_entries() 所以实际上该函数是在 LogManger append_entries
    // 的执行线程中执行的，和 replicator 日志复制线程无关。
    // 并且这个逻辑也比较容易修改成 pthread，与 bthread 偶核心较低。
    void try_send_entries_in_new_bthread(int64_t log_index);

    static void wait_for_caught_up(ReplicatorId, int64_t max_margin,
                                   CatchupClosure* done);

   private:
    enum St {
        IDLE,
        BLOCKING,
        APPENDING_ENTRIES,
        INSTALLING_SNAPSHOT,
        LEADER_RECOVERY,
        DESTOIED,
    };
    struct Stat {
        St st;
        union {
            int64_t first_log_index;
            int64_t last_log_included;
        };
        union {
            int64_t last_log_index;
            int64_t last_term_included;
        };
    };

    ~Replicator() = default;

    // call AppendEntries RPC to send log entries.
    // Leader recovery do not limit the number of entries to send.
    void send_entries();

    // 修改 replicator 的任期，从 RaftNode 传递下来
    void reset_term(int64_t new_term);

    // 1. used when rpc error occurs, block for a while and retry.
    // 2. after blocking timeout, retry AppendEntries RPC.
    void _block(long start_time_us, int error_code);
    static void _on_block_timedout(void* arg);
    static void* _on_block_timedout_in_new_thread(void* arg);

    // used when receive AppendEntriesResponse
    // 1. Ignore responce not in _append_entries_in_fly.
    // 2. If the rpc failed, sleep a will and retry.
    // 3. If responce is not success.
    //      3.1 If recive Larger term: TODO: stepdown().
    //      3.2 If recive less last_log_index: Reset replicator's next_index.
    static void _on_append_entries_rpc_returned(ReplicatorId id,
                                                brpc::Controller* cntl,
                                                AppendEntriesRequest* request,
                                                AppendEntriesResponse* response);

    // Allways vote for each LogEntry and return success.
    // Only for test !
    static void TEST_fake_on_rpc_returned(ReplicatorId id,
                                          brpc::Controller* cntl,
                                          AppendEntriesRequest* request,
                                          AppendEntriesResponse* response,
                                          int64_t);

   private:
    void _send_pull_entries_request(int64_t start_index);
    static void _on_pull_entries_rpc_returned(ReplicatorId id,
                                              brpc::Controller* cntl,
                                              PullEntriesRequest* request,
                                              PullEntriesResponse* response,
                                              int64_t rpc_send_time);

    // 填充 rpc 除了日志外的其它内容
    int _fill_common_fields(AppendEntriesRequest* request,
                            int64_t prev_log_index);
    // 获取正在发送的 rpc 中，最小的日志索引
    int64_t _min_flying_index() {
        return _next_index - _flying_append_entries_size;
    }
    // 1. 通过 offset 计算出日志在 LogEntryManager 中的索引。
    // 2. 然后将日志内容填充到 AppendEntriesRequest 中
    // total_data_size: the size of all the LogEntries in current
    // AppendEntriesRPC
    int _prepare_entry_meta(int offset, EntryMeta* em, butil::IOBuf* data,
                            int64_t total_data_size);

    // Cancel and clear all the rpcs in sending,
    // Reset the _next_index to befor sending.
    void _cancel_sending_rpcs_and_stop_sending_rpc();

    void _allow_sending_rpc();

    void _notify_on_caught_up(int error_code, bool before_destroy);

    bool _is_catchup(int64_t max_margin) {
        // We should wait until install snapshot finish. If the process is
        // throttled, it maybe very slow.
        // We should wait until install snapshot finish. If the process is
        // throttled, it maybe very slow.
        if (_next_index < _ln_manager->first_log_index()) {
            return false;
        }
        LOG(INFO) << "goal index is " << _ln_manager->last_log_index()
                  << " and now index is " << _min_flying_index() - 1 + max_margin;
        if (_min_flying_index() - 1 + max_margin <
            _ln_manager->last_log_index()) {
            return false;
        }
        return true;
    }

    // 通过发送空 rpc，获取 follower 的 next_index
    // 从而更新 replicator 的 _next_index
    void _send_empty_entries();

    // Cancel flying AppendEntries RPCs
    // 1. Used in stop()
    void _cancel_append_entries_rpcs();

    // 1. Used in stop()
    void _destroy();

    // 1. Called when blocking timeout (details see _block())
    // 2. Called by LogEntryManager when new log entry available.
    static int _continue_sending(void* arg, int error_code);
    struct _ContinueSendingArg {
        bthread_id_t id;
        int error_code;
    };
    // pthread version
    static void* _continue_sending(void* arg);

    struct FlyingAppendEntriesRpc {
        int64_t log_index;
        int entries_size;
        brpc::CallId call_id;
        FlyingAppendEntriesRpc(int64_t index, int size, brpc::CallId id)
            : log_index(index), entries_size(size), call_id(id) {}
    };

    std::list<brpc::CallId> _pull_entries_rpc_in_fly;

    bool is_learner() { return _is_learner; }

    brpc::Channel _sending_channel;

    // next log index to send
    int64_t _next_index;

    // the size of LogEntries in sending
    int64_t _flying_append_entries_size;

    Stat _st;

    // LogEntires in sending
    std::deque<FlyingAppendEntriesRpc> _append_entries_rpc_in_fly;

    // the id of the bthread which is running this replicator,
    // used for locking the replicator
    bthread_id_t _id;

    // consecutive rpc error times
    int32_t _consecutive_rpc_error_times;

    // 1. set to true when rpc error occurs, and block sending rpc for a while.
    //    in case of unnecessary network traffic
    // 2. set back to false when blocking timeout TODO: finish that
    bool _stop_sending_rpc{false};  // _is_waiter_canceled
    //===--------------------------------------------------------------------===//
    // From ReplicatorOptions
    //===--------------------------------------------------------------------===//
    GroupId _group_id;
    PeerId _server_id;
    PeerId _peer_id;

    // Get LogEntry
    LogEntryManager* _ln_manager = nullptr;

    // For vote
    BallotBox* _ballot_box;

    LogStream* _stream;

    LogCollector* _log_collector;

    // 每个 replicator 只对应一个 term，如果 term 变更，说明当前节点不再是
    // Leader
    int64_t _term;

    bool _is_learner = false;

    CatchupClosure* _catchup_closure;

    bool _has_succeeded;

    MetricTimer mtimer;

};

/*
生命周期：
    ReplicatorGroup 生命周期和 Stream 一致。
    Stream 初始化时，会同时初始化 ReplicatorManager。
开启日志复制：
    RaftNode 在当选 Leader 后，
*/

class ReplicatorManager {
   public:
    ReplicatorManager() = default;
    ~ReplicatorManager() = default;

    void init(ReplicatorManagerOptions&);

    // 在当前节点当选 Leader 时会调用
    // 根据 peer 创建一个 Replicator 实例
    // 会建立 channel 并立即开始尝试日志复制！
    int add_replicator(const PeerId& peer,  bool is_learner = false);

    // Change the replicators' state to AppendEntries by calling
    // r->_send_empty_entries()
    void start_replication();

    void start_new_replication(const PeerId& peer);

    // Called only when configure change
    int stop_replicator(const PeerId& peer);

    // Called when Leader stepdown or deconsruct
    // 关闭所有 Replicator
    int stop_all();

    // 1. 在运行途中，如果 Leader term 增加（OB算法中的情况），调用该接口变更
    // term
    // TODO: 严格设计 term 从变更到传递到 Replicator 的过程
    // 2. When node become leader, before start replicator, reset term.
    int reset_term(int64_t new_term);

    int64_t wait_catchup(const PeerId& peer, int64_t max_margin,
                         CatchupClosure* done);

   private:
    // 存储 peer_id -> replicator_id 的映射，ReplicatorId 用于在 bthread
    // 中互斥获取 replicator 实例
    std::map<PeerId, ReplicatorId> _rmap;

    // ReplicatorGroup 的 Options 中，peer_id 为本节点的配置
    // Used in add_replicator(), will give it to each replicator
    ReplicatorOptions _common_options;

    LogEntryManager* ln_manager;
    BallotBox* ballot_box;
    RaftNode* node;
};

}  // namespace mraft
#
