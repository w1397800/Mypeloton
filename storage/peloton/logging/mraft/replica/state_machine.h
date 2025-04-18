#pragma once

#include <cstring>
#include <memory>

// #include "replica/raft_node.h"
#include <bthread/bthread.h>
#include <bthread/execution_queue.h>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"

namespace mraft {

class LeaderChangeContext {
    DISALLOW_COPY_AND_ASSIGN(LeaderChangeContext);

   public:
    LeaderChangeContext(std::vector<PeerId> leader_ids, int64_t term)
        : leader_ids(leader_ids), _term(term){};
    // for on_start_following, the leader_id and term are of the new leader;
    // for on_stop_following, the leader_id and term are of the old leader.
    const std::vector<PeerId>& leader_id() const { return leader_ids; }
    int64_t term() const { return _term; }

   private:
    std::vector<PeerId> leader_ids;
    int64_t _term;
};

struct LeaderStartContext {
    LeaderStartContext(int64_t term_) : term(term_) {}

    int64_t term;
};

class StateMachine {
   public:

    // TODO: find a better way to pass the LogEntries
    virtual void on_apply();

    // Invoked once when the raft node was shut down.
    // Default do nothing
    virtual void on_shutdown();

    // Invoked when the belonging node becomes the leader of the group at |term|
    // Default: Do nothing
    virtual void on_leader_start(const LeaderStartContext& ctx);

    // Invoked when this node steps down from the leader of the replication
    // group and |status| describes detailed information
    virtual void on_leader_stop(const butil::Status& status);

    // this method is called when a follower stops following a leader and its
    // leader_id becomes NULL, situations including:
    // 1. handle lease_timeout
    // 2.
    virtual void on_stop_following(const LeaderChangeContext& ctx);

    // this method is called when a follower or candidate starts following a
    // leader and its leader_id (should be NULL before the method is called) is
    // set to the leader's id, situations including:
    // 1. a candidate receives append_entries from a leader
    // 2. election chooses a leader
    // 3.
    virtual void on_start_following(const LeaderChangeContext& ctx);
};

enum CallerTaskType {
    IDLE,
    COMMITTED,
    LEADER_STOP,
    LEADER_START,
    START_FOLLOWING,
    STOP_FOLLOWING,
    ERROR,
};

struct StateMachineTask {
    CallerTaskType type;

    // For applying log entry (including configuration change)
    int64_t committed_index = 0;

    // For on_leader_start
    LeaderStartContext* leader_start_context = nullptr;

    // For on_leader_stop
    butil::Status* status = nullptr;

    // For on_start_following and on_stop_following
    LeaderChangeContext* leader_change_context = nullptr;

    // For other operation
    Closure* done = nullptr;
};

struct StateMachineCallerOptions {
    StateMachineCallerOptions() = default;
    ~StateMachineCallerOptions() = default;
    StateMachine* state_machine = nullptr;
};

class StateMachineCaller {
   public:
    StateMachineCaller() = default;
    ~StateMachineCaller() { shutdown(); };
    void init(const StateMachineCallerOptions& options);
    void shutdown();
    void on_committed(int64_t committed_index);
    void on_leader_stop(const butil::Status& status);
    void on_leader_start(int64_t term);
    void on_start_following(const LeaderChangeContext& start_following_context);
    void on_stop_following(const LeaderChangeContext& stop_following_context);
    void join();

   private:
    static int run(void* meta, bthread::TaskIterator<StateMachineTask>& iter);

    bthread::ExecutionQueueId<StateMachineTask> _queue_id;

    StateMachine* _state_machine;

    CallerTaskType _cur_task;
};
}  // namespace mraft
