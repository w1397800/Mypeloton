#pragma once

#include <butil/logging.h>
#include <butil/status.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "common/configuration.h"
#include "common/raft.h"
#include "replica/configuration_manager.h"
#include "replica/election_module.h"
#include "replica/log_entry_manager.h"
#include "replica/log_service.h"
#include "replica/replication_common.h"
#include "replica/replicator.h"
#include "replica/state_machine.h"
#include "replica/stream.h"
#include "rpc/raft.pb.h"

namespace mraft {
// 1. management the LocalNode
// 2. redirect the rpc to the LocalNode

class Node {};

class RaftNode : public Node {
   public:
    RaftNode() = default;
    ~RaftNode() = default;
    struct RaftNodeOptions {
        RaftNodeOptions() = default;
        ~RaftNodeOptions() = default;

        std::string log_file_path;

        // If use gflag to set the config path, this path will be ignored
        std::string confg_file_path;

        // Pass to stream, details see LogStream::_log_manager
        LogManager* log_manager = nullptr;

        std::unique_ptr<StateMachine> state_machine = nullptr;

        int64_t catch_up_margin = 20;
    };

    butil::Status init(RaftNodeOptions& options);

    void shutdown(std::unique_ptr<Closure> done = nullptr);
    void join();

    // Start election and start working.
    // Called when database recovered all the logs.
    butil::Status start(bool enable_election = true);

    // Task:
    //     - butil::IOBuf* data;
    //     - Closure* done;
    // 将日志交给 RaftNode 处理
    // Not Allowed: multi-thread call
    butil::Status append_log_async(std::vector<std::shared_ptr<Task>>& tasks,
                          int32_t logger_id);

    // TODO:
    void append_log_sync(const Task& task);

    // Called when node become leader
    void on_node_become_leader();
    void TEST_on_node_become_leader_no_recovery();

    // Stream will call this function when it leader log recovery finished.
    void notify_one_stream_recovery_finished(int64_t stream_id);

    //  1. Leader: Node receive bigger term.
    //  2. Leader: Lease timeout.
    //  3. Follower: use this to reset state, prepare for replication.
    void on_node_become_follower(std::vector<PeerId> leader_ids);
    void on_node_become_follower(PeerId leader_id);
    // use "node0" as default leader.
    void TEST_on_node_become_follower();

    // lease expired => no leader
    void step_down(butil::Status status = butil::Status());

    // todo : need real calculated priority
    int64_t get_calculated_priority();

    int64_t stream_nums() const { return _streams.size(); }
    LogStream* get_stream(int64_t stream_id) {
        CHECK_LT(stream_id, _streams.size());
        return _streams[stream_id].get();
    }

    int64_t commit_index_of_stream(int64_t log_id) const;
    std::shared_ptr<LogEntry> get_log_entry(int64_t log_id, int64_t index);
    int64_t get_log_first_index(int64_t log_id) const;
    int64_t get_log_last_index(int64_t log_id) const;

    // get current term
    int64_t current_term() const { return _current_term; };
    RaftNodeState state() const { return _state; }

    int64_t get_leader_term() const { return _leader_id_term; };
    ElectionModule* get_election_module() { return _election_module.get(); }

    int64_t get_version_index();

    // Only used for test!!!
    // election module use it to update raft node's term
    void TEST_unsafe_reset_term(int64_t new_term);

    void reset_leader_id_term(int64_t new_leader_term);

    // Only used for test!!!
    void TEST_handle_append_entries_request(
        int64_t stream_id, brpc::Controller* cntl,
        const AppendEntriesRequest* request, AppendEntriesResponse* response,
        google::protobuf::Closure* done,
        bool from_append_entries_cache = false);

    // change leader manually
    void change_leader(PeerId dest_addr);

    int add_new_replicator_in_stream(const std::vector<PeerId> new_peers,std::string node_name);

    void on_caught_up(mraft::PeerId &addr, int64_t term,
                      int64_t version_index, butil::Status &st, std::string node_name);

    void check_on_caught_up_done(int64_t version_index,std::string node_name,bool succ);

    void leader_change_config();

    std::vector<LogDraft> get_log_drafts();

   private:
    void _start_log_recovery_from();

    bool _can_append_new_user_logs() const { return _state == STATE_LEADER; }

    // 节点状态
    RaftNodeState _state = STATE_UNINITIALIZED;

    // raft persistent state
    int64_t _current_term = INVALID_TERM;

    int64_t _leader_id_term = INVALID_TERM;

    // 每个日志流对应一个 stream
    std::vector<std::unique_ptr<LogStream>> _streams;

    // 选举模块
    std::unique_ptr<ElectionModule> _election_module;

    std::unique_ptr<ConfigurationManager> _config_manager;

    // config
    GroupId _group_id;
    PeerId _server_id;

    // For state change
    butil::Mutex _mutex;

    std::atomic<uint64_t> _recoveried_stream_nums{0};

    std::unique_ptr<StateMachine> _state_machine;

    std::unique_ptr<StateMachineCaller> _caller;

    std::map<std::string,std::atomic<int>> _adding_node;

    std::mutex _adding_node_mtx;

    int64_t _max_margin; // 追日志的最大边界
};

}  // namespace mraft