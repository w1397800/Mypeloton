#include "raft_node.h"

#include <bthread/bthread.h>
#include <butil/logging.h>
#include <butil/status.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <type_traits>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"
#include "replica/configuration_manager.h"
#include "replica/election_utils.h"
#include "replica/state_machine.h"
#include "replica/stream.h"

namespace mraft {

class OnCaughtUp : public CatchupClosure {
   public:
    OnCaughtUp(RaftNode *node, int64_t term, const PeerId &peer,
               int64_t version, std::string node_name)
        : _node(node),
          _term(term),
          _peer(peer),
          _version(version),
          _node_name(node_name) {}
    virtual void Run() {
        _node->on_caught_up(_peer, _term, _version, status(), _node_name);
        delete this;
    };

   private:
    RaftNode *_node;
    int64_t _term;
    PeerId _peer;
    int64_t _version;
    std::string _node_name;
};

butil::Status RaftNode::init(RaftNodeOptions &options) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _state = STATE_UNINITIALIZED;

    // StateMachine initialization
    if (!options.state_machine) {
        _state_machine.reset(new StateMachine());
    } else {
        _state_machine.reset(options.state_machine.release());
    }
    StateMachineCallerOptions caller_options;
    caller_options.state_machine = _state_machine.get();
    _caller.reset(new StateMachineCaller());
    _caller->init(caller_options);

    // Configuration Manager initialization
    ConfigurationManagerOptions cfg_options;
    cfg_options.config_file_path = options.confg_file_path;
    _config_manager.reset(new ConfigurationManager());
    auto st = _config_manager->init(std::move(cfg_options));
    if (!st.ok()) {
        LOG(ERROR) << "ConfigurationManager init failed: " << st.error_str();
        return st;
    } else {
        LOG(INFO) << "ConfigureManagerInit: " << _config_manager->to_string();
    }

    // Stream initialization
    auto stream_nums = _config_manager->get_stream_nums();
    for (int64_t stream_id = 0; stream_id < stream_nums; stream_id++) {
        LogStreamOptions stream_options;
        stream_options.group_id = _config_manager->get_group_id();
        auto server_ids = _config_manager->get_server_ids();
        CHECK(stream_nums == static_cast<int64_t>(server_ids.size()));
        if (stream_nums != static_cast<int64_t>(server_ids.size())) {
            LOG(ERROR) << "stream_nums=" << stream_nums
                       << ", server_ids.size()=" << server_ids.size();
            return butil::Status(EINVAL, "stream_nums != server_ids.size()");
        }
        stream_options.server_id = server_ids[stream_id];
        stream_options.peers = _config_manager->get_config_at_index(stream_id);
        stream_options.log_file_path = options.log_file_path + "_" +
                                       _config_manager->get_name() + "_" +
                                       "stream" + std::to_string(stream_id);
        stream_options.stream_id = stream_id;
        stream_options.log_manager = options.log_manager;
        stream_options.raft_node = this;
        stream_options.config_manager = _config_manager.get();

        _streams.emplace_back(new LogStream());
        auto st = _streams.back()->init(stream_options);
        if (!st.ok()) {
            LOG(ERROR) << "LogStream init failed: " << st.error_str();
            return st;
        }
    }

    // Channel use the first stream
    auto self_addr =
        _config_manager->get_server_ids_by_name(_config_manager->get_name())
            .at(0);
    auto priority =
        _config_manager->get_priority_by_name(_config_manager->get_name());
    _election_module.reset(new ElectionModule(
        self_addr, priority, _config_manager->get_server_ids_of_election(),
        this));

    _max_margin = options.catch_up_margin;

    // Wait for database recovery
    _state = STATE_LOG_RECOVERY;
    return butil::Status::OK();
}

void RaftNode::shutdown(std::unique_ptr<Closure> done) {
    {
        std::unique_lock<raft_mutex_t> lck(_mutex);

        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " start shutdown,"
                     " current_term "
                  << _current_term << " state " << state2str(_state);

        if (_state != STATE_SHUTDOWN && _state != STATE_END) {
            // change state to shutdown
            _state = STATE_SHUTDOWN;

            // shutdown all streams
            for (auto &stream : _streams) {
                stream->shutdown();
            }

            if (_caller) {
                _caller->shutdown();
            }

            if (_election_module) {
                _election_module->shutdown();
            }
        }
    }  // out of _mutex;

    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " shutdown done, current_term " << _current_term << " state "
              << state2str(_state);

    if (done) {
        run_closure_in_bthread(done.release());
    }
}

butil::Status RaftNode::start(bool enable_election) {  // Election module init
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _state = STATE_FOLLOWER;

    for (auto &stream : _streams) {
        auto st = stream->start_log_server();
        if (!st.ok()) {
            LOG(ERROR) << "Start log server failed: " << st.error_str();
            return st;
        }
    }

    if (enable_election) {
        _election_module->start();
    } else {
        LOG(WARNING) << "Election is disabled, make sure you are in test mode";
    }
    return butil::Status::OK();
}

butil::Status RaftNode::append_log_async(
    std::vector<std::shared_ptr<Task>> &tasks, int32_t logger_id) {
    // std::unique_lock<raft_mutex_t> lck(_mutex);

    // The number of FrontendLogger must be the same as stream's
    if (logger_id >= static_cast<int32_t>(_streams.size())) {
        LOG(FATAL) << "logger_id=" << logger_id << " is invalid";
        return butil::Status(EINVAL, "logger_id is invalid");
    }

    if (!_can_append_new_user_logs()) {
        butil::Status st;
        st.set_error(EPERM, "is not leader");
        LOG(ERROR) << "node " << _group_id << ":" << _server_id
                   << " can't apply : " << st << ", state is " << _state;

        for (auto &task : tasks) {
            if (task->done) {
                task->done->status() = st;
                run_closure_in_bthread(task->done.release());
            }
        }
        return st;
    }

    std::vector<std::shared_ptr<LogEntry>> entries;
    entries.reserve(tasks.size());

    _streams[logger_id]->append_entries_async(tasks);
    return butil::Status::OK();
}

void RaftNode::on_node_become_leader() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    LOG(INFO) << "Node become LEADER_CANDIDATE, last state is "
              << state2str(_state);
    _state = STATE_LEADER_CANDIDATE;
    _recoveried_stream_nums = 0;
    for (auto &stream : _streams) {
        stream->on_node_become_leader();
    }
}

void RaftNode::TEST_on_node_become_leader_no_recovery() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    LOG(INFO) << "Node become LEADER_CANDIDATE, last state is "
              << state2str(_state);
    _state = STATE_LEADER_CANDIDATE;

    LOG(INFO) << "now raft node's term is " << _current_term;
    for (auto &stream : _streams) {
        stream->on_node_become_leader();
    }

    _caller->on_leader_start(_current_term);
}

void RaftNode::notify_one_stream_recovery_finished(int64_t stream_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_state != STATE_LEADER_CANDIDATE) {
        LOG(WARNING) << "Node state is " << state2str(_state)
                     << ", can't notify stream recovery finished";
        return;
    }

    LOG(INFO) << "Stream " << stream_id << " leader recovery finished";
    if (++_recoveried_stream_nums == _streams.size()) {
        LOG(INFO) << "All stream leader recovery finished, now node can work "
                     "as Leader";
        _state = STATE_LEADER;
        _caller->on_leader_start(_current_term);
    }
}

void RaftNode::on_node_become_follower(std::vector<PeerId> leader_ids) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    CHECK_EQ(leader_ids.size(), _streams.size());
    _state = STATE_FOLLOWER;

    for (std::size_t i = 0; i < _streams.size(); i++) {
        _streams[i]->on_node_become_follower(_current_term, leader_ids[i]);
    }

    LeaderChangeContext ctx(leader_ids, _current_term);
    _caller->on_start_following(ctx);
}

void RaftNode::TEST_on_node_become_follower() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    auto leader_ids = _config_manager->get_server_ids_by_name("node0");
    CHECK_EQ(leader_ids.size(), _streams.size());
    _state = STATE_FOLLOWER;

    for (std::size_t i = 0; i < _streams.size(); i++) {
        _streams[i]->on_node_become_follower(_current_term, leader_ids[i]);
    }

    LeaderChangeContext ctx(leader_ids, _current_term);
    _caller->on_start_following(ctx);
}

void RaftNode::on_node_become_follower(mraft::PeerId leader_id) {
    on_node_become_follower(
        _config_manager->get_all_leader_id_by_peer_id(leader_id));
}

void RaftNode::step_down(butil::Status status) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _state = STATE_FOLLOWER;

    for (std::size_t i = 0; i < _streams.size(); i++) {
        _streams[i]->step_down();
    }

    // TODO: Let election module decide why the step down happend
    _caller->on_leader_stop(status);
}

void RaftNode::reset_leader_id_term(int64_t new_leader_term) {
    _leader_id_term = new_leader_term;
}

void RaftNode::TEST_handle_append_entries_request(
    int64_t stream_id, brpc::Controller *cntl,
    const AppendEntriesRequest *request, AppendEntriesResponse *response,
    google::protobuf::Closure *done, bool from_append_entries_cache) {
    _streams[stream_id]->handle_append_entries_request(
        cntl, request, response, done, from_append_entries_cache);
}

void RaftNode::TEST_unsafe_reset_term(int64_t new_term) {
    if (_current_term >= new_term) {
        LOG(ERROR) << "current term is " << _current_term << ", new term is "
                   << new_term << ",which is smaller!";
    }
    _current_term = new_term;
}

void RaftNode::change_leader(mraft::PeerId dest_addr) {
    _election_module->_proposer->change_leader_to(dest_addr);
}

int64_t RaftNode::get_calculated_priority() {
    return _election_module->_proposer->get_origin_priority();
}

int64_t RaftNode::commit_index_of_stream(int64_t log_id) const {
    CHECK_LT(log_id, _streams.size());
    return _streams[log_id]->commit_index();
}

std::shared_ptr<LogEntry> RaftNode::get_log_entry(int64_t log_id,
                                                  int64_t index) {
    CHECK_LT(log_id, _streams.size());
    return _streams[log_id]->get_log_entry(index);
}

int64_t RaftNode::get_log_first_index(int64_t log_id) const {
    CHECK_LT(log_id, _streams.size());
    return _streams[log_id]->first_log_index();
}

int64_t RaftNode::get_log_last_index(int64_t log_id) const {
    CHECK_LT(log_id, _streams.size());
    return _streams[log_id]->last_log_index();
}

std::vector<LogDraft> RaftNode::get_log_drafts() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    std::vector<LogDraft> drafts;
    for (auto &stream : _streams) {
        drafts.emplace_back(stream->get_log_draft());
    }
    return drafts;
}

int64_t RaftNode::get_version_index() {
    return _election_module->_proposer->get_member_list_version_index();
}

int RaftNode::add_new_replicator_in_stream(const std::vector<PeerId> new_peers,
                                           std::string node_name) {
    if (new_peers.size() != _streams.size()) {
        LOG(ERROR)
            << "the number of the new node's stream is mismatch the leader's";
        return -1;
    }

    {
        std::lock_guard<std::mutex> lck(_adding_node_mtx);
        _adding_node[node_name] = 0;
    }

    for (std::size_t i = 0; i < _streams.size(); i++) {
        if (_streams[i]->add_new_replicator(new_peers[i]) != 0) {
            LOG(ERROR) << "new streams add failed, peer id is "
                       << new_peers[i].to_string();
            check_on_caught_up_done(get_version_index(), node_name, false);
            return -1;
        }

        OnCaughtUp *caught_up = new OnCaughtUp(
            this, _current_term, new_peers[i], get_version_index(), node_name);
        if (_streams[i]->wait_catchup(new_peers[i], _max_margin, caught_up) !=
            0) {
            LOG(WARNING) << "node " << _server_id.to_string()
                         << " wait_caughtup failed, peer "
                         << new_peers[i].to_string();
            delete caught_up;
            check_on_caught_up_done(get_version_index(), node_name, false);
            return -1;
        }
    }

    return 0;
}

void RaftNode::on_caught_up(mraft::PeerId &addr, int64_t term,
                            int64_t version_index, butil::Status &st,
                            std::string node_name) {
    LOG(INFO) << "leader is calling on_caught_up by peer: " << addr.to_string();
    std::unique_lock<raft_mutex_t> lck(_mutex);

    if (_state != STATE_LEADER || term != _current_term) {
        LOG(WARNING) << "node " << _server_id.to_string()
                     << " stepped down when waiting peer " << addr.to_string()
                     << " to catch up, current state is " << state2str(_state)
                     << ", current term is " << _current_term
                     << ", expect term is " << term;
        return;
    }

    if (st.ok()) {  // success
        LOG(INFO) << "replicator catch up success, then check if all "
                     "replicators are available";
        check_on_caught_up_done(version_index, node_name, true);
    }

    // todo: if needed to retry

    // fail
    check_on_caught_up_done(get_version_index(), addr.get_addr_str(), false);
}

void RaftNode::check_on_caught_up_done(int64_t version_index,
                                       std::string node_name, bool succ) {
    if (version_index != get_version_index()) {
        LOG(WARNING) << "Node " << _server_id.to_string()
                     << " on_caughtup with unmatched version=" << version_index
                     << ", expect version=" << get_version_index();
        return;
    }

    if (succ) {
        LOG(INFO) << "before this catch, stream: " << node_name
                  << " replicator count is " << _adding_node[node_name];
        _adding_node[node_name].fetch_add(1);
        LOG(INFO) << "replicator is success, count is "
                  << _adding_node[node_name];
        if (_adding_node[node_name] == _streams.size()) {
            LOG(INFO) << "available replicators are enough, next generate "
                         "config change log";

            leader_change_config();

            {
                std::lock_guard<std::mutex> lck(_adding_node_mtx);
                _adding_node.erase(node_name);
            }
        }
    }

    // todo: if any replicator starts failed, try to stop the running
    // replicators & reset states
}

void RaftNode::leader_change_config() {
    LOG(INFO) << "NOW leader change config!!!!!";
}

}  // namespace mraft