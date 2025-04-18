#include "replica/stream.h"

#include <butil/logging.h>
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"
#include "replica/ballot_box.h"
#include "replica/log_collector.h"
#include "replica/log_entry_manager.h"
#include "replica/raft_node.h"

namespace mraft {

#define STREAM_LOG(level) \
    LOG(level) << "[" << __FUNCTION__ << ", stream_id: " << this->id() << "] "

DEFINE_int32(raft_max_append_entries_cache_size, 0,
             "the max size of out-of-order append entries cache");
BRPC_VALIDATE_GFLAG(raft_max_append_entries_cache_size,
                    ::brpc::NonNegativeInteger);

DEFINE_int64(raft_append_entry_high_lat_us, 1000 * 1000,
             "append entry high latency us");
BRPC_VALIDATE_GFLAG(raft_append_entry_high_lat_us, brpc::PositiveInteger);

DEFINE_bool(raft_trace_append_entry_latency, false,
            "trace append entry latency");

DECLARE_int32(raft_max_flying_entries_size);

//===--------------------------------------------------------------------===//
// Stream functions implementation
//===--------------------------------------------------------------------===//

void LogStream::shutdown() {
    step_down();

    if (_log_entry_manager) {
        _log_entry_manager->shutdown();
    }
}

void LogStream::TEST_on_node_become_leader_no_recovery() { return; }

void LogStream::on_node_become_leader() {
    bthread_t tid;
    if (bthread_start_urgent(&tid, NULL, on_node_become_leader, this) != 0) {
        LOG(ERROR) << "Fail to start bthread";
        on_node_become_leader(this);
    }
    (void)tid;
}

void* LogStream::on_node_become_leader(void* args) {
    auto stream = static_cast<LogStream*>(args);
    stream->_on_node_become_leader_impl();
    return nullptr;
}

void LogStream::_on_node_become_leader_impl() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    auto new_term = _raft_node->current_term();
    if (new_term <= _current_term) {
        CHECK_GT(new_term, _current_term) << "term cannot be decreased";
        return;
    }
    _current_term = new_term;
    _leader_id = _server_id;

    _start_leader_recovery();
}

int LogStream::_start_leader_recovery() {
    auto start_index = _get_start_index();
    // Init LogCollector
    LogCollectorOptions options;
    options.start_index = start_index;
    options.peers = peers;
    options.stream = this;
    CHECK(_log_collector != nullptr);
    _log_collector->init(options);

    // Start replicator and pull entries from peers
    _start_replicators();

    // Collect entries from self
    std::vector<std::shared_ptr<LogEntry>> entries;
    for (auto index = start_index;
         index <= _log_entry_manager->last_log_index(); index++) {
        entries.push_back(_log_entry_manager->get_entry(index));
    }
    _log_collector->collect_entires(std::move(entries), _server_id, start_index,
                                    _log_entry_manager->last_log_index());

    LOG(INFO) << "Stream start recovery, term " << _current_term
              << " ,leader_id " << _leader_id << " ,stream_id " << _stream_id;

    return 0;
}

void LogStream::on_node_become_follower(int64_t new_term, PeerId leader_id) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (new_term <= _current_term) {
        CHECK_GT(new_term, _current_term) << "term cannot be decreased";
        return;
    }
    _current_term = new_term;
    _leader_id = leader_id;

    _rep_manager->stop_all();

    LOG(INFO) << "Stream become follower, term " << _current_term
              << " ,leader_id " << _leader_id << " ,stream_id " << _stream_id;

    return;
}

void LogStream::on_leader_recovery_done() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    // 2. Write a no-op entry to the log.
    // 3. Change the state of the stream to "AppendEntries".
    STREAM_LOG(INFO) << "Stream become leader, term " << _current_term
                     << " ,leader_id " << _leader_id << " ,stream_id "
                     << _stream_id;

    _rep_manager->start_replication();
    _ballot_box->reset_pending_index(_log_entry_manager->last_log_index() + 1);
    _raft_node->notify_one_stream_recovery_finished(_stream_id);
    return;
}

int LogStream::step_down() {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    _leader_id.reset();
    _rep_manager->stop_all();
    _ballot_box->clear();

    LOG(INFO) << "Stream become follower, term " << _current_term
              << ", set leader_id empty";
    return 0;
}

int LogStream::reset_term(int64_t new_term) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (new_term <= _current_term) {
        CHECK_GT(new_term, _current_term) << "term cannot be decreased";
        return -1;
    }
    _current_term = new_term;
    return 0;
}

int LogStream::_start_replicators() {
    CHECK(!peers.empty());
    _rep_manager->reset_term(_current_term);
    LOG(INFO) << "_start_replication term is " << _current_term;
    for (std::set<PeerId>::const_iterator iter = peers.begin();
         iter != peers.end(); ++iter) {
        if (*iter == _server_id) {
            continue;
        }

        LOG(INFO) << "node " << _group_id << ":" << _server_id << " term "
                  << _current_term << " add replicator " << *iter;

        if (_rep_manager->add_replicator(*iter) != 0) {
            STREAM_LOG(ERROR) << "Fail to add replicator";
            return -1;
        }
    }

    return 0;
}

int LogStream::add_new_replicator(PeerId new_stream_addr) {
    if (new_stream_addr.is_empty()) {
        LOG(ERROR) << "empty new stream addr";
        return -1;
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id << " term "
              << _current_term << " add replicator "
              << new_stream_addr.to_string();

    if (_rep_manager->add_replicator(new_stream_addr) != 0) {
        STREAM_LOG(ERROR) << "Fail to add replicator";
        return -1;
    }

    _rep_manager->start_new_replication(new_stream_addr);

    return 0;
}

int64_t LogStream::wait_catchup(const mraft::PeerId& peer, int64_t max_margin,
                                mraft::CatchupClosure* done) {
    return _rep_manager->wait_catchup(peer, max_margin, done);
}

butil::Status LogStream::init(LogStreamOptions& options) {
    _group_id = options.group_id;
    _server_id = options.server_id;
    _stream_id = options.stream_id;
    peers = options.peers;

    // CHECK(options.log_manager);
    _log_manager = options.log_manager;

    if (!options.raft_node) {
        LOG(ERROR) << "raft_node is null";
        return butil::Status(EINVAL, "raft_node is null");
    }
    _raft_node = options.raft_node;

    if (!options.config_manager) {
        LOG(ERROR) << "config_manager is null";
        return butil::Status(EINVAL, "config_manager is null");
    }
    _conf_manager = options.config_manager;

    // BallotBox initialization
    _ballot_box.reset(new BallotBox());
    BallotBoxOptions ballot_box_options;
    ballot_box_options.log_stream = this;
    _ballot_box->init(ballot_box_options);

    // LogCollector initialization
    _log_collector.reset(new LogCollector());

    // LogEntryManager initialization
    _log_entry_manager.reset(new LogEntryManager());
    LogEntryManagerOptions log_entry_manager_options;
    log_entry_manager_options.log_storage_path = options.log_file_path;
    log_entry_manager_options.node = options.raft_node;
    log_entry_manager_options.stream = this;
    _log_entry_manager->init(log_entry_manager_options);

    // ReplicatorManager initialization
    _rep_manager.reset(new ReplicatorManager());
    ReplicatorManagerOptions rep_manager_options;
    rep_manager_options.stream = this;
    rep_manager_options.ln_manager = this->_log_entry_manager.get();
    rep_manager_options.ballot_box = this->_ballot_box.get();
    rep_manager_options.server_id = options.server_id;
    rep_manager_options.log_collector = this->_log_collector.get();

    _rep_manager->init(rep_manager_options);

    return butil::Status::OK();
}

void LogStream::append_entries_async(
    std::vector<std::shared_ptr<Task>>& tasks) {
    std::vector<std::shared_ptr<LogEntry>> entries;
    entries.reserve(tasks.size());
    for (auto& task : tasks) {
        entries.emplace_back(new LogEntry());
        entries.back()->data.swap(*task->data);
        entries.back()->id.term = _current_term;
        entries.back()->type = ENTRY_TYPE_DATA;

        _ballot_box->append_ballot_and_closure(peers, std::move(task->done));
    }

    _log_entry_manager->append_entries_async(
        std::move(entries),
        new LeaderStableClosure(NodeId(_group_id, _server_id), entries.size(),
                                _ballot_box.get()));
}

void LogStream::append_entries_from_collector(
    std::vector<std::shared_ptr<LogEntry>>& entries,
    std::unique_ptr<Closure> done) {
    auto start_index = entries.front()->get_index();
    CHECK_EQ(start_index, _get_start_index());
    CHECK(done != nullptr);

    // need to re quorum the logs.
    _ballot_box->reset_pending_index(start_index);
    for (size_t i = 0; i < entries.size() - 1; i++) {
        _ballot_box->append_ballot_and_closure(peers, nullptr);
    }
    // When the last log committed, the closure will be called.
    _ballot_box->append_ballot_and_closure(peers, std::move(done));

    auto closure = new LeaderStableClosure(NodeId(_group_id, _server_id),
                                           entries.size(), _ballot_box.get());
    closure->_first_log_index = start_index;
    _log_entry_manager->append_entries_async(std::move(entries), closure, true);
}

void LogStream::append_noop_entry(
    std::unique_ptr<NoopEntryCommittedClosure> done) {
    std::vector<std::shared_ptr<LogEntry>> entries;
    entries.emplace_back(new LogEntry);
    entries.back()->id.term = _current_term;
    entries.back()->type = ENTRY_TYPE_NO_OP;

    _ballot_box->append_ballot_and_closure(peers, std::move(done));
    _log_entry_manager->append_entries_async(
        std::move(entries),
        new LeaderStableClosure(NodeId(_group_id, _server_id), entries.size(),
                                _ballot_box.get()));
}

void LogStream::handle_append_entries_request(
    brpc::Controller* cntl, const AppendEntriesRequest* request,
    AppendEntriesResponse* response, google::protobuf::Closure* done,
    bool from_append_entries_cache) {
    //
    std::vector<std::shared_ptr<LogEntry>> entries;
    entries.reserve(request->entries_size());
    brpc::ClosureGuard done_guard(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // pre set term, to avoid get term in lock
    response->set_term(_current_term);

    // 1. Check the state of the raft node.
    if (!is_active_state(_state())) {
        const int64_t saved_current_term = _current_term;
        const RaftNodeState saved_state = _state();
        lck.unlock();
        STREAM_LOG(WARNING) << "node " << _group_id << ":" << _server_id
                            << " is not in active state "
                            << "current_term " << saved_current_term
                            << " state " << state2str(saved_state);
        cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s",
                        _group_id.c_str(), _server_id.to_string().c_str(),
                        state2str(saved_state));
        return;
    }

    PeerId req_server_id;
    if (0 != req_server_id.parse(request->server_id())) {
        lck.unlock();
        STREAM_LOG(WARNING) << "node " << _group_id << ":" << _server_id
                            << " received AppendEntries from "
                            << request->server_id() << " server_id bad format";
        cntl->SetFailed(brpc::EREQUEST, "Fail to parse server_id `%s'",
                        request->server_id().c_str());
        return;
    }

    // 2. Check request's term, return false if term < current_term
    if (request->term() < _current_term) {
        const int64_t saved_current_term = _current_term;
        lck.unlock();
        STREAM_LOG(WARNING)
            << "node " << _group_id << ":" << _server_id
            << " ignore stale AppendEntries from " << request->server_id()
            << " in term " << request->term() << " current_term "
            << saved_current_term;
        response->set_success(false);
        response->set_term(saved_current_term);
        return;
    }

    if (request->term() > _current_term) {
        CHECK_NE(_raft_node->state(), STATE_LEADER) << "leader should not "
                                                       "receive higher "
                                                       "term message";
        STREAM_LOG(INFO) << "Follower receives message "
                            "from new leader with the higher term."
                         << " " << request->term() << " > " << _current_term;
        _current_term = request->term();
        _leader_id = request->server_id();
        // FIXME: Should I call caller->on_follower_start() here?
    }

    // 3. Check the split brain
    if (_leader_id.is_empty()) {
        _leader_id = req_server_id;
    }
    if (req_server_id != _leader_id) {
        STREAM_LOG(ERROR) << "Another peer "
                          << ":" << req_server_id
                          << " declares that it is the leader at term="
                          << _current_term
                          << " which was occupied by leader=" << _leader_id;
        butil::Status status;
        status.set_error(ELEADERCONFLICT,
                         "More than one leader in the same term.");
        // TODO: notify election module
        response->set_success(false);
        response->set_term(request->term() + 1);
        return;
    }

    // 4. Check if there is a gap between prev_log_index and the last_log_index
    const int64_t prev_log_index = request->prev_log_index();
    const int64_t prev_log_term = request->prev_log_term();
    auto local_last_index = _log_entry_manager->last_log_index();
    if (local_last_index < prev_log_index) {
        response->set_success(false);
        response->set_term(_current_term);
        response->set_last_log_index(local_last_index);
        lck.unlock();
        STREAM_LOG(WARNING)
            << "node " << _group_id << ":" << _server_id
            << " reject gap AppendEntries from " << request->server_id()
            << " in term " << request->term() << " prev_log_index "
            << request->prev_log_index() << " prev_log_term "
            << request->prev_log_term() << " last_log_index "
            << local_last_index << " entries_size " << request->entries_size()
            << " from_append_entries_cache: " << from_append_entries_cache;
        return;
    }

    // 5. Check the prev_log_term
    const int64_t local_prev_log_term =
        _log_entry_manager->get_term(prev_log_index);
    if (local_prev_log_term != prev_log_term) {
        response->set_success(false);
        response->set_term(_current_term);
        response->set_last_log_index(local_last_index);
        lck.unlock();
        STREAM_LOG(WARNING)
            << "node " << _group_id << ":" << _server_id
            << " reject term_unmatched AppendEntries from "
            << request->server_id() << " in term " << request->term()
            << " prev_log_index " << request->prev_log_index()
            << " prev_log_term " << request->prev_log_term()
            << " local_prev_log_term " << local_prev_log_term
            << " last_log_index " << local_last_index << " entries_size "
            << request->entries_size()
            << " from_append_entries_cache: " << from_append_entries_cache;
        return;
    }

    // 6. Empty request, just update commit index and return next_log_index
    if (request->entries_size() == 0) {
        response->set_success(true);
        response->set_term(_current_term);
        response->set_last_log_index(_log_entry_manager->last_log_index());
        lck.unlock();
        // see the comments at FollowerStableClosure::run()
        _ballot_box->set_last_committed_index(
            std::min(request->committed_index(), prev_log_index));

        LOG(INFO) << "node " << _group_id << ":" << _server_id
                  << " received empty AppendEntries from "
                  << request->server_id() << " in term " << request->term()
                  << " prev_log_index " << request->prev_log_index()
                  << " prev_log_term " << request->prev_log_term()
                  << " last_log_index " << _log_entry_manager->last_log_index()
                  << " entries_size " << request->entries_size();
        return;
    }

    // 7. Parse entries and give it to log_entry_manager.
    butil::IOBuf data_buf;
    data_buf.swap(cntl->request_attachment());
    int64_t index = prev_log_index;
    for (int i = 0; i < request->entries_size(); i++) {
        index++;
        const EntryMeta& entry = request->entries(i);
        if (entry.type() != ENTRY_TYPE_UNKNOWN) {
            std::shared_ptr<LogEntry> log_entry(new LogEntry);
            log_entry->id.term = entry.term();
            log_entry->id.index = index;
            log_entry->type = (EntryType)entry.type();
            if (entry.has_data_len()) {
                int len = entry.data_len();
                data_buf.cutn(&log_entry->data, len);
            }
            entries.push_back(log_entry);
        }
    }

    LOG(INFO) << "node " << _group_id << ":" << _server_id
              << " received AppendEntries from " << request->server_id()
              << " in term " << request->term() << " prev_log_index "
              << request->prev_log_index() << " prev_log_term "
              << request->prev_log_term() << " last_log_index "
              << _log_entry_manager->last_log_index() << " entries_size "
              << request->entries_size();

    // The log index unmatch will be solved in this function.
    _log_entry_manager->append_entries_async(
        std::move(entries),
        new FollowerStableClosure(cntl, request, response, done_guard.release(),
                                  this, _current_term));

    // update configuration after lgn_manager updated its memory status
    // TODO: refresh configuration from lgn_manager
    // lgn_manager->check_and_set_configuration(&peers);
}

int64_t LogStream::_get_start_index() {
    int64_t index = _log_entry_manager->last_log_index() -
                    FLAGS_raft_max_flying_entries_size + 1;
    return index < 1 ? 1 : index;
}

void LogStream::handle_pull_entries_request(brpc::Controller* cntl,
                                            const PullEntriesRequest* request,
                                            PullEntriesResponse* response,
                                            google::protobuf::Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::unique_lock<raft_mutex_t> lck(_mutex);

    // Pre set term, to avoid get term in lock
    response->set_term(_current_term);

    // 1. Check the request
    if (!is_active_state(_state())) {
        const int64_t saved_current_term = _current_term;
        const RaftNodeState saved_state = _state();
        lck.unlock();
        STREAM_LOG(WARNING) << "node " << _group_id << ":" << _server_id
                            << " is not in active state "
                            << "current_term " << saved_current_term
                            << " state " << state2str(saved_state);
        cntl->SetFailed(EINVAL, "node %s:%s is not in active state, state %s",
                        _group_id.c_str(), _server_id.to_string().c_str(),
                        state2str(saved_state));
        return;
    }

    PeerId req_server_id;
    if (0 != req_server_id.parse(request->server_id())) {
        lck.unlock();
        STREAM_LOG(WARNING) << "node " << _group_id << ":" << _server_id
                            << " received AppendEntries from "
                            << request->server_id() << " server_id bad format";
        cntl->SetFailed(brpc::EREQUEST, "Fail to parse server_id `%s'",
                        request->server_id().c_str());
        return;
    }

    // 2. Check request's term, return false if term < current_term
    if (request->term() < _current_term) {
        const int64_t saved_current_term = _current_term;
        lck.unlock();
        STREAM_LOG(WARNING)
            << "node " << _group_id << ":" << _server_id
            << " ignore stale AppendEntries from " << request->server_id()
            << " in term " << request->term() << " current_term "
            << saved_current_term;
        response->set_success(false);
        response->set_term(saved_current_term);
        return;
    }

    if (request->term() > _current_term) {
        CHECK_NE(_raft_node->state(), STATE_LEADER) << "leader should not "
                                                       "receive higher "
                                                       "term message";
        STREAM_LOG(INFO) << "Follower receives message "
                            "from new leader with the higher term."
                         << " " << request->term() << " > " << _current_term;
        _current_term = request->term();
        _leader_id = request->server_id();
    }

    // 3. Check the split brain
    if (_leader_id.is_empty()) {
        _leader_id = req_server_id;
    }
    if (req_server_id != _leader_id) {
        STREAM_LOG(ERROR) << "Another peer "
                          << ":" << req_server_id
                          << " declares that it is the leader at term="
                          << _current_term
                          << " which was occupied by leader=" << _leader_id;
        butil::Status status;
        status.set_error(ELEADERCONFLICT,
                         "More than one leader in the same term.");
        // TODO: notify election module
        response->set_success(false);
        response->set_term(request->term() + 1);
        return;
    }

    // 2. Do the response
    auto start_index = request->start_index();
    auto last_index = _log_entry_manager->last_log_index();
    response->set_last_index(last_index);
    if (last_index < start_index) {
        response->set_first_index(last_index);
        response->set_success(true);
        LOG(INFO) << "No log reply to leader, This node's last_index is less "
                     "than start_index, "
                  << "node " << _group_id << ":" << _server_id
                  << " received PullEntries from " << request->server_id()
                  << " in term " << request->term() << " start_index "
                  << request->start_index() << " last_index " << last_index
                  << " entries_size " << response->entries_size();
        return;
    }

    EntryMeta em;
    auto& data = cntl->response_attachment();
    // FIXME: Define with gflag and also restrict the max size of entries.
    const int max_entries_size = 1024;
    CHECK_GT(max_entries_size, 0);
    for (int i = 0; i < max_entries_size && i + start_index <= last_index;
         ++i) {
        auto entry = _log_entry_manager->get_entry(start_index + i);
        CHECK(entry != nullptr);
        em.set_term(entry->id.term);
        em.set_type(entry->type);
        em.set_data_len(entry->data.size());
        data.append(entry->data);

        response->add_entries()->Swap(&em);
    }

    response->set_first_index(start_index);
    response->set_success(true);

    STREAM_LOG(INFO) << "node " << _group_id << ":" << _server_id
                     << " received PullEntries from " << request->server_id()
                     << " in term " << request->term() << " start_index "
                     << request->start_index() << " last_index "
                     << _log_entry_manager->last_log_index() << " entries_size "
                     << response->entries_size();
}

//===--------------------------------------------------------------------===//
// Closure for Leader flush logs
//===--------------------------------------------------------------------===//

LeaderStableClosure::LeaderStableClosure(const NodeId& node_id, size_t nentries,
                                         BallotBox* ballot_box)
    : _node_id(node_id),
      _nentries(nentries),
      _ballot_box(ballot_box),
      _mtimer("Leader flush logs latancy") {
    this->type = StableClosureType::LEADER;
    _mtimer.start();
}

void LeaderStableClosure::Run() {
    if (status().ok()) {
        if (_ballot_box) {
            _ballot_box->vote_at(_first_log_index,
                                 _first_log_index + _nentries - 1,
                                 _node_id.peer_id);
        }
        _mtimer.end();
        int64_t now = butil::cpuwide_time_us();
        if (FLAGS_raft_trace_append_entry_latency &&
            now - metric.start_time_us >
                (int64_t)FLAGS_raft_append_entry_high_lat_us) {
            LOG(WARNING) << "leader append entry latency us "
                         << (now - metric.start_time_us) << " greater than "
                         << FLAGS_raft_append_entry_high_lat_us << metric
                         << " node " << _node_id << " log_index ["
                         << _first_log_index << ", "
                         << _first_log_index + _nentries - 1 << "]";
        }
    } else {
        LOG(ERROR) << "node " << _node_id << " append [" << _first_log_index
                   << ", " << _first_log_index + _nentries - 1 << "] failed";
    }
    delete this;
}

//===--------------------------------------------------------------------===//
// Closure for Follower flush logs
//===--------------------------------------------------------------------===//

FollowerStableClosure::FollowerStableClosure(
    brpc::Controller* cntl, const AppendEntriesRequest* request,
    AppendEntriesResponse* response, google::protobuf::Closure* done,
    LogStream* stream, int64_t term)
    : _cntl(cntl),
      _request(request),
      _response(response),
      _done(done),
      _stream(stream),
      _term(term) {
    this->type = StableClosureType::FOLLOWER;
}

void FollowerStableClosure::Run() {
    _run_impl();
    delete this;
}

FollowerStableClosure::~FollowerStableClosure() {
    if (_stream) {
        // _stream->Release();
    }
}

void FollowerStableClosure::_run_impl() {
    brpc::ClosureGuard done_guard(_done);
    if (!status().ok()) {
        _cntl->SetFailed(status().error_code(), "%s", status().error_cstr());
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_stream->_mutex);
    if (_term != _stream->_current_term) {
        // The change of term indicates that leader has been changed during
        // appending entries, so we can't respond ok to the old leader
        // because we are not sure if the appended logs would be truncated
        // by the new leader.So we have to respond failure to the old leader and
        // set the new term to make it stepped down if it didn't.
        LOG(WARNING) << "node " << _stream->_group_id << ":"
                     << _stream->_server_id
                     << " reject stale AppendEntries from "
                     << _request->server_id() << " in term " << _request->term()
                     << " current_term " << _stream->_current_term;
        _response->set_success(false);
        _response->set_term(_stream->_current_term);
        return;
    }
    // It's safe to release lck as we know everything is ok at this point.
    lck.unlock();

    // DON'T touch _stream any more
    _response->set_success(true);
    _response->set_term(_term);

    const int64_t committed_index =
        std::min(_request->committed_index(),
                 // ^^^ committed_index is likely less than the
                 // last_log_index
                 _request->prev_log_index() + _request->entries_size()
                 // ^^^ The logs after the appended entries are
                 // untrustable so we can't commit them even if their
                 // indexes are less than request->committed_index()
        );
    //_ballot_box is thread safe and tolerates disorder.
    _stream->_ballot_box->set_last_committed_index(committed_index);
    int64_t now = butil::cpuwide_time_us();
    if (FLAGS_raft_trace_append_entry_latency &&
        now - metric.start_time_us >
            (int64_t)FLAGS_raft_append_entry_high_lat_us) {
        LOG(WARNING) << "follower append entry latency us "
                     << (now - metric.start_time_us) << " greater than "
                     << FLAGS_raft_append_entry_high_lat_us << metric
                     << " node " << _stream->node_id() << " log_index ["
                     << _request->prev_log_index() + 1 << ", "
                     << _request->prev_log_index() + _request->entries_size() -
                            1
                     << "]";
    }
}

//===--------------------------------------------------------------------===//
// AppendEntriesCache
//===--------------------------------------------------------------------===//

int64_t AppendEntriesCache::first_index() const {
    CHECK(!_rpc_map.empty());
    CHECK(!_rpc_queue.empty());
    return _rpc_map.begin()->second->request->prev_log_index() + 1;
}

int64_t AppendEntriesCache::cache_version() const { return _cache_version; }

bool AppendEntriesCache::empty() const { return _rpc_map.empty(); }

bool AppendEntriesCache::store(AppendEntriesRpc* rpc) {
    if (!_rpc_map.empty()) {
        bool need_clear = false;
        std::map<int64_t, AppendEntriesRpc*>::iterator it =
            _rpc_map.lower_bound(rpc->request->prev_log_index());
        int64_t rpc_prev_index = rpc->request->prev_log_index();
        int64_t rpc_last_index = rpc_prev_index + rpc->request->entries_size();

        // Some rpcs with the overlap log index alredy exist, means
        // retransmission happend, simplely clean all out of order requests, and
        // store the new one.
        if (it != _rpc_map.begin()) {
            --it;
            AppendEntriesRpc* prev_rpc = it->second;
            if (prev_rpc->request->prev_log_index() +
                    prev_rpc->request->entries_size() >
                rpc_prev_index) {
                need_clear = true;
            }
            ++it;
        }
        if (!need_clear && it != _rpc_map.end()) {
            AppendEntriesRpc* next_rpc = it->second;
            if (next_rpc->request->prev_log_index() < rpc_last_index) {
                need_clear = true;
            }
        }
        if (need_clear) {
            clear();
        }
    }
    _rpc_queue.Append(rpc);
    _rpc_map.insert(std::make_pair(rpc->request->prev_log_index(), rpc));

    // The first rpc need to start the timer
    if (_rpc_map.size() == 1) {
        if (!start_timer()) {
            clear();
            return true;
        }
    }
    HandleAppendEntriesFromCacheArg* arg = NULL;
    while (_rpc_map.size() > (size_t)FLAGS_raft_max_append_entries_cache_size) {
        std::map<int64_t, AppendEntriesRpc*>::iterator it = _rpc_map.end();
        --it;
        AppendEntriesRpc* rpc_to_release = it->second;
        rpc_to_release->RemoveFromList();
        _rpc_map.erase(it);
        if (arg == NULL) {
            arg = new HandleAppendEntriesFromCacheArg;
            arg->stream = _stream;
        }
        arg->rpcs.Append(rpc_to_release);
    }
    if (arg != NULL) {
        start_to_handle(arg);
    }
    return true;
}

void AppendEntriesCache::process_runable_rpcs(int64_t local_last_index) {
    CHECK(!_rpc_map.empty());
    CHECK(!_rpc_queue.empty());
    HandleAppendEntriesFromCacheArg* arg = NULL;
    for (std::map<int64_t, AppendEntriesRpc*>::iterator it = _rpc_map.begin();
         it != _rpc_map.end();) {
        AppendEntriesRpc* rpc = it->second;
        if (rpc->request->prev_log_index() > local_last_index) {
            break;
        }
        local_last_index =
            rpc->request->prev_log_index() + rpc->request->entries_size();
        _rpc_map.erase(it++);
        rpc->RemoveFromList();
        if (arg == NULL) {
            arg = new HandleAppendEntriesFromCacheArg;
            arg->stream = _stream;
        }
        arg->rpcs.Append(rpc);
    }
    if (arg != NULL) {
        start_to_handle(arg);
    }
    if (_rpc_map.empty()) {
        stop_timer();
    }
}

void AppendEntriesCache::clear() {
    BRAFT_VLOG << "node " << _stream->_group_id << ":" << _stream->_server_id
               << " clear append entries cache";
    stop_timer();
    HandleAppendEntriesFromCacheArg* arg = new HandleAppendEntriesFromCacheArg;
    arg->stream = _stream;
    while (!_rpc_queue.empty()) {
        AppendEntriesRpc* rpc = _rpc_queue.head()->value();
        rpc->RemoveFromList();
        arg->rpcs.Append(rpc);
    }
    _rpc_map.clear();
    start_to_handle(arg);
}

void AppendEntriesCache::ack_fail(AppendEntriesRpc* rpc) {
    rpc->cntl->SetFailed(EINVAL, "Fail to handle out-of-order requests");
    rpc->done->Run();
    delete rpc;
}

void AppendEntriesCache::start_to_handle(HandleAppendEntriesFromCacheArg* arg) {
    // _stream->AddRef();
    bthread_t tid;
    // Sequence if not important
    if (bthread_start_background(&tid, NULL,
                                 LogStream::_handle_append_entries_from_cache,
                                 arg) != 0) {
        LOG(ERROR) << "Fail to start bthread";
        // We cant't call Stream::handle_append_entries_from_cache
        // here since we are in the mutex, which will cause dead lock, just
        // set the rpc fail, and let leader block for a while.
        butil::LinkedList<AppendEntriesRpc>& rpcs = arg->rpcs;
        while (!rpcs.empty()) {
            AppendEntriesRpc* rpc = rpcs.head()->value();
            rpc->RemoveFromList();
            ack_fail(rpc);
        }
        // _stream->Release();
        delete arg;
    }
}

bool AppendEntriesCache::start_timer() {
    ++_timer_version;
    AppendEntriesCacheTimerArg* timer_arg = new AppendEntriesCacheTimerArg;
    timer_arg->stream = _stream;
    timer_arg->timer_version = _timer_version;
    timer_arg->cache_version = _cache_version;
    timer_arg->timer_start_ms = _rpc_queue.head()->value()->receive_time_ms;
    timespec duetime = butil::milliseconds_from(
        butil::milliseconds_to_timespec(timer_arg->timer_start_ms), 1);
    // _stream->AddRef();
    if (bthread_timer_add(&_timer, duetime,
                          LogStream::_on_append_entries_cache_timedout,
                          timer_arg) != 0) {
        LOG(ERROR) << "Fail to add timer";
        delete timer_arg;
        // _stream->Release();
        return false;
    }
    return true;
}

void AppendEntriesCache::stop_timer() {
    if (_timer == bthread_timer_t()) {
        return;
    }
    ++_timer_version;
    if (bthread_timer_del(_timer) == 0) {
        // _stream->Release();
        _timer = bthread_timer_t();
    }
}

void AppendEntriesCache::do_handle_append_entries_cache_timedout(
    int64_t timer_version, int64_t timer_start_ms) {
    if (timer_version != _timer_version) {
        return;
    }
    CHECK(!_rpc_map.empty());
    CHECK(!_rpc_queue.empty());
    // If the head of out-of-order requests is not be handled, clear the entire
    // cache, otherwise, start a new timer.
    if (_rpc_queue.head()->value()->receive_time_ms <= timer_start_ms) {
        clear();
        return;
    }
    if (!start_timer()) {
        clear();
    }
}

void* LogStream::_handle_append_entries_from_cache(void* arg) {
    HandleAppendEntriesFromCacheArg* handle_arg =
        (HandleAppendEntriesFromCacheArg*)arg;
    LogStream* stream = handle_arg->stream;
    butil::LinkedList<AppendEntriesRpc>& rpcs = handle_arg->rpcs;
    while (!rpcs.empty()) {
        AppendEntriesRpc* rpc = rpcs.head()->value();
        rpc->RemoveFromList();
        stream->handle_append_entries_request(rpc->cntl, rpc->request,
                                              rpc->response, rpc->done, true);
        delete rpc;
    }
    // node->Release();
    delete handle_arg;
    return NULL;
}

void LogStream::_on_append_entries_cache_timedout(void* arg) {
    bthread_t tid;
    if (bthread_start_background(
            &tid, NULL, LogStream::_handle_append_entries_cache_timedout,
            arg) != 0) {
        LOG(ERROR) << "Fail to start bthread";
        LogStream::_handle_append_entries_cache_timedout(arg);
    }
}

void* LogStream::_handle_append_entries_cache_timedout(void* arg) {
    AppendEntriesCacheTimerArg* timer_arg = (AppendEntriesCacheTimerArg*)arg;
    LogStream* stream = timer_arg->stream;

    std::unique_lock<raft_mutex_t> lck(stream->_mutex);
    if (stream->_append_entries_cache &&
        timer_arg->cache_version ==
            stream->_append_entries_cache->cache_version()) {
        stream->_append_entries_cache->do_handle_append_entries_cache_timedout(
            timer_arg->timer_version, timer_arg->timer_start_ms);
        if (stream->_append_entries_cache->empty()) {
            // FIXME: why delete the cache hear?
            stream->_append_entries_cache.reset(nullptr);
            // stream->_append_entries_cache = NULL;
        }
    }
    lck.unlock();
    delete timer_arg;
    // node->Release();
    return NULL;
}

bool LogStream::_handle_out_of_order_append_entries(
    brpc::Controller* cntl, const AppendEntriesRequest* request,
    AppendEntriesResponse* response, google::protobuf::Closure* done,
    int64_t local_last_index) {
    if (!(FLAGS_raft_max_append_entries_cache_size > 0) ||
        local_last_index >= request->prev_log_index() ||
        request->entries_size() == 0) {
        return false;
    }
    if (!_append_entries_cache) {
        _append_entries_cache.reset(
            new AppendEntriesCache(this, ++_append_entries_cache_version));
    }
    AppendEntriesRpc* rpc = new AppendEntriesRpc;
    rpc->cntl = cntl;
    rpc->request = request;
    rpc->response = response;
    rpc->done = done;
    rpc->receive_time_ms = butil::gettimeofday_ms();
    bool rc = _append_entries_cache->store(rpc);
    if (!rc && _append_entries_cache->empty()) {
        _append_entries_cache.reset(nullptr);
    }
    return rc;
}

void LogStream::handle_add_node(const mraft::AddNodeRequest* request,
                                brpc::Controller* cntl) {
    LOG(INFO) << "handle add node req from " << request->sender()
              << " leader id is " << request->receiver() << " data is "
              << request->serialized_info();

    if (_leader_id != request->receiver()) {
        LOG(ERROR) << "NOW leader id is " << _leader_id
                   << " but in new node's option, leader id is "
                   << request->receiver();
        cntl->SetFailed(EINVAL, "Wrong leader_id `%s'",
                        request->receiver().c_str());

        return;
    }

    nlohmann::json addresses =
        nlohmann::json::parse(request->serialized_info());
    std::vector<std::string> new_addresses = addresses["addresses"];

    for (auto addr : new_addresses) {
        LOG(INFO) << "new addr " << addr;
    }
    std::vector<PeerId> new_peers;
    for (auto addr : new_addresses) {
        new_peers.push_back(PeerId(addr));
    }

    if (_raft_node->add_new_replicator_in_stream(new_peers,
                                                 request->node_name()) != 0) {
        cntl->SetFailed(EINVAL, "Failed to build new stream");
    }
}

// FIXME: add command
void LogStream::_check_append_entries_cache(int64_t local_last_index) {
    if (!_append_entries_cache) {
        return;
    }
    _append_entries_cache->process_runable_rpcs(local_last_index);
    if (_append_entries_cache->empty()) {
        _append_entries_cache.reset(nullptr);
    }
}

RaftNodeState LogStream::_state() const { return _raft_node->state(); }

butil::Status LogStream::start_log_server() {
    if (_brpc_server) {
        STREAM_LOG(ERROR) << "brpc server is not null";
        return butil::Status(EINVAL, "brpc server is not null");
    }
    _brpc_server.reset(new brpc::Server());

    if (0 != _brpc_server->AddService(new LogServiceImpl(this, _raft_node),
                                      brpc::SERVER_OWNS_SERVICE)) {
        STREAM_LOG(ERROR) << "Fail to add service";
        return butil::Status(EINVAL, "Fail to add service");
    }

    // FIXME: Find the best brpc options
    // brpc::ServerOptions server_options;
    if (0 != _brpc_server->Start(_server_id.addr, nullptr)) {
        STREAM_LOG(ERROR) << "Fail to start rpc server";
        return butil::Status(EINVAL, "Fail to start rpc server");
    }

    return butil::Status::OK();
}

NodeId LogStream::node_id() const { return NodeId(_group_id, _server_id); }

int64_t LogStream::commit_index() const {
    return _ballot_box->last_committed_index();
}

std::shared_ptr<LogEntry> LogStream::get_log_entry(int64_t index) {
    return _log_entry_manager->get_entry(index);
}

int64_t LogStream::first_log_index() const {
    return _log_entry_manager->first_log_index();
}
int64_t LogStream::last_log_index() const {
    return _log_entry_manager->last_log_index();
}

}  // namespace mraft