#include "replica/replicator.h"  // Replicator

#include <brpc/channel.h>
#include <butil/logging.h>  // MLOG(ERROR)
#include <sys/types.h>

#include <cassert>
#include <cstdint>

// #include "common/mlog.h"
#include "common/common.h"
#include "common/util.h"
#include "replica/ballot_box.h"  // BallotBox
#include "replica/log_entry_manager.h"
#include "replica/stream.h"
#include "rpc/raft.pb.h"

// When using replicator's static functions that need r->fuc();
#define REPLICATOR_LOG_R(level)                                              \
    LOG(level) << "[" << __FUNCTION__ << ", stream_id: " << r->_stream->id() \
               << "] "

// When using replicator's regular functions.
#define REPLICATOR_LOG(level)                                             \
    LOG(level) << "[" << __FUNCTION__ << ", stream_id: " << _stream->id() \
               << "] "

namespace mraft {

DEFINE_int32(raft_max_flying_entries_size, 1,
             "The max number of entries in sending");
BRPC_VALIDATE_GFLAG(raft_max_flying_entries_size, ::brpc::PositiveInteger);

DEFINE_int32(raft_max_parallel_append_entries_rpc_num, 8,
             "The max number of parallel AppendEntries requests");
BRPC_VALIDATE_GFLAG(raft_max_parallel_append_entries_rpc_num,
                    ::brpc::PositiveInteger);

DEFINE_int32(raft_retry_replicate_interval_ms, 1000,
             "Interval of retry to append failed AppendEntriesRPC");
BRPC_VALIDATE_GFLAG(raft_retry_replicate_interval_ms, brpc::PositiveInteger);

DEFINE_bool(use_fake_on_rpc_returned, false, "Is in test mode");

// 用于追踪高延迟的 ApendEntriesRPC
DECLARE_int64(raft_append_entry_high_lat_us);

// Default 1,024 MB
DEFINE_uint64(raft_max_body_size, 1073741824,
              "The max byte size of AppendEntriesRequest");

DEFINE_int32(raft_rpc_channel_connect_timeout_ms, 2000,
             "Timeout for connecting to peer");

// DEFINE_bool(raft_trace_append_entry_latency, false,
//             "trace append entry latency");
// BRPC_VALIDATE_GFLAG(raft_trace_append_entry_latency, brpc::PassValidate);

// DEFINE_int32(raft_max_parallel_append_entries_rpc_num, 1,
//              "The max number of parallel AppendEntries requests");
// BRPC_VALIDATE_GFLAG(raft_max_parallel_append_entries_rpc_num,
//                     ::brpc::PositiveInteger);

//===--------------------------------------------------------------------===//
// ReplicatorManager functions
//===--------------------------------------------------------------------===//

void ReplicatorManager::init(ReplicatorManagerOptions& options) {
    _common_options.ln_manager = options.ln_manager;
    _common_options.ballot_box = options.ballot_box;
    _common_options.stream = options.stream;
    _common_options.server_id = options.server_id;
    _common_options.term = options.term;
    _common_options.log_collector = options.log_collector;
}

int ReplicatorManager::add_replicator(const PeerId& peer, bool is_learner) {
    CHECK_NE(0, _common_options.term);
    if (_rmap.find(peer) != _rmap.end()) {
        return 0;
    }
    ReplicatorOptions options = _common_options;
    options.peer_id = peer;
    options.term = _common_options.term;
    options.log_collector = _common_options.log_collector;
    options.is_learner = is_learner;

    // Not needed for now
    // options.replicator_status = new ReplicatorStatus;

    ReplicatorId rid;
    if (Replicator::start(options, &rid) != 0) {
        MLOG(ERROR) << "Group " << options.group_id
                    << " Fail to start replicator to peer=" << peer;
        return -1;
    }
    _rmap[peer] = rid;
    return 0;
}

int ReplicatorManager::stop_replicator(const PeerId& peer) {
    auto iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    // Calling ReplicatorId::stop might lead to calling stop_replicator again,
    // erase iter first to avoid race condition
    _rmap.erase(iter);
    return Replicator::stop(rid);
}

int ReplicatorManager::stop_all() {
    std::vector<ReplicatorId> rids;
    rids.reserve(_rmap.size());
    for (auto iter = _rmap.begin(); iter != _rmap.end(); ++iter) {
        rids.push_back(iter->second);
    }
    _rmap.clear();
    for (size_t i = 0; i < rids.size(); ++i) {
        Replicator::stop(rids[i]);
    }
    return 0;
}

void ReplicatorManager::start_replication() {
    for (auto iter = _rmap.begin(); iter != _rmap.end(); ++iter) {
        Replicator::start_replication(iter->second);
    }
}

void ReplicatorManager::start_new_replication(const mraft::PeerId& peer) {
    Replicator::start_replication(_rmap[peer]);
}

int ReplicatorManager::reset_term(int64_t new_term) {
    if (new_term <= _common_options.term) {
        CHECK_GT(new_term, _common_options.term) << "term cannot be decreased";
        return -1;
    }
    _common_options.term = new_term;
    return 0;
}

int64_t ReplicatorManager::wait_catchup(const mraft::PeerId& peer,
                                        int64_t max_margin,
                                        mraft::CatchupClosure* done) {
    auto iter = _rmap.find(peer);
    if (iter == _rmap.end()) {
        return -1;
    }
    ReplicatorId rid = iter->second;
    Replicator::wait_for_caught_up(rid, max_margin, done);
    return 0;
}

void Replicator::wait_for_caught_up(ReplicatorId id, int64_t max_margin,
                                    mraft::CatchupClosure* done) {
    bthread_id_t dummy_id = {id};
    Replicator* r = nullptr;
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        done->status().set_error(EINVAL, "No such replicator");
        run_closure_in_bthread(done);
        return;
    }

    if (r->_catchup_closure != NULL) {
        LOG(ERROR) << "Previous wait_for_caught_up is not over"
                   << ", group " << r->_group_id;
        done->status().set_error(EINVAL, "Duplicated call");
        run_closure_in_bthread(done);
        return;
    }

    done->_max_margin = max_margin;
    if (r->_has_succeeded && r->_is_catchup(max_margin)) {
        LOG(INFO) << "Already catch up before add catch up timer"
                  << ", group " << r->_group_id;
        run_closure_in_bthread(done);
        return;
    }

    if (r->_has_succeeded && r->_is_catchup(max_margin)) {
        run_closure_in_bthread(done);
        return;
    }

    r->_catchup_closure = done;
    return;
}

//===--------------------------------------------------------------------===//
// Replicator functions
//===--------------------------------------------------------------------===//
int Replicator::start(ReplicatorOptions& options, ReplicatorId* id) {
    if (options.ln_manager == NULL || options.ballot_box == NULL ||
        options.stream == NULL) {
        MLOG(ERROR) << "Invalid arguments, group " << options.group_id;
        return -1;
    }
    Replicator* r = new Replicator();
    brpc::ChannelOptions channel_opt;
    channel_opt.connect_timeout_ms = FLAGS_raft_rpc_channel_connect_timeout_ms;
    channel_opt.timeout_ms = -1;  // We don't need RPC timeout
    if (r->_sending_channel.Init(options.peer_id.addr, &channel_opt) != 0) {
        MLOG(ERROR) << "Fail to init sending channel"
                    << ", group " << options.group_id;
        delete r;
        return -1;
    }

    // Replicator stop is async
    r->_group_id = options.group_id;
    r->_server_id = options.server_id;
    r->_peer_id = options.peer_id;
    r->_ln_manager = options.ln_manager;
    r->_ballot_box = options.ballot_box;
    r->_stream = options.stream;
    r->_term = options.term;
    r->_log_collector = options.log_collector;
    r->_is_learner = options.is_learner;
    r->_catchup_closure = NULL;
    r->_has_succeeded = false;
    CHECK(r->_ballot_box != nullptr);
    CHECK(r->_stream != nullptr);
    CHECK(r->_log_collector != nullptr);

    r->_next_index = r->_ln_manager->last_log_index();
    r->_next_index = r->_next_index < 1 ? 1 : r->_next_index;
    // Why the bthread_id_create() fucking need on_error functions?
    if (bthread_id_create(
            &r->_id, r, [](bthread_id_t id, void* arg, int error_code) {
                (void)id;
                Replicator* r = (Replicator*)arg;
                CHECK(false) << "Group " << r->_group_id
                             << " Unknown error_code=" << error_code;
                return -1;
            }) != 0) {
        MLOG(ERROR) << "Fail to create bthread_id"
                    << ", group " << options.group_id;
        delete r;
        return -1;
    }

    ReplicatorLockGuard lg(r->_id, r);
    if (!lg.success()) {
        return -1;
    } else {
        *id = r->_id.value;
    }

    r->_ln_manager->register_replicator(r);

    MLOG(INFO) << "Replicator=" << r->_id << "@" << r->_peer_id << " is started"
               << ", group " << r->_group_id << ", server_id " << r->_server_id
               << ", term " << r->_term;

    // Start leader recovery
    r->_st.st = LEADER_RECOVERY;
    auto start_index = r->_log_collector->start_index();
    r->_send_pull_entries_request(start_index);
    return 0;
}

int Replicator::start_replication(const ReplicatorId& id) {
    bthread_id_t dummy_id = {id};
    Replicator* r = nullptr;
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        return -1;
    }

    REPLICATOR_LOG_R(INFO) << "Group " << r->_group_id << " Replicator=" << id
                           << " peer=" << r->_peer_id << " start replication";
    LOG(INFO) << "start_replication";
    r->_send_empty_entries();
    return 0;
}

int Replicator::stop(ReplicatorId id) {
    bthread_id_t dummy_id = {id};
    Replicator* r = nullptr;
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        return -1;
    }

    // TODO: Do we need this?
    // to run _catchup_closure if it is not NULL
    // r->_notify_on_caught_up(EPERM, true);

    r->_cancel_append_entries_rpcs();
    r->_ln_manager->remove_replicator(r);
    MLOG(INFO) << "Group " << r->_group_id << " Replicator=" << id
               << " is going to quit";

    bthread_id_t saved_id = r->_id;
    CHECK_EQ(0, lg.destory());
    MLOG(INFO) << "Replicator=" << saved_id << " is going to quit";
    delete r;

    return 0;
}

void Replicator::_destroy() {
    bthread_id_t saved_id = _id;
    CHECK_EQ(0, bthread_id_unlock_and_destroy(saved_id));
    MLOG(INFO) << "Replicator=" << saved_id << " is going to quit";
    delete this;
}

void Replicator::_cancel_append_entries_rpcs() {
    for (std::deque<FlyingAppendEntriesRpc>::iterator rpc_it =
             _append_entries_rpc_in_fly.begin();
         rpc_it != _append_entries_rpc_in_fly.end(); ++rpc_it) {
        brpc::StartCancel(rpc_it->call_id);
    }
    for (auto id : _pull_entries_rpc_in_fly) {
        brpc::StartCancel(id);
    }
    _pull_entries_rpc_in_fly.clear();
    _append_entries_rpc_in_fly.clear();
}

int Replicator::_fill_common_fields(AppendEntriesRequest* request,
                                    int64_t prev_log_index) {
    const int64_t prev_log_term = _ln_manager->get_term(prev_log_index);
    request->set_term(_term);
    request->set_group_id(_group_id);
    request->set_server_id(_server_id.to_string());
    request->set_peer_id(_peer_id.to_string());
    request->set_prev_log_index(prev_log_index);
    request->set_prev_log_term(prev_log_term);
    request->set_committed_index(_ballot_box->last_committed_index());
    return 0;
}

int Replicator::_prepare_entry_meta(int offset, EntryMeta* em,
                                    butil::IOBuf* data,
                                    int64_t total_data_size) {
    // TODO: deal with the case that rpc-body are to big

    const int64_t log_index = _next_index + offset;
    auto entry = _ln_manager->get_entry(log_index);
    if (!entry) {
        return -1;
    } else if (entry->data.size() + total_data_size >=
               FLAGS_raft_max_body_size) {
        REPLICATOR_LOG(WARNING)
            << "Group " << _group_id << " entry size " << entry->data.size()
            << " total data size " << total_data_size << " greater than "
            << FLAGS_raft_max_body_size;
        return -1;
    }

    em->set_term(entry->id.term);
    em->set_type(entry->type);

    em->set_data_len(entry->data.length());
    data->append(entry->data);
    return 0;
}

void Replicator::send_entries() {
    // Leader recovery do not limit the number of entries to send.
    if ((_st.st == APPENDING_ENTRIES &&
         (_flying_append_entries_size >= FLAGS_raft_max_flying_entries_size)) ||
        _append_entries_rpc_in_fly.size() >=
            (size_t)FLAGS_raft_max_parallel_append_entries_rpc_num ||
        _st.st == BLOCKING) {
        REPLICATOR_LOG(WARNING)
            << "node " << _group_id << ":" << _server_id
            << " skip sending AppendEntriesRequest to " << _peer_id
            << ", too many requests in flying, or the replicator is in block,"
            << " next_index " << _next_index << " flying_size "
            << _flying_append_entries_size;
        return;
    }

    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<AppendEntriesRequest> request(new AppendEntriesRequest);
    std::unique_ptr<AppendEntriesResponse> response(new AppendEntriesResponse);

    if (_fill_common_fields(request.get(), _next_index - 1)) {
        MLOG(FATAL) << "Unexpeced error when filling common fields";
        // TODO: snapshoot handling.
    }

    // 1. prepare entries
    EntryMeta em;
    auto max_entries_size =
        _st.st == LEADER_RECOVERY
            ? INT_MAX  // Leader recovery do not limit the number of
                       // entries to send.
            : FLAGS_raft_max_flying_entries_size - _flying_append_entries_size;
    CHECK_GT(max_entries_size, 0);

    int64_t data_size = 0;
    for (int i = 0; i < max_entries_size; ++i) {
        if (0 != _prepare_entry_meta(i, &em, &cntl->request_attachment(),
                                     data_size)) {
            // reach the end of log
            break;
        }
        data_size += em.data_len();
        request->add_entries()->Swap(&em);
    }
    if (request->entries_size() == 0) {
        // TODO: snapshoot handling.
        return;
    }

    // 2. update status
    _append_entries_rpc_in_fly.push_back(FlyingAppendEntriesRpc(
        _next_index, request->entries_size(), cntl->call_id()));
    _next_index += request->entries_size();
    _flying_append_entries_size += request->entries_size();
    _st.st = APPENDING_ENTRIES;
    _st.first_log_index = _next_index - _flying_append_entries_size;
    _st.last_log_index = _next_index - 1;

    MLOG(INFO) << "node " << _group_id << ":" << _server_id
               << " send AppendEntriesRequest to " << _peer_id << " term "
               << _term << " last_committed_index "
               << request->committed_index() << " prev_log_index "
               << request->prev_log_index() << " prev_log_term "
               << request->prev_log_term() << " next_index " << _next_index
               << " count " << request->entries_size();

    // 3. send rpc
    mtimer.start();
    google::protobuf::Closure* done =
        brpc::NewCallback(_on_append_entries_rpc_returned, _id.value,
                          cntl.get(), request.get(), response.get());
    LogService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), response.release(),
                        done);
}

void Replicator::_cancel_sending_rpcs_and_stop_sending_rpc() {
    _next_index -= _flying_append_entries_size;
    _flying_append_entries_size = 0;
    _stop_sending_rpc = true;

    // cancel append entries rpcs ();
    for (std::deque<FlyingAppendEntriesRpc>::iterator rpc_it =
             _append_entries_rpc_in_fly.begin();
         rpc_it != _append_entries_rpc_in_fly.end(); ++rpc_it) {
        brpc::StartCancel(rpc_it->call_id);
    }
    _append_entries_rpc_in_fly.clear();
}

void Replicator::_allow_sending_rpc() { _stop_sending_rpc = false; }

void* Replicator::_on_block_timedout_in_new_thread(void* arg) {
    Replicator* r = nullptr;
    auto id_value = *static_cast<uint64_t*>(arg);
    bthread_id_t id = {id_value};
    ReplicatorLockGuard lg(id, r);
    if (!lg.success()) {
        LOG(WARNING) << "Fail to lock replicator ";
        return NULL;
    }

    r->_st.st = IDLE;
    lg.unlock();
    Replicator::_continue_sending(arg, ETIMEDOUT);
    return NULL;
}

void Replicator::_on_block_timedout(void* arg) {
    bthread_t tid;
    if (bthread_start_background(&tid, NULL, _on_block_timedout_in_new_thread,
                                 arg) != 0) {
        PLOG(ERROR) << "Fail to start bthread";
        _on_block_timedout_in_new_thread(arg);
    }
}

void Replicator::_block(long start_time_us, int error_code) {
    // mainly for pipeline case, to avoid too many block timer when this
    // replicator is something wrong
    if (_st.st == BLOCKING) {
        return;
    }

    int blocking_time = FLAGS_raft_retry_replicate_interval_ms;
    if (error_code == EBUSY || error_code == EINTR) {
        // MLOG(INFO) << "Fail to send AppendEntriesRequest to " << _peer_id
        //            << "  " << berror(error_code) << ", group " << _group_id;
    } else {
        // MLOG(INFO) << "Other rpc error: " << error_code;
    }
    const timespec due_time = butil::milliseconds_from(
        butil::microseconds_to_timespec(start_time_us), blocking_time);
    bthread_timer_t timer;
    const int rc =
        bthread_timer_add(&timer, due_time, _on_block_timedout, &_id.value);
    if (rc == 0) {
        MLOG(INFO) << "Blocking " << _peer_id << " for " << blocking_time
                   << "ms"
                   << ", due to rpc error: " << berror(error_code);
        _st.st = BLOCKING;
        return;
    } else {
        MLOG(ERROR) << "Fail to add block timer, " << berror(rc);
        return _send_empty_entries();
    }
}

void Replicator::_on_append_entries_rpc_returned(
    ReplicatorId id, brpc::Controller* cntl, AppendEntriesRequest* request,
    AppendEntriesResponse* response) {
    if (FLAGS_use_fake_on_rpc_returned) {
        // return TEST_fake_on_rpc_returned(id, cntl, request, response,
        //                                  rpc_send_time);
    }

    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<AppendEntriesRequest> req_guard(request);
    std::unique_ptr<AppendEntriesResponse> res_guard(response);
    Replicator* r = nullptr;
    bthread_id_t dummy_id = {id};
    const long start_time_us = butil::gettimeofday_us();
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        return;
    }
    r->mtimer.end();

    bool valid_rpc = false;
    int64_t rpc_first_index = request->prev_log_index() + 1;
    int64_t min_flying_index = r->_min_flying_index();
    CHECK_GT(min_flying_index, 0);

    // 1. check if the rpc_response is in _append_entries_in_fly.
    for (auto rpc_it = r->_append_entries_rpc_in_fly.begin();
         rpc_it != r->_append_entries_rpc_in_fly.end(); ++rpc_it) {
        if (rpc_it->log_index > rpc_first_index) {
            break;
        }
        if (rpc_it->call_id == cntl->call_id()) {
            valid_rpc = true;
        }
    }
    if (!valid_rpc) {
        REPLICATOR_LOG_R(WARNING) << " ignore invalid rpc";
        return;
    }

    // 2. check the rpc failure
    if (cntl->Failed()) {
        r->_consecutive_rpc_error_times++;
        // REPLICATOR_LOG_R(WARNING) << "AppendEntriesRPC fail, sleep a will";
        // LOG_IF(WARNING, (r->_consecutive_error_times++) % 10 == 0)
        REPLICATOR_LOG_R(WARNING)
            << "Group " << r->_group_id << " fail to issue RPC to "
            << r->_peer_id
            << " _consecutive_error_times=" << r->_consecutive_rpc_error_times
            << ", " << cntl->ErrorText();
        // If the follower crashes, any RPC to the follower fails immediately,
        // so we need to block the follower for a while instead of looping until
        // it comes back or be removed
        r->_cancel_sending_rpcs_and_stop_sending_rpc();
        return r->_block(start_time_us, cntl->ErrorCode());
    }

    REPLICATOR_LOG_R(INFO) << "node " << r->_group_id << ":" << r->_server_id
                           << " received AppendEntriesResponse from "
                           << r->_peer_id << " prev_log_index "
                           << request->prev_log_index() << " prev_log_term "
                           << request->prev_log_term() << " count "
                           << request->entries_size();

    // 3. check the response
    r->_consecutive_rpc_error_times = 0;
    if (!response->success()) {
        // 3.1 recieve a response with larger term, means a new leader appears
        if (response->term() > r->_term) {
            REPLICATOR_LOG_R(ERROR)
                << " fail, greater term " << response->term() << " expect term "
                << r->_term;
            r->_cancel_sending_rpcs_and_stop_sending_rpc();

            // TODO: deal with term increase, how to notify the election module
            REPLICATOR_LOG_R(FATAL)
                << "Term increased, need to notify election module";
            return;
        }
        REPLICATOR_LOG_R(WARNING)
            << " fail, find next_index remote last_log_index "
            << response->last_log_index() << " local next_index "
            << r->_next_index << " rpc prev_log_index "
            << request->prev_log_index();

        // 3.2 prev_log_index and prev_log_term doesn't match
        r->_cancel_sending_rpcs_and_stop_sending_rpc();
        if (response->last_log_index() + 1 < r->_next_index) {
            // 3.2.1 The peer needs older logs, rest the _next_index.
            REPLICATOR_LOG_R(INFO) << "Group " << r->_group_id
                                   << " last_log_index at peer=" << r->_peer_id
                                   << " is " << response->last_log_index();
            // The peer contains less logs than leader
            r->_next_index = response->last_log_index() + 1;
        } else {
            // 3.2.2 The peer contains logs from old term which should be
            // truncated, decrease _last_log_at_peer by one to test the right
            // index to keep
            if (BAIDU_LIKELY(r->_next_index > 1)) {
                REPLICATOR_LOG_R(INFO)
                    << "Group " << r->_group_id
                    << " log_index=" << r->_next_index << " mismatch";
                --(r->_next_index);
            } else {
                REPLICATOR_LOG_R(ERROR)
                    << "Group " << r->_group_id << " peer=" << r->_peer_id
                    << " declares that log at index=0 doesn't match,"
                       " which is not supposed to happen";
            }
        }

        // Use this to verify the correctness of _next_index
        LOG(INFO) << "_on_append_entries_rpc_returned";
        r->_send_empty_entries();
        return;
    }

    // 4. Dismached term
    if (response->term() != r->_term) {
        REPLICATOR_LOG_R(ERROR)
            << "Group " << r->_group_id << " fail, response term "
            << response->term() << " mismatch, expect term " << r->_term;
        r->_cancel_sending_rpcs_and_stop_sending_rpc();
        return;
    }

    // MLOG(INFO) << " success";
    r->_allow_sending_rpc();

    // 5. Vote for each LogEntry
    // r->_update_last_rpc_send_timestamp(rpc_send_time);
    const int entries_size = request->entries_size();
    const int64_t rpc_last_log_index = request->prev_log_index() + entries_size;
    if (entries_size > 0) {
        REPLICATOR_LOG_R(INFO)
            << "Group " << r->_group_id << " replicated logs in ["
            << min_flying_index << ", " << rpc_last_log_index << "] to peer "
            << r->_peer_id;

        if (!r->is_learner()) {
            r->_ballot_box->vote_at(min_flying_index, rpc_last_log_index,
                                    r->_peer_id);
        }
        int64_t rpc_latency_us = cntl->latency_us();
        if (rpc_latency_us > FLAGS_raft_append_entry_high_lat_us) {
            REPLICATOR_LOG_R(WARNING)
                << "append entry rpc latency us " << rpc_latency_us
                << " greater than " << FLAGS_raft_append_entry_high_lat_us
                << " Group " << r->_group_id << " to peer  " << r->_peer_id
                << " request entry size " << entries_size
                << " request data size " << cntl->request_attachment().size();
        }
    }

    // 6. A rpc is marked as success, means all request before it are success,
    // erase them sequentially.
    while (!r->_append_entries_rpc_in_fly.empty() &&
           r->_append_entries_rpc_in_fly.front().log_index <= rpc_first_index) {
        r->_flying_append_entries_size -=
            r->_append_entries_rpc_in_fly.front().entries_size;
        r->_append_entries_rpc_in_fly.pop_front();
    }

    // TODO: caught up logic
    r->_has_succeeded = true;
    r->_notify_on_caught_up(0, false);

    // Try continue sending entries
    r->send_entries();
    return;
}

void Replicator::TEST_fake_on_rpc_returned(ReplicatorId id,
                                           brpc::Controller* cntl,
                                           AppendEntriesRequest* request,
                                           AppendEntriesResponse* response,
                                           int64_t rpc_send_time) {
    (void)rpc_send_time;
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<AppendEntriesRequest> req_guard(request);
    std::unique_ptr<AppendEntriesResponse> res_guard(response);
    Replicator* r = nullptr;
    bthread_id_t dummy_id = {id};
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        return;
    }

    REPLICATOR_LOG_R(INFO) << "node " << r->_group_id << ":" << r->_server_id
                           << " received AppendEntriesResponse from "
                           << r->_peer_id << " prev_log_index "
                           << request->prev_log_index() << " prev_log_term "
                           << request->prev_log_term() << " count "
                           << request->entries_size();

    bool valid_rpc = false;
    int64_t rpc_first_index = request->prev_log_index() + 1;
    int64_t min_flying_index = r->_min_flying_index();
    CHECK_GT(min_flying_index, 0);

    // 1 check if the rpc_response is in _append_entries_in_fly.
    for (auto rpc_it = r->_append_entries_rpc_in_fly.begin();
         rpc_it != r->_append_entries_rpc_in_fly.end(); ++rpc_it) {
        if (rpc_it->log_index > rpc_first_index) {
            break;
        }
        if (rpc_it->call_id == cntl->call_id()) {
            valid_rpc = true;
        }
    }
    if (!valid_rpc) {
        REPLICATOR_LOG_R(WARNING) << " ignore invalid rpc";
        return;
    }

    // MLOG(INFO) << " success";
    r->_allow_sending_rpc();

    // 5 Vote for each LogEntry
    // r->_update_last_rpc_send_timestamp(rpc_send_time);
    const int entries_size = request->entries_size();
    const int64_t rpc_last_log_index = request->prev_log_index() + entries_size;
    if (entries_size > 0) {
        REPLICATOR_LOG_R(INFO)
            << "Group " << r->_group_id << " replicated logs in ["
            << min_flying_index << ", " << rpc_last_log_index << "] to peer "
            << r->_peer_id;

        r->_ballot_box->vote_at(min_flying_index, rpc_last_log_index,
                                r->_peer_id);
        int64_t rpc_latency_us = cntl->latency_us();
        if (rpc_latency_us > FLAGS_raft_append_entry_high_lat_us) {
            REPLICATOR_LOG_R(WARNING)
                << "append entry rpc latency us " << rpc_latency_us
                << " greater than " << FLAGS_raft_append_entry_high_lat_us
                << " Group " << r->_group_id << " to peer  " << r->_peer_id
                << " request entry size " << entries_size
                << " request data size " << cntl->request_attachment().size();
        }
    }

    // 6 A rpc is marked as success, means all request before it are success,
    // erase them sequentially.
    while (!r->_append_entries_rpc_in_fly.empty() &&
           r->_append_entries_rpc_in_fly.front().log_index <= rpc_first_index) {
        r->_flying_append_entries_size -=
            r->_append_entries_rpc_in_fly.front().entries_size;
        r->_append_entries_rpc_in_fly.pop_front();
    }

    // TODO: caught up logic
    // r->_notify_on_caught_up(0, false);

    // Try continue sending entries
    r->send_entries();
    return;
}

void Replicator::try_send_entries_in_new_bthread(int64_t log_index) {
    // FIXME: Is this safe wihtout locking?
    if (_stop_sending_rpc || log_index < _next_index || _st.st == BLOCKING) {
        REPLICATOR_LOG(WARNING)
            << "Group " << _group_id << " Replicator=" << _id.value
            << " is not sending entries, stop_sending_rpc=" << _stop_sending_rpc
            << ", log_index=" << log_index << ", next_index=" << _next_index
            << ", state=" << _st.st;
        return;
    } else {
        bthread_t tid;
        if (bthread_start_urgent(&tid, NULL, _continue_sending, &_id.value) !=
            0) {
            MLOG(ERROR) << "Fail to start bthread";
            _continue_sending(&_id.value);
        }
    }
}

void* Replicator::_continue_sending(void* args) {
    auto ecode = _continue_sending(args, 0);
    if (ecode != 0) {
        MLOG(ERROR) << "Fail to continue sending, error code: " << ecode;
    }
    return nullptr;
}

int Replicator::_continue_sending(void* arg, int error_code) {
    Replicator* r = nullptr;
    auto id_value = *static_cast<uint64_t*>(arg);
    bthread_id_t id = {id_value};
    auto lg = ReplicatorLockGuard(id, r);
    if (!lg.success()) {
        LOG(WARNING) << "Fail to lock replicator, id: " << id.value;
        return -1;
    }

    if (error_code == ETIMEDOUT) {
        // 1. Called when blocking timeout (details see _block())

        LOG(INFO) << "_continue_sending";
        r->_send_empty_entries();
        // Q: Why use sending empty entries to update log index ?
        // A: the follower will return its last log index in response.
        // Q: Why we need to use _send_empty_entries() to reset last_log_index ?
        // A: the last_log_index was updated in send_entries(), befor response,
        // so we need to reset it after a faild rpc.

    } else if (error_code != ESTOP && !r->_stop_sending_rpc) {
        // 2. Called by LogEntryManager when new log entry available.
        r->send_entries();

    } else if (r->_stop_sending_rpc) {
        // The replicator is checking current next index by sending empty
        // entries or install snapshot now. Although the registered waiter will
        // be canceled before the operations, there is still a little chance
        // that LogManger already waked up the waiter, and _continue_sending is
        // waiting to execute.
        REPLICATOR_LOG_R(WARNING)
            << "Group " << r->_group_id << " Replicator=" << id
            << " now is temply stop sending entries";
    } else {
        REPLICATOR_LOG_R(FATAL)
            << "Group " << r->_group_id << " Replicator=" << id
            << " unexpected error_code=" << error_code;
    }
    return 0;
}

void Replicator::_send_empty_entries() {
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<AppendEntriesRequest> request(new AppendEntriesRequest);
    std::unique_ptr<AppendEntriesResponse> response(new AppendEntriesResponse);

    _fill_common_fields(request.get(), _next_index - 1);

    _st.st = APPENDING_ENTRIES;
    _st.first_log_index = _next_index;
    _st.last_log_index = _next_index - 1;

    // _send_empty_entries() is only called for updating last_log_index,
    // so there must be no entries in sending
    if (!_append_entries_rpc_in_fly.empty()) {
        // When leader recovery done, means quorum return response of
        // sendentriesRPC But some replicator may not recieve the response,
        // means there are rpc in fly. At that time, the leader will send empty
        // entries to start the replication, that is this case.
        LOG(INFO) << "Group " << _group_id << " Replicator=" << _id.value
                  << " is sending empty entries, but there are "
                  << _append_entries_rpc_in_fly.size()
                  << " AppendEntriesRequest in fly";
        return;
    }

    _append_entries_rpc_in_fly.push_back(
        FlyingAppendEntriesRpc(_next_index, 0, cntl->call_id()));

    REPLICATOR_LOG(INFO) << "node " << _group_id << ":" << _server_id
                         << " send EmptyEntries to " << _peer_id << " term "
                         << _term << " prev_log_index "
                         << request->prev_log_index()
                         << " last_committed_index "
                         << request->committed_index();

    mtimer.start();
    google::protobuf::Closure* done =
        brpc::NewCallback(_on_append_entries_rpc_returned, _id.value,
                          cntl.get(), request.get(), response.get());

    LogService_Stub stub(&_sending_channel);
    stub.append_entries(cntl.release(), request.release(), response.release(),
                        done);
}

void Replicator::_send_pull_entries_request(int64_t start_index) {
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
    std::unique_ptr<PullEntriesRequest> request(new PullEntriesRequest);
    std::unique_ptr<PullEntriesResponse> response(new PullEntriesResponse);

    request->set_term(_term);
    request->set_group_id(_group_id);
    request->set_server_id(_server_id.to_string());
    request->set_peer_id(_peer_id.to_string());
    request->set_start_index(start_index);

    if (_st.st != LEADER_RECOVERY) {
        REPLICATOR_LOG(WARNING)
            << "Cancle pull_entries_request() due to replicator state "
               "is not LEADER_RECOVERY, it's: "
            << _st.st;
        return;
    }

    REPLICATOR_LOG(INFO) << "node " << _group_id << ":" << _server_id
                         << " send PullEntriesRequest to " << _peer_id
                         << " term " << _term << " start_index "
                         << request->start_index();

    google::protobuf::Closure* done = brpc::NewCallback(
        _on_pull_entries_rpc_returned, _id.value, cntl.get(), request.get(),
        response.get(), butil::monotonic_time_ms());

    LogService_Stub stub(&_sending_channel);
    _pull_entries_rpc_in_fly.push_back(cntl->call_id());
    stub.pull_entries(cntl.release(), request.release(), response.release(),
                      done);
}

void Replicator::_on_pull_entries_rpc_returned(ReplicatorId id,
                                               brpc::Controller* cntl,
                                               PullEntriesRequest* request,
                                               PullEntriesResponse* response,
                                               int64_t rpc_send_time) {
    (void)rpc_send_time;
    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<PullEntriesRequest> req_guard(request);
    std::unique_ptr<PullEntriesResponse> resp_guard(response);

    Replicator* r = nullptr;
    bthread_id_t dummy_id = {id};
    ReplicatorLockGuard lg(dummy_id, r);
    if (!lg.success()) {
        LOG(WARNING) << "Fail to lock replicator ";
        return;
    }

    r->_pull_entries_rpc_in_fly.pop_front();
    if (cntl->Failed()) {
        REPLICATOR_LOG_R(WARNING)
            << "Fail to send PullEntriesRequest to " << cntl->remote_side()
            << " : " << cntl->ErrorText() << ", retry after "
            << FLAGS_raft_retry_replicate_interval_ms << "ms";
        // FIXME: Should we use bthread_timer_add() to sleep a while ?
        bthread_usleep(FLAGS_raft_retry_replicate_interval_ms * 1000UL);
        r->_send_pull_entries_request(request->start_index());
        return;
    }

    auto last_index = response->last_index();
    auto first_index = response->first_index();
    auto entries_size = response->entries_size();
    CHECK_GE(last_index, first_index + entries_size - 1);

    // Get all entries from response
    std::vector<std::shared_ptr<LogEntry>> entries;
    entries.reserve(entries_size);
    butil::IOBuf data_buf;
    data_buf.swap(cntl->response_attachment());
    int64_t index = first_index;
    for (int i = 0; i < entries_size; i++) {
        const EntryMeta& entry = response->entries(i);
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
        index++;
    }

    REPLICATOR_LOG_R(INFO) << "node " << r->_group_id << ":" << r->_server_id
                           << " received PullEntriesResponse from "
                           << r->_peer_id << " first_index " << first_index
                           << " last_index " << last_index << " entries_size "
                           << response->entries_size();

    CHECK(r->_log_collector);

    r->_log_collector->collect_entires(std::move(entries), r->_peer_id,
                                       first_index, last_index);
    bool need_more_entries =
        last_index > 0 && first_index + entries_size - 1 < last_index;
    if (need_more_entries) {
        REPLICATOR_LOG_R(WARNING)
            << "need more entries, first_index " << first_index
            << " last_index " << last_index << " entries_size " << entries_size;
        return r->_send_pull_entries_request(request->start_index() +
                                             entries_size);
    }
}

void Replicator::_notify_on_caught_up(int error_code, bool before_destroy) {
    LOG(INFO) << "judge if on caught";
    if (_catchup_closure == NULL) {
        LOG(INFO) << "catch up closure is null";
        return;
    }
    if (error_code != ETIMEDOUT && error_code != EPERM) {
        if (!_is_catchup(_catchup_closure->_max_margin)) {
            LOG(INFO) << " not catch up yet";
            return;
        }
        if (_catchup_closure->_error_was_set) {
            return;
        }
        _catchup_closure->_error_was_set = true;
        if (error_code) {
            _catchup_closure->status().set_error(error_code, "%s",
                                                 berror(error_code));
        }
        if (_catchup_closure->_has_timer) {
            if (!before_destroy &&
                bthread_timer_del(_catchup_closure->_timer) == 1) {
                // There's running timer task, let timer task trigger
                // on_caught_up to void ABA problem
                return;
            }
        }
    } else {  // Timed out or leader step_down
        if (!_catchup_closure->_error_was_set) {
            _catchup_closure->status().set_error(error_code, "%s",
                                                 berror(error_code));
        }
    }
    LOG(INFO) << "catch up now! replicator:" << _peer_id.to_string();
    Closure* saved_catchup_closure = _catchup_closure;
    _catchup_closure = NULL;
    return run_closure_in_bthread(saved_catchup_closure);
}

}  // namespace mraft