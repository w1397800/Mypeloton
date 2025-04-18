#include "replica/ballot_box.h"

#include <bthread/unstable.h>
#include <butil/scoped_lock.h>
#include <bvar/latency_recorder.h>

#include <chrono>

#include "common/raft.h"
#include "common/util.h"
#include "replica/ballot.h"
#include "replica/stream.h"
// #include "braft/fsm_caller.h"
// #include "braft/closure_queue.h"

namespace mraft {

BallotBox::BallotBox()
    : _last_committed_index(0), _waiting_queue_start_index(0) {}

BallotBox::~BallotBox() { clear(); }

void default_commit_func(int64_t committed_index, FrontendLogger* logger) {
    // LOG(INFO) << "LogEntry committed, index: " << committed_index;
    (void)committed_index;
    (void)logger;
}

int BallotBox::init(const BallotBoxOptions& options) {
    if (options._commit_at == nullptr) {
        LOG(ERROR) << "commit_at is NULL";
        return EINVAL;
    }
    _commit_at = options._commit_at;
    _stream = options.log_stream;
    CHECK(_stream);
    return 0;
}

int BallotBox::vote_at(int64_t first_log_index, int64_t last_log_index,
                       const PeerId& peer) {
    // FIXME: The cricital section is unacceptable because it
    // blocks all the other Replicators and LogManagers
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_waiting_queue_start_index == 0) {
        return EINVAL;
    }
    if (last_log_index < _waiting_queue_start_index) {
        return 0;
    }
    if (last_log_index >=
        _waiting_queue_start_index + (int64_t)_pending_ballots_queue.size()) {
        return ERANGE;
    }

    int64_t last_committed_index = 0;
    const int64_t start_at =
        std::max(_waiting_queue_start_index, first_log_index);
    Ballot::PosHint pos_hint;
    for (int64_t log_index = start_at; log_index <= last_log_index;
         ++log_index) {
        Ballot& bl =
            _pending_ballots_queue[log_index - _waiting_queue_start_index];
        pos_hint = bl.grant(peer, pos_hint);
        if (bl.granted()) {
            last_committed_index = log_index;
        }
    }

    if (last_committed_index == 0) {
        return 0;
    }

    // When removing a peer off the raft group which contains even number of
    // peers, the quorum would decrease by 1, e.g. 3 of 4 changes to 2 of 3. In
    // this case, the log after removal may be committed before some previous
    // logs, since we use the new configuration to deal the quorum of the
    // removal request, we think it's safe to commit all the uncommitted
    // previous logs, which is not well proved right now
    for (int64_t index = _waiting_queue_start_index;
         index <= last_committed_index; ++index) {
        _pending_ballots_queue.pop_front();

        auto done = std::move(_pending_closures_queue.front());
        _pending_closures_queue.pop_front();
        if (done) {
            run_closure_in_bthread(done.release());
        }
    }

    _waiting_queue_start_index = last_committed_index + 1;
    _last_committed_index.store(last_committed_index,
                                butil::memory_order_relaxed);
    lck.unlock();
    // The order doesn't matter
    _commit_at(last_committed_index, nullptr);
    LOG(INFO) << "LogEntry committed, index: " << last_committed_index
              << ", Stream id: " << _stream->id();

    return 0;
}

int BallotBox::append_ballot_and_closure(const ClusterConfiguration& conf,
                                         std::unique_ptr<Closure> done) {
    Ballot bl;
    if (bl.init(conf, nullptr) != 0) {
        CHECK(false) << "Fail to init ballot";
        return -1;
    }

    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_waiting_queue_start_index > 0);
    _pending_ballots_queue.push_back(Ballot());
    _pending_ballots_queue.back().swap(bl);

    _pending_closures_queue.emplace_back(std::move(done));
    return 0;
}

int BallotBox::set_last_committed_index(int64_t last_committed_index) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    if (_waiting_queue_start_index != 0 || !_pending_ballots_queue.empty()) {
        CHECK(last_committed_index < _waiting_queue_start_index)
            << "node changes to leader, pending_index="
            << _waiting_queue_start_index
            << ", parameter last_committed_index=" << last_committed_index;
        return -1;
    }
    if (last_committed_index <
        _last_committed_index.load(butil::memory_order_relaxed)) {
        return EINVAL;
    }
    if (last_committed_index >
        _last_committed_index.load(butil::memory_order_relaxed)) {
        _last_committed_index.store(last_committed_index,
                                    butil::memory_order_relaxed);
        lck.unlock();
        _commit_at(last_committed_index, nullptr);
        LOG(INFO) << "LogEntry committed in follower, index "
                  << last_committed_index << ", Stream " << _stream->id();
    }
    return 0;
}

int BallotBox::clear() {
    std::deque<Ballot> saved_meta;
    {
        BAIDU_SCOPED_LOCK(_mutex);
        saved_meta.swap(_pending_ballots_queue);
        _waiting_queue_start_index = 0;
    }
    return 0;
}

int BallotBox::reset_pending_index(int64_t new_pending_index) {
    BAIDU_SCOPED_LOCK(_mutex);
    CHECK(_pending_ballots_queue.empty())
        << "pending_index " << _waiting_queue_start_index
        << " new_pending_index " << new_pending_index << " pending_meta_queue "
        << _pending_ballots_queue.size();
    CHECK_GT(new_pending_index,
             _last_committed_index.load(butil::memory_order_relaxed));
    _waiting_queue_start_index = new_pending_index;
    return 0;
}

void BallotBox::describe(std::ostream& os, bool use_html) {
    std::unique_lock<raft_mutex_t> lck(_mutex);
    int64_t committed_index = _last_committed_index;
    int64_t pending_index = 0;
    size_t pending_queue_size = 0;
    if (_waiting_queue_start_index != 0) {
        pending_index = _waiting_queue_start_index;
        pending_queue_size = _pending_ballots_queue.size();
    }
    lck.unlock();
    const char* newline = use_html ? "<br>" : "\r\n";
    os << "last_committed_index: " << committed_index << newline;
    if (pending_queue_size != 0) {
        os << "pending_index: " << pending_index << newline;
        os << "pending_queue_size: " << pending_queue_size << newline;
    }
}

void BallotBox::get_status(BallotBoxStatus* status) {
    if (!status) {
        return;
    }
    std::unique_lock<raft_mutex_t> lck(_mutex);
    status->committed_index = _last_committed_index;
    if (_pending_ballots_queue.size() != 0) {
        status->pending_index = _waiting_queue_start_index;
        status->pending_queue_size = _pending_ballots_queue.size();
    }
}

}  // namespace mraft
