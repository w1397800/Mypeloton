#include "replica/log_collector.h"

#include <bthread/mutex.h>
#include <butil/iobuf.h>
#include <butil/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"
#include "common/util.h"
#include "replica/stream.h"
#include "rpc/raft.pb.h"

namespace mraft {

#define COLLECTOR_LOG(level)                                              \
    LOG(level) << "[" << __FUNCTION__ << ", stream_id: " << _stream->id() \
               << "] "

void LogCollector::init(const LogCollectorOptions& options) {
    _quorum = options.peers.size() / 2 + 1;
    _peers = options.peers;
    _stream = options.stream;
    _start_index = options.start_index;
    _end_index = _start_index;

    for (auto& peer : _peers) {
        _received_index_map[peer.to_string()] = _start_index - 1;
    }
    _peer_quorum = _quorum;

    COLLECTOR_LOG(INFO) << "LogCollector init, start_index: " << _start_index
                        << ", end_index: " << _end_index
                        << ", confirmed_index: " << _confirmed_index
                        << ", quorum: " << _quorum
                        << ", peer_quorum: " << _peer_quorum;
}

void LogCollector::collect_entires(
    std::vector<std::shared_ptr<LogEntry>>&& entires, PeerId peer,
    int64_t first_index, int64_t last_index_of_peer) {
    std::unique_lock<butil::Mutex> lck(_mutex);
    // 1. Aready confirmed all the entries.
    if (_peer_quorum <= 0) {
        COLLECTOR_LOG(INFO)
            << "LogCollector has already confirmed all the entries, "
               "nothing todo.";
        return;
    }

    // 2. This peer has no LogEntry after start_index
    if (last_index_of_peer < _start_index) {
        COLLECTOR_LOG(INFO)
            << "This peer has no LogEntry after start_index, peer: " << peer
            << ", start_index: " << _start_index
            << ", last_index_of_peer: " << last_index_of_peer;
        _check_peer_quorum(lck);
        return;
    }

    // 3. Duplicate entries from this peer.
    int64_t entries_size = static_cast<int64_t>(entires.size());
    int64_t right_boundary = first_index + entries_size - 1;
    if (right_boundary <= _received_index_map[peer.to_string()]) {
        COLLECTOR_LOG(WARNING)
            << "Received duplicate entries from " << peer
            << ", first_index: " << first_index
            << ", entries size: " << entires.size()
            << ", received_index: " << _received_index_map[peer.to_string()];
        return;
    }
    _received_index_map[peer.to_string()] = right_boundary;

    // 4. Vote for each entry.
    for (int64_t i = 0; i < entries_size; ++i) {
        if (first_index + i <= _confirmed_index) {
            continue;
        }
        CHECK(entires[i] != nullptr);
        auto index = first_index + i - _start_index;
        if (index == _entry_quorums.size()) {
            _entry_quorums.emplace_back(nullptr, _quorum);
        }
        _entry_quorums[index].vote_entry(entires[i]);
        if (_entry_quorums[index].granted()) {
            _confirmed_index = _confirmed_index < first_index + i
                                   ? first_index + i
                                   : _confirmed_index;
        }
    }

    // Recieved all the entries from this peer.
    if (right_boundary == last_index_of_peer) {
        _check_peer_quorum(lck);
    }

    COLLECTOR_LOG(INFO) << "LogCollector collect entries from " << peer
                        << ", first_index: " << first_index
                        << ", entries size: " << entires.size()
                        << ", last_index_of_peer: " << last_index_of_peer;
}

void LogCollector::_check_peer_quorum(std::unique_lock<butil::Mutex>& lck) {
    CHECK_GT(_peer_quorum, 0);

    // Confirm all the entries when reach the quorum.
    if (--_peer_quorum == 0) {
        // 1. Check the collected entries.
        std::vector<std::shared_ptr<LogEntry>> entries;
        int64_t last_term = 0;
        for (auto& eq : _entry_quorums) {
            CHECK(eq.entry() != nullptr);
            if (eq.entry()->get_term() < last_term) {
                COLLECTOR_LOG(WARNING)
                    << "skip the entry with smaller term, "
                       "last_term: "
                    << last_term << ", entry_term: " << eq.entry()->get_term();
                break;
            }
            entries.emplace_back(eq.entry());
        }

        // 2. If no entries need to be confirmed, return.
        // This will happen when all the nodes are fresh
        if (entries.empty()) {
            COLLECTOR_LOG(INFO)
                << "LogCollector has no entries to confirm. Start STATE_LEADER";
            lck.unlock();
            run_closure_in_bthread(new NoopEntryCommittedClosure(_stream));
            return;
        }

        // 3. Send the entries to the followers.
        lck.unlock();
        COLLECTOR_LOG(INFO)
            << "LogCollector confirm all entries, start_index: " << _start_index
            << ", end_index: " << _end_index
            << ", confirmed_index: " << _confirmed_index
            << ", quorum: " << _quorum
            << ", entries to confirm: " << entries.size();

        std::unique_ptr<Closure> done(new PullEntriesFinishedClosure(_stream));
        _stream->append_entries_from_collector(entries, std::move(done));
        return;
    }

    COLLECTOR_LOG(INFO) << "quorum: " << _peer_quorum;
}

void PullEntriesFinishedClosure::Run() {
    COLLECTOR_LOG(INFO) << "Leader recovery finished, sending noop log";

    std::unique_ptr<NoopEntryCommittedClosure> done(
        new NoopEntryCommittedClosure(_stream));
    _stream->append_noop_entry(std::move(done));

    delete this;
}

void NoopEntryCommittedClosure::Run() {
    CHECK(_stream);
    _stream->on_leader_recovery_done();

    delete this;
}

};  // namespace mraft
