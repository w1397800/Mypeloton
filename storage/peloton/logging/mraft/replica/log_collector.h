#pragma once

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/synchronization/lock.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"
#include "common/util.h"
#include "replica/ballot.h"
#include "replica/log_entry_manager.h"
#include "rpc/raft.pb.h"

namespace mraft {

class LogStream;

class EntryQuorum {
   public:
    EntryQuorum(std::shared_ptr<LogEntry> entry, int64_t quorum)
        : _entry(entry), _quorum(quorum) {}

    int vote_entry(std::shared_ptr<LogEntry> entry) {
        if (!_entry) {
            _entry = entry;
            return 0;
        }

        _quorum--;

        if (entry->get_term() > _entry->get_term()) {
            _entry = entry;
            return 1;
        }

        return 0;
    }

    std::shared_ptr<LogEntry> entry() { return _entry; }

    bool empty() { return _entry == nullptr; }

    bool granted() { return _quorum <= 0; }

   private:
    std::shared_ptr<LogEntry> _entry;
    int64_t _quorum = 0;
};

class PullEntriesFinishedClosure : public Closure {
   public:
    PullEntriesFinishedClosure(LogStream* stream) : _stream(stream) {}

    // Send a no-op entry to the stream, as the end of recovery.
    void Run();

   private:
    LogStream* _stream = nullptr;
};

class NoopEntryCommittedClosure : public Closure {
   public:
    NoopEntryCommittedClosure(LogStream* stream) : _stream(stream) {}

    // 1. Notify the stream to cancel all the pull entries requests.
    // 2. Write a no-op entry to the log.
    // 3. Change the state of the stream to "AppendEntries".
    void Run();

   private:
    LogStream* _stream = nullptr;
};

struct LogCollectorOptions {
    LogCollectorOptions() = default;
    ~LogCollectorOptions() = default;

    int64_t start_index = 0;

    LogStream* stream = nullptr;

    ClusterConfiguration peers;
};
class LogCollector {
   public:
    LogCollector() = default;
    ~LogCollector() = default;

    void init(const LogCollectorOptions& options);

    // Deal with the response of pull entries request.
    // Called by Replicator::_on_pull_entries_rpc_returned();
    // 1. Get all the entries from the response.
    // 2. fill into _entries, reserve the entry with largest term.
    // 3. Culculate the _end_index when qourum responses are received.
    // 4. Confirm the entries when all the logs are received quorum.
    void collect_entires(std::vector<std::shared_ptr<LogEntry>>&& entires,
                         PeerId peer, int64_t first_index, int64_t last_index);

    int64_t start_index() { return _start_index; }

   private:
    // Use append_entries() to send the entries to the followers.
    // Simulate normal log replication process.
    void _confirm_all_entries(const int64_t index);

    void _check_peer_quorum(std::unique_lock<butil::Mutex> &lock);

   private:
   private:
    int64_t _start_index = 0;
    int64_t _end_index = 0;
    int64_t _confirmed_index = 0;
    int64_t _quorum = 0;

    std::vector<EntryQuorum> _entry_quorums;

    LogStream* _stream = nullptr;

    ClusterConfiguration _peers;

    // record the entries received from each peer.
    // avoid duplicate entries from one peer.
    // string: peer_id.to_string()
    // int64_t: the index of the last entry received from this peer.
    std::unordered_map<std::string, int64_t> _received_index_map;
    // Count the number of peers that have received all the entries.
    int64_t _peer_quorum = 0;

    butil::Mutex _mutex;
};

}  // namespace mraft
