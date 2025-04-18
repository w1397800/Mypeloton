
#ifndef BRAFT_RAFT_H
#define BRAFT_RAFT_H

#include <brpc/callback.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <butil/status.h>

#include <memory>
#include <string>

#include "common/configuration.h"
#include "rpc/enum.pb.h"
#include "rpc/errno.pb.h"

template <typename T>
class scoped_refptr;

namespace brpc {
class Server;
}  // namespace brpc

namespace mraft {

// enum State
enum RaftNodeState {
    // Don't change the order if you are not sure about the usage.
    STATE_LEADER = 1,
    STATE_FOLLOWER = 2,
    STATE_LEARNER = 3,
    STATE_ERROR = 5,
    STATE_UNINITIALIZED = 6,
    STATE_LEADER_CANDIDATE = 7,
    STATE_LOG_RECOVERY = 8,
    STATE_SHUTDOWN = 9,
    STATE_END,
};

inline const char* state2str(RaftNodeState state) {
    switch (state) {
        case STATE_LEADER:
            return "STATE_LEADER";
        case STATE_FOLLOWER:
            return "STATE_FOLLOWER";
        case STATE_ERROR:
            return "STATE_ERROR";
        case STATE_UNINITIALIZED:
            return "STATE_UNINITIALIZED";
        case STATE_LEADER_CANDIDATE:
            return "STATE_LEADER_CANDIDATE";
        case STATE_LOG_RECOVERY:
            return "STATE_LOG_RECOVERY";
        case STATE_SHUTDOWN:
            return "STATE_SHUTDOWN";
        case STATE_LEARNER:
            return "STATE_LEARNER";
        default:
            return "Unknown";
    }
}

inline bool is_active_state(RaftNodeState s) {
    // FIXME: is STATE_LEADER_CANDIDATE an active state?
    return s == STATE_LEADER || s == STATE_FOLLOWER || s == STATE_LEARNER;
}

const PeerId ANY_PEER(butil::EndPoint(butil::IP_ANY, 0), 0);

// Raft-specific closure which encloses a butil::Status to report if the
// operation was successful.
class Closure : public google::protobuf::Closure {
   public:
    butil::Status& status() { return _st; }
    const butil::Status& status() const { return _st; }

   private:
    butil::Status _st;
};

// Describe a specific error
class Error {
   public:
    Error() : _type(ERROR_TYPE_NONE) {}
    Error(const Error& e) : _type(e._type), _st(e._st) {}
    ErrorType type() const { return _type; }
    const butil::Status& status() const { return _st; }
    butil::Status& status() { return _st; }
    void set_type(ErrorType type) { _type = type; }

    Error& operator=(const Error& rhs) {
        _type = rhs._type;
        _st = rhs._st;
        return *this;
    }

   private:
    // Intentionally copyable
    ErrorType _type;
    butil::Status _st;
};

inline const char* errortype2str(ErrorType t) {
    switch (t) {
        case ERROR_TYPE_NONE:
            return "None";
        case ERROR_TYPE_LOG:
            return "LogError";
        case ERROR_TYPE_STABLE:
            return "StableError";
        case ERROR_TYPE_SNAPSHOT:
            return "SnapshotError";
        case ERROR_TYPE_STATE_MACHINE:
            return "StateMachineError";
    }
    return "Unknown";
}

inline std::ostream& operator<<(std::ostream& os, const Error& e) {
    os << "{type=" << errortype2str(e.type())
       << ", error_code=" << e.status().error_code() << ", error_text=`"
       << e.status().error_cstr() << "'}";
    return os;
}

// Basic message structure of libraft
struct Task {
    Task() : data(NULL), done(nullptr) {}
    Task(butil::IOBuf* data) : data(data), done(nullptr) {}

    // The data applied to StateMachine
    butil::IOBuf* data;

    // Continuation when the data is applied to StateMachine or error occurs.
    std::unique_ptr<Closure> done;
};

}  // namespace mraft

#endif  // BRAFT_RAFT_H
