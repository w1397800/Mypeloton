#pragma once

// #include <glog/logging.h>

#include <butil/logging.h>

#include <cassert>

#include "rpc/raft.pb.h"  // AppendEntriesRPC

namespace mraft {

class LogStream;
class ElectionModule;
class RaftNode;
// for multi-stream, each local node has a rpc server.
// this server should be started in LocalNode::add_append_entries_rpc_server().
class LogServiceImpl : public LogService {
   public:
    LogServiceImpl(LogStream *stream, RaftNode *node);

    ~LogServiceImpl() = default;

    void append_entries(google::protobuf::RpcController *controller,
                        const AppendEntriesRequest *request,
                        AppendEntriesResponse *response,
                        google::protobuf::Closure *done);

    void install_snapshoot(google::protobuf::RpcController *controller,
                           const InstallSnapshotRequest *request,
                           InstallSnapshotResponse *response,
                           google::protobuf::Closure *done);

    void pull_entries(google::protobuf::RpcController *controller,
                      const PullEntriesRequest *request,
                      PullEntriesResponse *response,
                      google::protobuf::Closure *done);

    void prepare(google::protobuf::RpcController *controller,
                 const ElectionPrepareRequest *request,
                 ElectionPrepareResponse *response,
                 google::protobuf::Closure *done);

    void accept(google::protobuf::RpcController *controller,
                const ElectionAcceptRequest *request,
                ElectionAcceptResponse *response,
                google::protobuf::Closure *done);

    void prepare_request(google::protobuf::RpcController *controller,
                         const ElectionPrepareRequest *request,
                         EmptyResponse *response,
                         google::protobuf::Closure *done);

    void prepare_response(google::protobuf::RpcController *controller,
                          const ElectionPrepareResponse *request,
                          EmptyResponse *response,
                          google::protobuf::Closure *done);

    void accept_request(google::protobuf::RpcController *controller,
                        const ElectionAcceptRequest *request,
                        EmptyResponse *response,
                        google::protobuf::Closure *done);

    void accept_response(google::protobuf::RpcController *controller,
                         const ElectionAcceptResponse *request,
                         EmptyResponse *response,
                         google::protobuf::Closure *done);

    void change_leader(google::protobuf::RpcController *controller,
                       const ElectionChangeLeaderRequest *request,
                       EmptyResponse *response,
                       google::protobuf::Closure *done);

    void add_node(google::protobuf::RpcController *controller,
                  const AddNodeRequest *request,
                  EmptyResponse *response,
                  google::protobuf::Closure *done);

    void remove_node(google::protobuf::RpcController *controller,
                  const RemoveNodeRequest *request,
                  EmptyResponse *response,
                  google::protobuf::Closure *done);

   private:
    LogStream *_stream;
    ElectionModule *_election_module;
};

}  // namespace mraft