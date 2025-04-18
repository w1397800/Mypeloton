#include "log_service.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>

#include "common/configuration.h"
#include "replica/election_module.h"
#include "replica/stream.h"
namespace mraft {

LogServiceImpl::LogServiceImpl(mraft::LogStream *stream,
                               mraft::RaftNode *node) {
    CHECK(stream != nullptr);
    _stream = stream;
    CHECK(node->get_election_module() != nullptr);
    _election_module = node->get_election_module();
}

void LogServiceImpl::append_entries(google::protobuf::RpcController *controller,
                                    const AppendEntriesRequest *request,
                                    AppendEntriesResponse *response,
                                    google::protobuf::Closure *done) {
    return _stream->handle_append_entries_request(
        static_cast<brpc::Controller *>(controller), request, response, done);
}

void LogServiceImpl::install_snapshoot(
    google::protobuf::RpcController *controller,
    const InstallSnapshotRequest *request, InstallSnapshotResponse *response,
    google::protobuf::Closure *done) {
    LOG(FATAL) << "Not implemented";
    (void)controller;
    (void)request;
    (void)response;
    (void)done;
}

void LogServiceImpl::pull_entries(google::protobuf::RpcController *controller,
                                  const PullEntriesRequest *request,
                                  PullEntriesResponse *response,
                                  google::protobuf::Closure *done) {
    return _stream->handle_pull_entries_request(
        static_cast<brpc::Controller *>(controller), request, response, done);
}

void LogServiceImpl::prepare(google::protobuf::RpcController *controller,
                             const ElectionPrepareRequest *request,
                             ElectionPrepareResponse *response,
                             google::protobuf::Closure *done) {
    LOG(FATAL) << "Not implemented";
    (void)controller;
    (void)request;
    (void)response;
    (void)done;
}

void LogServiceImpl::accept(google::protobuf::RpcController *controller,
                            const ElectionAcceptRequest *request,
                            ElectionAcceptResponse *response,
                            google::protobuf::Closure *done) {
    LOG(FATAL) << "Not implemented";
    (void)controller;
    (void)request;
    (void)response;
    (void)done;
}

void LogServiceImpl::prepare_request(
    google::protobuf::RpcController *controller,
    const ElectionPrepareRequest *request, EmptyResponse *response,
    google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (_election_module == nullptr) {
        cntl->SetFailed(ENOENT, "node does not exist");
        return;
    }

    LOG(INFO) << _election_module->_proposer->get_ballot_number();
    CHECK(_election_module->_proposer != nullptr);
    _election_module->handle_prepare_request(request);
}

void LogServiceImpl::prepare_response(
    google::protobuf::RpcController *controller,
    const ElectionPrepareResponse *request, EmptyResponse *response,
    google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (_election_module == nullptr) {
        cntl->SetFailed(ENOENT, "node does not exist");
        return;
    }

    _election_module->handle_prepare_response(request);
}

void LogServiceImpl::accept_request(google::protobuf::RpcController *controller,
                                    const ElectionAcceptRequest *request,
                                    EmptyResponse *response,
                                    google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (_election_module == nullptr) {
        cntl->SetFailed(ENOENT, "node does not exist");
        return;
    }

    _election_module->handle_accept_request(request);
}

void LogServiceImpl::accept_response(
    google::protobuf::RpcController *controller,
    const ElectionAcceptResponse *request, EmptyResponse *response,
    google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (_election_module == nullptr) {
        cntl->SetFailed(ENOENT, "node does not exist");
        return;
    }

    _election_module->handle_accept_response(request);
}

void LogServiceImpl::change_leader(google::protobuf::RpcController *controller,
                                   const ElectionChangeLeaderRequest *request,
                                   EmptyResponse *response,
                                   google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    if (_election_module == nullptr) {
        cntl->SetFailed(ENOENT, "node does not exist");
        return;
    }

    _election_module->handle_change_leader(request);
}

void LogServiceImpl::add_node(google::protobuf::RpcController *controller,
                              const mraft::AddNodeRequest *request,
                              mraft::EmptyResponse *response,
                              google::protobuf::Closure *done) {
    (void)response;
    brpc::ClosureGuard done_guard(done);
    brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

    mraft::PeerId peer_id;
    if (0 != peer_id.parse(request->receiver())) {
        cntl->SetFailed(EINVAL, "peer_id invalid");
        return;
    }

    _stream->handle_add_node(request,
                             static_cast<brpc::Controller *>(controller));
}

void LogServiceImpl::remove_node(google::protobuf::RpcController *controller,
                                 const mraft::RemoveNodeRequest *request,
                                 mraft::EmptyResponse *response,
                                 google::protobuf::Closure *done) {
    (void)response;
    (void)controller;
    (void)request;
    brpc::ClosureGuard done_guard(done);
}

}  // namespace mraft