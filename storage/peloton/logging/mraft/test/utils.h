#pragma once

#include <brpc/controller.h>
#include <butil/status.h>
#include <google/protobuf/stubs/callback.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/configuration.h"
#include "replica/raft_node.h"
#include "storage/storage.h"

namespace mraft {

std::string formatFileName(int nodeNumber, int streamNumber, int nodeIndex);
butil::Status config_generator(int nodeNumber, int streamNumber,
                               std::string path = "./");
butil::Status learner_config_generator(int nodeNumber, int streamNumber,
                                       std::string path);

butil::Status send_one_log(RaftNode& node, int64_t log_size = 1024);

inline void prepare_request(AppendEntriesRequest* request, int req_num,
                            int entries_per_request, int total_entries,
                            brpc::Controller* cntl) {
    request->set_group_id("");
    request->set_server_id("172.0.0.1:8011");
    request->set_peer_id("172.0.0.1:8011");
    if (req_num == 0)
        request->set_prev_log_term(0);
    else
        request->set_prev_log_term(1);
    request->set_term(1);
    request->set_prev_log_index(req_num * entries_per_request);
    request->set_committed_index(req_num * entries_per_request);

    int entries_in_this_request = std::min(
        entries_per_request, total_entries - req_num * entries_per_request);
    EntryMeta em;
    for (int i = 0; i < entries_in_this_request; ++i) {
        em.set_term(1);
        em.set_type(ENTRY_TYPE_DATA);

        em.set_data_len(1024);
        cntl->request_attachment().append(new char[1024], 1024);
        request->add_entries()->Swap(&em);
    }
}

}  // namespace mraft