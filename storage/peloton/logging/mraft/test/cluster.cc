#include "cluster.h"

#include <butil/status.h>

#include <cstddef>
#include <vector>

#include "common/raft.h"
#include "replica/raft_node.h"
#include "utils.h"

DEFINE_int64(max_node_init_timeout_us, 5 * 1000 * 1000,
             "max election timeout in ms");

namespace mraft {
butil::Status ClusterImpl::init(ClusterOptions& options) {
    butil::Status st;
    st = this->clean();
    if (!st.ok()) {
        LOG(ERROR) << st.error_str();
        return st;
    }
    _enable_election = options.enable_election;
    _nodeNumber = options.nodeNumber;
    _streamNumber = options.streamNumber;

    if (!std::filesystem::create_directory(_runtime_path)) {
        LOG(ERROR) << "create directory failed: " << _runtime_path;
        return butil::Status(EINVAL, "create directory failed");
    }
    st = config_generator(_nodeNumber, _streamNumber, _runtime_path);
    if (!st.ok()) {
        LOG(ERROR) << st.error_str();
        return st;
    }

    return _init_nodes();
}

butil::Status ClusterImpl::shutdown() {
    for (int i = 0; i < _nodeNumber; ++i) {
        if (_nodes[i]) _nodes[i]->shutdown();
    }
    return butil::Status::OK();
}

butil::Status ClusterImpl::clean() {
    if (!_nodes.empty()) {
        _nodes.clear();
    }

    if (std::filesystem::exists(_runtime_path)) {
        if (!std::filesystem::remove_all(_runtime_path)) {
            LOG(ERROR) << "remove directory failed: " << _runtime_path;
            return butil::Status(EINVAL, "remove directory failed");
        }
    } else {
        LOG(INFO) << "Directory does not exist, no need to remove: "
                  << _runtime_path;
    }
    return butil::Status::OK();
}

butil::Status ClusterImpl::shutdown_node(int node_index,
                                         std::unique_ptr<Closure> done) {
    if (node_index < 0 || node_index >= _nodeNumber) {
        LOG(ERROR) << "node_index out of range: " << node_index << " vs "
                   << _nodeNumber;
        return butil::Status(EINVAL, "node_index out of range");
    }
    if (!_nodes[node_index]) {
        LOG(ERROR) << "node is nullptr: " << node_index;
        return butil::Status(EINVAL, "node_index is nullptr");
    }
    _nodes[node_index]->shutdown(std::move(done));
    _nodes[node_index].reset();
    return butil::Status::OK();
}

butil::Status ClusterImpl::set_leader(int node_index) {
    if (!_enable_election) {
        LOG(ERROR) << "election is disabled";
        return butil::Status(EINVAL, "election is disabled");
    }

    if (node_index < 0 || node_index >= _nodeNumber) {
        LOG(ERROR) << "node_index out of range: " << node_index << " vs "
                   << _nodeNumber;
        return butil::Status(EINVAL, "node_index out of range");
    }

    if (!_nodes[node_index]) {
        LOG(ERROR) << "node is nullptr: " << node_index;
        return butil::Status(EINVAL, "node_index is nullptr");
    }

    _nodes[node_index]->on_node_become_leader();

    int64_t start_time = butil::gettimeofday_us();
    while (_nodes[node_index]->state() != STATE_LEADER) {
        if (butil::gettimeofday_us() - start_time >
            FLAGS_max_node_init_timeout_us) {
            LOG(ERROR) << "set_leader timeout";
            return butil::Status(EINVAL, "set_leader timeout");
        }
    }
    return butil::Status::OK();
}

butil::Status ClusterImpl::get_leader_index(int& idx) {
    idx = -1;
    if (!_enable_election) {
        LOG(ERROR) << "election is disabled";
        return butil::Status(EINVAL, "election is disabled");
    }
    for (int i = 0; i < _nodeNumber; ++i) {
        if (_nodes[i] && _nodes[i]->state() == STATE_LEADER) {
            idx = i;
            return butil::Status::OK();
        }
    }
    return butil::Status(EINVAL, "no leader");
}

butil::Status ClusterImpl::get_leader(RaftNode*& node, int* idx) {
    if (!_enable_election) {
        LOG(ERROR) << "election is disabled";
        node = nullptr;
        return butil::Status(EINVAL, "election is disabled");
    }
    for (int i = 0; i < _nodeNumber; ++i) {
        if (_nodes[i] && _nodes[i]->state() == STATE_LEADER) {
            node = _nodes[i].get();
            if (idx) {
                *idx = i;
            }
            return butil::Status::OK();
        }
    }
    node = nullptr;
    return butil::Status::OK();
}

butil::Status ClusterImpl::wait_leader(RaftNode*& node, int timeout_s,
                                       int* idx) {
    node = nullptr;
    int64_t start_time = butil::gettimeofday_us();
    int idx_wrapper = -1;
    while (true) {
        auto status = get_leader(node, &idx_wrapper);
        if (node != nullptr) {
            CHECK(status.ok());
            if (idx) {
                *idx = idx_wrapper;
            }
            return status;
        }
        if (!status.ok()) {
            LOG(ERROR) << "get_leader failed: " << status.error_str();
            return status;
        }
        if (butil::gettimeofday_us() - start_time > timeout_s * 1000 * 1000) {
            LOG(ERROR) << "wait_leader timeout";
            return butil::Status(EINVAL, "wait_leader timeout");
        }
    }
}

butil::Status ClusterImpl::get_node(int node_index, RaftNode*& node) {
    if (node_index < 0 || node_index >= _nodeNumber) {
        LOG(ERROR) << "node_index out of range: " << node_index << " vs "
                   << _nodeNumber;
        node = nullptr;
        return butil::Status(EINVAL, "node_index out of range");
    }
    if (!_nodes[node_index]) {
        LOG(ERROR) << "node is nullptr: " << node_index;
        node = nullptr;
        return butil::Status(EINVAL, "node_index is nullptr");
    }
    node = _nodes[node_index].get();
    return butil::Status::OK();
}

butil::Status ClusterImpl::_init_nodes() {
    for (int i = 0; i < _nodeNumber; ++i) {
        RaftNode::RaftNodeOptions options;
        options.log_file_path = _runtime_path;
        options.confg_file_path =
            _runtime_path + formatFileName(_nodeNumber, _streamNumber, i);

        std::unique_ptr<RaftNode> node(new RaftNode());

        auto st = node->init(options);
        if (!st.ok()) {
            LOG(ERROR) << st.error_str();
            return st;
        }

        st = node->start(_enable_election);
        if (!st.ok()) {
            LOG(ERROR) << st.error_str();
            return st;
        }

        int64_t start_time = butil::gettimeofday_us();
        while (node->state() != STATE_LEADER &&
               node->state() != STATE_FOLLOWER) {
            if (butil::gettimeofday_us() - start_time >
                FLAGS_max_node_init_timeout_us) {
                LOG(ERROR) << "set_leader timeout";
                return butil::Status(EINVAL, "set_leader timeout");
            }
        }

        _nodes.push_back(std::move(node));
    }
    return butil::Status::OK();
}

butil::Status ClusterImpl::check_brain_split() {
    if (!_enable_election) {
        LOG(ERROR) << "election is disabled";
        return butil::Status(EINVAL, "election is disabled");
    }
    int leader_index = -1;
    auto status = get_leader_index(leader_index);
    if (!status.ok()) {
        LOG(ERROR) << "get_leader_index failed: " << status.error_str();
        return status;
    }
    for (int i = 0; i < _nodeNumber; ++i) {
        if (i == leader_index) {
            continue;
        }
        if (!_nodes[i] && _nodes[i]->state() == STATE_LEADER) {
            LOG(ERROR) << "brain split detected";
            return butil::Status(EINVAL, "brain split detected");
        }
    }
    return butil::Status::OK();
}

// 1. Index should be continuous increasing
// 2. Term should not be decreasing
butil::Status simple_logdraft_check(const LogDraft& ld) {
    if (ld.ids.empty()) {
        return butil::Status::OK();
    }
    for (size_t i = 1; i < ld.ids.size(); ++i) {
        if (ld.ids[i].index != ld.ids[i - 1].index + 1 ||
            ld.ids[i].term < ld.ids[i - 1].term) {
            LOG(ERROR) << "log draft check failed: " << ld.ids[i - 1] << " vs "
                       << ld.ids[i];
            return butil::Status(EINVAL, "log draft check failed");
        }
    }
    return butil::Status::OK();
}

butil::Status ClusterImpl::check_log_consistency() {
    if (!_enable_election) {
        LOG(ERROR) << "election is disabled";
        return butil::Status(EINVAL, "election is disabled");
    }
    RaftNode* leader = nullptr;
    int leader_idx = -1;
    auto st = wait_leader(leader, 8, &leader_idx);
    if (!st.ok()) {
        LOG(ERROR) << "wait_leader failed: " << st.error_str();
        return st;
    }

    auto leader_ld = leader->get_log_drafts();
    st = simple_logdraft_check(leader_ld[0]);
    if (!st.ok()) {
        LOG(ERROR) << "simple_logdraft_check failed: " << st.error_str();
        return st;
    }

    // after leader recovery, each stream should have a quorum replica
    // that has received all the logs.
    // "_nodeNumber / 2 + 1 - 1" is because the leader itself is count;
    std::vector<int> quorums(_nodeNumber, _nodeNumber / 2 + 1 - 1);
    // Compare with each follower
    for (int i = 0; i < _nodeNumber; ++i) {
        if (i == leader_idx) {
            continue;
        }
        if (!_nodes[i]) {
            LOG(WARNING) << "node is nullptr: " << i;
            continue;
        }
        auto follower_ld = _nodes[i]->get_log_drafts();
        st = simple_logdraft_check(follower_ld[0]);
        if (!st.ok()) {
            LOG(ERROR) << "simple_logdraft_check failed: " << st.error_str();
            return st;
        }

        // Stream size must match
        if (leader_ld.size() != follower_ld.size()) {
            LOG(ERROR) << "log size not equal: " << leader_ld.size() << " vs "
                       << follower_ld.size();
            return butil::Status(EINVAL, "log size not equal");
        }

        // Check each stream
        for (size_t j = 0; j < leader_ld.size(); ++j) {
            auto& leader_sld = leader_ld[j].ids;
            auto& follower_sld = follower_ld[j].ids;
            size_t k = 0;
            for (; k < leader_sld.size(); ++k) {
                // The log index must match
                if (leader_sld[k].index != follower_sld[k].index) {
                    LOG(ERROR) << "log index not equal: " << leader_sld[k]
                               << " vs " << follower_sld[k];
                    return butil::Status(EINVAL, "log index not equal");
                }

                // The Leader's term should not be less than the follower's+
                if (leader_sld[k].term < follower_sld[k].term) {
                    LOG(ERROR) << "Leader term is less than follower's: "
                               << leader_sld[k] << " vs " << follower_sld[k];
                    return butil::Status(EINVAL, "log term not equal");
                }

                // This will happen if this replica did not receive the log when
                // leader recovery
                if (leader_sld[k].term > follower_sld[k].term) {
                    LOG(WARNING) << "Leader term is greater than follower's: "
                                 << leader_sld[k] << " vs " << follower_sld[k];
                    break;
                }
            }

            // There's one replica that has recived all the logs
            if (k == leader_sld.size()) {
                quorums[i]--;
            }
        }  // end for each stream
    }      // end for each follower

    return butil::Status::OK();
}

void ClusterImpl::start() {}
}  // namespace mraft