#pragma once

#include <butil/status.h>

#include <cstddef>
#include <string>
#include <vector>

#include "replica/raft_node.h"

namespace mraft {

struct ClusterOptions {
    bool enable_election = true;
    int nodeNumber;
    int streamNumber;
};

class Cluster {
   public:
    Cluster(){};

   protected:
    bool _enable_election = true;
    int _nodeNumber = 0;
    int _streamNumber = 0;
    std::vector<std::unique_ptr<RaftNode>> _nodes;
    std::string _runtime_path = "./runtime/";
};

class ClusterImpl : public Cluster {
   public:
    ClusterImpl(){};
    butil::Status init(ClusterOptions &options);
    butil::Status shutdown();
    butil::Status clean();

    butil::Status shutdown_node(int node_index,
                                std::unique_ptr<Closure> done = nullptr);
    butil::Status restart_node(int node_index);
    
    butil::Status set_leader(int node_index);
    butil::Status get_leader_index(int &idx);
    butil::Status get_leader(RaftNode *&node, int *idx = nullptr);
    butil::Status wait_leader(RaftNode *&node, int timeout_s = 8, int *idx = nullptr);
    butil::Status get_node(int node_index, RaftNode *&node);
    butil::Status check_brain_split();
    butil::Status check_log_consistency();
    void start();

    size_t get_node_nums() { return _nodes.size(); }

   private:
    butil::Status _init_nodes();
};
}  // namespace mraft