#include <brpc/controller.h>
#include <butil/status.h>
#include <google/protobuf/stubs/callback.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include "cluster.h"
#include "common/configuration.h"
#include "gtest/gtest.h"
#include "replica/raft_node.h"
#include "storage/storage.h"
#include "utils.h"

namespace mraft {

/*
describe:
    1. init cluster with different node number and stream number
    2. shutdown cluster
    3. clean cluster
*/
TEST(FunctionTest,
     DISABLED_raft_node_should_init_and_shutdown_without_election) {
    ClusterImpl cluster;

    int max_node_num = 5;
    int max_stream_num = 5;
    for (int i = 1; i <= max_node_num; ++i) {
        for (int j = 1; j <= max_stream_num; ++j) {
            ClusterOptions options;
            options.nodeNumber = i;
            options.streamNumber = j;
            options.enable_election = false;
            butil::Status st;
            st = cluster.init(options);
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.shutdown();
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.clean();
            ASSERT_TRUE(st.ok()) << st.error_str();
        }
    }
}

TEST(FunctionTest, DISABLED_raft_node_should_init_and_shutdown_with_election) {
    ClusterImpl cluster;

    int max_node_num = 5;
    int max_stream_num = 5;
    for (int i = 1; i <= max_node_num; ++i) {
        for (int j = 1; j <= max_stream_num; ++j) {
            ClusterOptions options;
            options.nodeNumber = i;
            options.streamNumber = j;
            options.enable_election = true;
            butil::Status st;
            st = cluster.init(options);
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.shutdown();
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.clean();
            ASSERT_TRUE(st.ok()) << st.error_str();
        }
    }
}

/*
describe:
    1. Init cluster with single node and numbers of stream
    2. Wait leader
    3. Check brain split
    4. Shutdown cluster
    5. Clean cluster
*/
TEST(FunctionTest, DISABLED_single_node_should_elected_as_leader) {
    ClusterImpl cluster;
    int max_node_num = 1;
    int max_stream_num = 2;
    for (int i = 1; i <= max_node_num; ++i) {
        for (int j = 1; j <= max_stream_num; ++j) {
            ClusterOptions options;
            options.nodeNumber = 1;
            options.streamNumber = 1;
            options.enable_election = true;
            butil::Status st;
            st = cluster.init(options);
            ASSERT_TRUE(st.ok()) << st.error_str();

            // check leader
            RaftNode* node = nullptr;
            st = cluster.wait_leader(node);
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.check_brain_split();
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.shutdown();
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.clean();
            ASSERT_TRUE(st.ok()) << st.error_str();
        }
    }
}

/*
describe:
    1. Init cluster with single node and numbers of stream
    2. Wait leader
    3. Append lots of logs
*/
TEST(FunctionTest, DISABLED_single_node_should_append_log) {
    ClusterImpl cluster;

    ClusterOptions options;
    options.nodeNumber = 1;
    options.streamNumber = 3;
    options.enable_election = true;
    butil::Status st;
    st = cluster.init(options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // check leader
    RaftNode* node = nullptr;
    st = cluster.wait_leader(node);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.check_brain_split();
    ASSERT_TRUE(st.ok()) << st.error_str();

    int log_size_bytes = 1024 * 1024;  // 1MB
    int log_nums = 16;                 // 1GB
    for (int i = 0; i < log_nums; i++) {
        send_one_log(*node, log_size_bytes);

        while (node->commit_index_of_stream(0) < i) {
        }
    }

    st = cluster.shutdown();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.clean();
    ASSERT_TRUE(st.ok()) << st.error_str();
}

// FIXME: rpc will fail in this test
// 初步判定是由于 rpc server 关闭，rpc 工作线程 IO 失败？
// 发送 LogEntry 后，如果 LogEntry 析构，可能会出错
TEST(FunctionTest,
     DISABLED_multi_node_multi_stream_should_append_log_to_leader) {
    ClusterImpl cluster;

    int max_node_num = 3;
    int max_stream_num = 3;
    for (int i = 1; i <= max_node_num; ++i) {
        for (int j = 1; j <= max_stream_num; ++j) {
            ClusterOptions options;
            options.nodeNumber = i;
            options.streamNumber = j;
            options.enable_election = true;
            butil::Status st;
            st = cluster.init(options);
            ASSERT_TRUE(st.ok()) << st.error_str();

            // check leader
            RaftNode* node = nullptr;
            st = cluster.wait_leader(node);
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.check_brain_split();
            ASSERT_TRUE(st.ok()) << st.error_str();

            int log_size_bytes = 1024 * 1024;  // 1MB
            int log_nums = 16;                 // 128MB
            for (int i = 0; i < log_nums; i++) {
                send_one_log(*node, log_size_bytes);

                while (node->commit_index_of_stream(0) < i) {
                }
            }

            st = cluster.shutdown();
            ASSERT_TRUE(st.ok()) << st.error_str();

            st = cluster.clean();
            ASSERT_TRUE(st.ok()) << st.error_str();
        }
    }
}

TEST(FunctionTest, DISABLED_cluster_should_append_log_affter_leader_change) {
    ClusterImpl cluster;

    ClusterOptions options;
    options.nodeNumber = 5;
    options.streamNumber = 3;
    options.enable_election = true;
    butil::Status st;
    st = cluster.init(options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // check leader
    RaftNode* node = nullptr;
    int leader_idx = 0;
    st = cluster.wait_leader(node, 8, &leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.check_brain_split();
    ASSERT_TRUE(st.ok()) << st.error_str();

    // append log
    int log_size_bytes = 1024 * 1024;  // 1MB
    int log_nums = 16;
    int i = 0;
    for (; i < log_nums / 2; i++) {
        send_one_log(*node, log_size_bytes);
        while (node->commit_index_of_stream(0) < i) {
        }
    }

    // shut down leader
    st = cluster.shutdown_node(leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // wait new leader
    st = cluster.wait_leader(node, 8, &leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // append log
    for (; i < log_nums; i++) {
        send_one_log(*node, log_size_bytes);
        while (node->commit_index_of_stream(0) < i) {
        }
    }

    st = cluster.shutdown();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.clean();
    ASSERT_TRUE(st.ok()) << st.error_str();
}

TEST(FunctionTest,
     DISABLED_cluster_should_election_new_leader_when_leader_down) {
    ClusterImpl cluster;

    ClusterOptions options;
    options.nodeNumber = 3;
    options.streamNumber = 3;
    options.enable_election = true;
    butil::Status st;
    st = cluster.init(options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // check leader
    RaftNode* node = nullptr;
    int leader_idx = 0;
    st = cluster.wait_leader(node, 8, &leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.check_brain_split();
    ASSERT_TRUE(st.ok()) << st.error_str();

    int log_size_bytes = 1024 * 1024;  // 1MB
    int log_nums = 16;
    int i = 0;
    for (; i < log_nums / 2; i++) {
        send_one_log(*node, log_size_bytes);

        while (node->commit_index_of_stream(0) < i) {
        }
    }

    st = cluster.shutdown_node(leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.wait_leader(node);
    ASSERT_TRUE(st.ok()) << st.error_str();

    for (; i < log_nums; i++) {
        send_one_log(*node, log_size_bytes);

        while (node->commit_index_of_stream(0) < i) {
        }
    }

    st = cluster.shutdown();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.clean();
    ASSERT_TRUE(st.ok()) << st.error_str();
}

TEST(FunctionTest, DISABLED_follower_should_reject_log_append) {
    ClusterImpl cluster;

    ClusterOptions options;
    options.nodeNumber = 3;
    options.streamNumber = 3;
    options.enable_election = true;
    butil::Status st;
    st = cluster.init(options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // check leader
    RaftNode* node = nullptr;
    int leader_idx = 0;
    st = cluster.wait_leader(node, 8, &leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.check_brain_split();
    ASSERT_TRUE(st.ok()) << st.error_str();

    // get follower
    for (int i = 0; i < options.nodeNumber; i++) {
        if (i != leader_idx) {
            node = nullptr;
            st = cluster.get_node(i, node);
            ASSERT_TRUE(st.ok()) << st.error_str();
            break;
        }
    }

    // append log
    int log_size_bytes = 1024 * 1024;  // 1MB
    int log_nums = 16;
    int i = 0;
    for (; i < log_nums / 2; i++) {
        auto st = send_one_log(*node, log_size_bytes);
        ASSERT_NE(st.ok(), true) << st.error_str();
    }

    st = cluster.shutdown();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.clean();
    ASSERT_TRUE(st.ok()) << st.error_str();
}

TEST(FunctionTest, DISABLED_check_log_consistency_should_work) {
    ClusterImpl cluster;

    ClusterOptions options;
    options.nodeNumber = 3;
    options.streamNumber = 3;
    options.enable_election = true;
    butil::Status st;
    st = cluster.init(options);
    ASSERT_TRUE(st.ok()) << st.error_str();

    // check leader
    RaftNode* node = nullptr;
    int leader_idx = 0;
    st = cluster.wait_leader(node, 8, &leader_idx);
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.check_brain_split();
    ASSERT_TRUE(st.ok()) << st.error_str();

    // append log
    int log_size_bytes = 1024 * 1024;  // 1MB
    int log_nums = 16;
    int i = 0;
    for (; i < log_nums; i++) {
        send_one_log(*node, log_size_bytes);

        while (node->commit_index_of_stream(0) < i) {
        }
    }

    st = cluster.check_log_consistency();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.shutdown();
    ASSERT_TRUE(st.ok()) << st.error_str();

    st = cluster.clean();
    ASSERT_TRUE(st.ok()) << st.error_str();
}



}  // namespace mraft

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    return RUN_ALL_TESTS();
}