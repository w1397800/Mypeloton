#pragma once

#include <brpc/controller.h>
#include <brpc/server.h>
#include <butil/status.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/log_entry.h"
#include "common/raft.h"
#include "common/util.h"
#include "replica/ballot_box.h"
#include "replica/configuration_manager.h"
#include "replica/log_collector.h"
#include "replica/log_entry_manager.h"
#include "replica/log_service.h"
#include "replica/replicator.h"
#include "rpc/raft.pb.h"

namespace mraft {

class ConfigurationManager;
class LogManager;
class AppendEntriesCache;

struct LogStreamOptions {
    LogStreamOptions() = default;
    ~LogStreamOptions() = default;

    // Directory to store the log entries
    std::string log_file_path;

    // 本 stream 的 server 信息
    GroupId group_id;
    PeerId server_id;

    // 该 stream 的 id，用于在 RaftNode 的配置中获取该 stream 独有的配置
    // 每个 stream 独有
    int64_t stream_id;

    ClusterConfiguration peers;

    LogManager* log_manager;

    RaftNode* raft_node;

    ConfigurationManager* config_manager;

    commit_at_func commit_at;
};

class LogStream {
    friend class FollowerStableClosure;
    friend class LeaderStableClosure;
    friend class AppendEntriesCache;

   public:
    // all the members are initialized in init()
    LogStream(){};
    ~LogStream(){};

    butil::Status init(LogStreamOptions& options);
    void shutdown();

    void append_entries_async(std::vector<std::shared_ptr<Task>>& tasks);

    void append_entries_from_collector(
        std::vector<std::shared_ptr<LogEntry>>& entries,
        std::unique_ptr<Closure> done);

    void append_noop_entry(std::unique_ptr<NoopEntryCommittedClosure> done);

    // 1. reset the term of stream and check it.
    // 2. recovery log from peers.
    // 3. add replicators.
    void on_node_become_leader();
    void _on_node_become_leader_impl();
    // Used for ElectionModule run on_node_become_leader() in a new bthread.
    static void* on_node_become_leader(void* args);
    void TEST_on_node_become_leader_no_recovery();

    // 1. reset the term of stream and check it.
    // 2. stop replicators if any.
    // 3. reset leader_id.
    void on_node_become_follower(int64_t new_term, PeerId leader_id);

    void on_leader_recovery_done();

    int64_t wait_catchup(const PeerId& peer, int64_t max_margin,
                         CatchupClosure* done);

    // 1. stop replicators if any.
    // 3. set leader_id empty.
    int step_down();

    // 任期变更时使用
    // - 调用 ReplicatorGroup::reset_term() 修改其 term
    // - 重置所有 replicator 的 term
    int reset_term(int64_t new_term);

    // init the rpc server and add the rpc service to the server.
    // each stream must run on a different port, so each stream own a
    // brpc::server.
    // @return true if success, otherwise return false.
    butil::Status start_log_server();

    // Get the _group_id + _server_id of this stream
    NodeId node_id() const;

    int64_t commit_index() const;

    int64_t id() const { return _stream_id; }

    std::shared_ptr<LogEntry> get_log_entry(int64_t index);
    int64_t first_log_index() const;
    int64_t last_log_index() const;

    LogDraft get_log_draft() {
        CHECK(_log_entry_manager != nullptr);
        return _log_entry_manager->get_log_draft();
    }

    void reset_commit_at_func(commit_at_func func, FrontendLogger* logger) {
        CHECK(func != nullptr);
        CHECK(logger != nullptr);
        _ballot_box->set_comit_at(func, logger);
    }

    //===--------------------------------------------------------------------===//
    // RPC handlers
    //===--------------------------------------------------------------------===//

    // AppendEntries RPC 的具体实现，遵循 raft 协议的实现
    // - 前四个参数均是 brpc 的参数，用于接收请求和返回响应
    // - from_append_entries_cache 用于区分是从乱序 cache 中调用还是直接调用
    // 1. Use LogEntryManager to stablelze the log entries.
    // 2. Update the commit index of BallotBox in FollowerStableClosure::Run()
    // 3. Return the response to Leader in FollowerStableClosure::Run()
    void handle_append_entries_request(brpc::Controller* cntl,
                                       const AppendEntriesRequest* request,
                                       AppendEntriesResponse* response,
                                       google::protobuf::Closure* done,
                                       bool from_append_entries_cache = false);

    // used for throughput test
    // just return success for each request
    void TEST_handle_append_entries_request_nocheck(
        brpc::Controller* cntl, const AppendEntriesRequest* request,
        AppendEntriesResponse* response, google::protobuf::Closure* done);

    void handle_install_snapshot_request(brpc::Controller* cntl,
                                         const InstallSnapshotRequest* request,
                                         InstallSnapshotResponse* response,
                                         google::protobuf::Closure* done);

    void handle_pull_entries_request(brpc::Controller* cntl,
                                     const PullEntriesRequest* request,
                                     PullEntriesResponse* response,
                                     google::protobuf::Closure* done);

    void handle_add_node(const AddNodeRequest* request, brpc::Controller* cntl);

    int add_new_replicator(PeerId new_stream_addr);

   private:
    // Check the term when receive a AppendEntriesRPC
    // Follower should step down if the term is larger than current term.
    void _check_term_increase();

    int _start_leader_recovery();

    // 开启日志复制，在节点当选 Leader 时调用
    // - 创建所有 replicator
    // - 所有 replicator 在 LogManager 注册回调函数，在 LogManager
    // 有新日志到来时，在新线程调用该回调函数进行日志复制
    int _start_replicators();

    int64_t _get_start_index();
    //===--------------------------------------------------------------------===//
    // Append entries cache
    //===--------------------------------------------------------------------===//

    // 1. return false if the append_entires_cache size is set to 0.
    bool _handle_out_of_order_append_entries(
        brpc::Controller* cntl, const AppendEntriesRequest* request,
        AppendEntriesResponse* response, google::protobuf::Closure* done,
        int64_t local_last_index);

    // 每次收到 AppendEntries RPC 时，都会检查一下 cache 中是否有可以处理的 RPC
    void _check_append_entries_cache(int64_t local_last_index);

    // use handle_append_entries_from_cache() to handle all the rpcs in the
    // cache.
    static void* _handle_append_entries_from_cache(void* arg);
    static void _on_append_entries_cache_timedout(void* arg);
    static void* _handle_append_entries_cache_timedout(void* arg);

    // Initialized in _handle_out_of_order_append_entries()
    // Destroyed when empty
    std::unique_ptr<AppendEntriesCache> _append_entries_cache;
    int64_t _append_entries_cache_version{0};

    RaftNodeState _state() const;

   private:
    //===--------------------------------------------------------------------===//
    // Set by LogStreamOptions
    //===--------------------------------------------------------------------===//

    // 本 stream 的 server 信息
    GroupId _group_id;
    PeerId _server_id;

    // 该 stream 的 id，用于在 RaftNode 的配置中获取该 stream 独有的配置
    // 每个 stream 独有
    int32_t _stream_id;

    // each stream has a rpc server, and each rpc server has a rpc service.
    // so that we can set different port for each stream.
    std::unique_ptr<brpc::Server> _brpc_server;

    // 用于获取 term，节点成分等关键信息
    RaftNode* _raft_node;

    // 用于传递对上层的一些操作，比如拒绝落盘日志，回滚事务等。
    LogManager* _log_manager;

    ConfigurationManager* _conf_manager;

    // lock the stream
    // 1. now be used in handle_append_entries_request
    butil::Mutex _mutex;

    // used for log entry storage and management
    std::unique_ptr<LogEntryManager> _log_entry_manager;

    // For vote, each log entry will vote in the ballot box.
    std::unique_ptr<BallotBox> _ballot_box;

    // For log replication
    std::unique_ptr<ReplicatorManager> _rep_manager;

    std::unique_ptr<LogCollector> _log_collector;
    //===--------------------------------------------------------------------===//
    // Changeble members
    //===--------------------------------------------------------------------===//
    int64_t _current_term = 0;

    // 获取配置信息
    ClusterConfiguration peers;
    PeerId _leader_id;
};

struct AppendEntriesRpc : public butil::LinkNode<AppendEntriesRpc> {
    brpc::Controller* cntl;
    const AppendEntriesRequest* request;
    AppendEntriesResponse* response;
    google::protobuf::Closure* done;
    int64_t receive_time_ms;
};

struct HandleAppendEntriesFromCacheArg {
    LogStream* stream;
    butil::LinkedList<AppendEntriesRpc> rpcs;
};

struct AppendEntriesCacheTimerArg {
    LogStream* stream;
    int64_t timer_version;
    int64_t cache_version;
    int64_t timer_start_ms;
};
// A simple cache to temporaryly store out-of-order AppendEntries requests.
class AppendEntriesCache {
   public:
    AppendEntriesCache(LogStream* stream, int64_t version)
        : _stream(stream),
          _timer(bthread_timer_t()),
          _cache_version(0),
          _timer_version(0) {
        (void)version;
    }

    int64_t first_index() const;
    int64_t cache_version() const;
    bool empty() const;
    // append the rpc to the cache's tail, pop the oldest rpc if full.
    bool store(AppendEntriesRpc* rpc);
    void process_runable_rpcs(int64_t local_last_index);
    void clear();
    void do_handle_append_entries_cache_timedout(int64_t timer_version,
                                                 int64_t timer_start_ms);

   private:
    void ack_fail(AppendEntriesRpc* rpc);
    void start_to_handle(HandleAppendEntriesFromCacheArg* arg);
    bool start_timer();
    void stop_timer();

    LogStream* _stream;
    butil::LinkedList<AppendEntriesRpc> _rpc_queue;
    std::map<int64_t, AppendEntriesRpc*> _rpc_map;
    bthread_timer_t _timer;
    int64_t _cache_version;
    int64_t _timer_version;
};

//===--------------------------------------------------------------------===//
// Closure for Leader flush logs
//===--------------------------------------------------------------------===//

class LeaderStableClosure : public LogEntryManager::StableClosure {
   public:
    void Run();
    LeaderStableClosure(const NodeId& node_id, size_t nentries,
                        BallotBox* ballot_box);
    ~LeaderStableClosure(){};
    friend class LogStream;
    NodeId _node_id;
    size_t _nentries;
    BallotBox* _ballot_box;
    MetricTimer _mtimer;
};

class FollowerStableClosure : public LogEntryManager::StableClosure {
    friend class LogStream;

   public:
    FollowerStableClosure(brpc::Controller* cntl,
                          const AppendEntriesRequest* request,
                          AppendEntriesResponse* response,
                          google::protobuf::Closure* done, LogStream* stream,
                          int64_t term);

    // Call _run_impl()
    void Run();

   private:
    ~FollowerStableClosure();

    // 1. Return RPC response to Leader
    // 2. Update the commit index of BallotBox, trigger apply.
    void _run_impl();

    brpc::Controller* _cntl;
    const AppendEntriesRequest* _request;
    AppendEntriesResponse* _response;
    google::protobuf::Closure* _done;
    LogStream* _stream;
    int64_t _term;
};

}  // namespace mraft
