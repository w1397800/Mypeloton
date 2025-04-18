// Copyright (c) 2015 Baidu.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Authors: Zhangyi Chen(chenzhangyi01@baidu.com)

#pragma once

#include <butil/atomicops.h>  // butil::atomic
#include <stdint.h>           // int64_t

#include <deque>
#include <memory>
#include <set>  // std::set

#include "common/raft.h"
#include "common/util.h"
#include "replica/ballot.h"

class FrontendLogger;
namespace mraft {

class LogStream;

// 用于提交日志，原本的实现对应 braft 的 FSMCaller->on_committed()。
// @param committed_index 已经提交的日志的最大 index
using commit_at_func = void (*)(int64_t committed_index, FrontendLogger* logger);

void default_commit_func(int64_t committed_index, FrontendLogger* logger = nullptr);

struct BallotBoxOptions {
    BallotBoxOptions() {}
    commit_at_func _commit_at = default_commit_func;
    LogStream* log_stream = nullptr;
};

struct BallotBoxStatus {
    BallotBoxStatus()
        : committed_index(0), pending_index(0), pending_queue_size(0) {}
    int64_t committed_index;
    int64_t pending_index;
    int64_t pending_queue_size;
};

class BallotBox {
   public:
    BallotBox();
    ~BallotBox();

    int init(const BallotBoxOptions& options);

    // 功能：Set logs in [first_log_index, last_log_index] are stable at |peer|.
    // 对 first_log_index 和 last_log_index 之间的日志进行投票
    // 如果某个 index 达成了 quorum，则将 _last_committed_index 设置为该 index,
    // 并且对该 commit index 位置注册的回调函数进行调用
    // 在两个地方使用：
    //    1. Leader 落盘后
    //    2. Replicator 收到 AppendEntriesResponse 后
    int vote_at(int64_t first_log_index, int64_t last_log_index,
                const PeerId& peer);  // commit_at()

    // 功能：Called when the leader steps down.
    // When a leader steps down, the uncommitted user applications should
    // fail immediately, which the new leader will deal whether to commit or
    // truncate.
    int clear();  // commit_pending_tasks()

    // 功能：Called when a candidate becomes the new leader.
    // According to the raft algorithm, the logs from pervious terms can't be
    // committed until a log at the new term becomes committed, so
    // |new_pending_index| should be |last_log_index| + 1.
    int reset_pending_index(int64_t new_pending_index);

    // 功能：Called by leader，在有新 LogEntry 需要达成共识时，往这里添加
    // ballot， 每条个 log_index 对应的 LogEntry 都需要按序添加。
    int append_ballot_and_closure(const ClusterConfiguration& conf,
                                  std::unique_ptr<Closure> done = nullptr);

    // Called by follower.
    // 功能：从节点处不需要投票，而是根据 Leader 的恢复直接设置
    // _last_committed_index， 和 vote_at() 的结果一样，最终会触发和
    // _last_committed_index 相关的回调函数
    int set_last_committed_index(int64_t last_committed_index);

    int64_t last_committed_index() {
        return _last_committed_index.load(butil::memory_order_acquire);
    }

    void describe(std::ostream& os, bool use_html);

    void get_status(BallotBoxStatus* ballot_box_status);

    void set_comit_at(commit_at_func func, FrontendLogger* logger) {
        CHECK(func != nullptr);
        CHECK(logger != nullptr);
        _commit_at = func;
        this->_logger = logger;
    }

   private:
    // commit 功能回调函数
    commit_at_func _commit_at;

    //   ClosureQueue* _closure_queue; // 暂时不需要处理 task，只考虑投票功能

    // 大锁，整个 ballot box 的操作都需要加锁
    raft_mutex_t _mutex;

    // 用于记录已经提交的日志的最大 index
    butil::atomic<int64_t> _last_committed_index;

    // 记录等待达成共识的最小 Log index
    int64_t _waiting_queue_start_index;

    // Ballots wait for vote, each log_index has a corresponding Ballot.
    std::deque<Ballot> _pending_ballots_queue;

    // Closures wait for excute
    std::deque<std::unique_ptr<Closure>> _pending_closures_queue;

    LogStream* _stream = nullptr;

    FrontendLogger* _logger = nullptr;
};

}  // namespace mraft
