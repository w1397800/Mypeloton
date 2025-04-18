// Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
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

#include "common/configuration.h"

namespace mraft {

class Ballot {
   public:
    struct PosHint {
        PosHint() : pos0(-1), pos1(-1) {}
        int pos0;  // 在 _peers 中的位置
        int pos1;  // 在 _old_peers 中的位置
    };

    Ballot();
    ~Ballot();
    void swap(Ballot& rhs) {
        _peers.swap(rhs._peers);
        std::swap(_quorum, rhs._quorum);
        _old_peers.swap(rhs._old_peers);
        std::swap(_old_quorum, rhs._old_quorum);
    }

    // 功能：初始化 Ballot，传入集群配置
    int init(const ClusterConfiguration& conf,
             const ClusterConfiguration* old_conf);

    // 功能：某个 peer 对其进行投票
    void grant(const PeerId& peer);

    PosHint grant(const PeerId& peer, PosHint hint);

    // 功能：判断是否已经达成 quorum
    bool granted() const { return _quorum <= 0 && _old_quorum <= 0; }

   private:
    struct UnfoundPeerId {
        UnfoundPeerId(const PeerId& peer_id) : peer_id(peer_id), found(false) {}
        PeerId peer_id;
        bool found;
        bool operator==(const PeerId& id) const { return peer_id == id; }
    };

    // 功能：在 peers 中查找 peer，如果找到，返回其在 peers 中的位置，否则返回
    // -1
    std::vector<UnfoundPeerId>::iterator _find_peer(
        const PeerId& peer, std::vector<UnfoundPeerId>& peers, int pos_hint) {
        if (pos_hint < 0 || pos_hint >= (int)peers.size() ||
            peers[pos_hint].peer_id != peer) {
            for (std::vector<UnfoundPeerId>::iterator iter = peers.begin();
                 iter != peers.end(); ++iter) {
                if (*iter == peer) {
                    return iter;
                }
            }
            return peers.end();
        }
        return peers.begin() + pos_hint;
    }

    // 当前配置
    std::vector<UnfoundPeerId> _peers;

    // 当前配置的 quorum 数量
    int _quorum;

    // 两阶段配置变更时，旧配置
    std::vector<UnfoundPeerId> _old_peers;
    int _old_quorum;
};

};  // namespace mraft
