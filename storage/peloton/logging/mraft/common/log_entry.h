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

#ifndef E05C25F5_F414_49B9_B18A_B95665B23715
#define E05C25F5_F414_49B9_B18A_B95665B23715

#include <cstdint>
#ifndef BRAFT_LOG_ENTRY_H
#define BRAFT_LOG_ENTRY_H

#include <butil/iobuf.h>               // butil::IOBuf
#include <butil/memory/ref_counted.h>  // butil::RefCountedThreadSafe
#include <butil/third_party/murmurhash3/murmurhash3.h>  // fmix64

#include "common/configuration.h"
#include "common/util.h"
#include "rpc/raft.pb.h"

namespace mraft {

// Log identifier
struct LogId {
    LogId() : index(0), term(0) {}
    LogId(int64_t index_, int64_t term_) : index(index_), term(term_) {}
    int64_t index;
    int64_t term;
};

// term start from 1, log index start from 1
struct BraftLogEntry : public butil::RefCountedThreadSafe<BraftLogEntry> {
   public:
    EntryType type;  // log type
    LogId id;
    std::vector<PeerId>* peers;      // peers
    std::vector<PeerId>* old_peers;  // peers
    butil::IOBuf data;

    BraftLogEntry();

   private:
    DISALLOW_COPY_AND_ASSIGN(BraftLogEntry);
    friend class butil::RefCountedThreadSafe<BraftLogEntry>;
    virtual ~BraftLogEntry();
};

struct LogEntry {
   public:
    virtual ~LogEntry();
    EntryType type = ENTRY_TYPE_DATA;  // log type
    LogId id;
    std::vector<PeerId>* peers = nullptr;      // peers
    std::vector<PeerId>* old_peers = nullptr;  // peers
    butil::IOBuf data;

    int64_t pel_transaction_id = 0;

    LogEntry();
    void set_index(int64_t index) { id.index = index; }
    void set_term(int64_t term) { id.term = term; }
    void set_data(char* data, int64_t size) {
        CHECK(data);
        this->data.append(data, size);
    }
    int64_t get_term() const { return id.term; }
    int64_t get_index() const { return id.index; }

   private:
    DISALLOW_COPY_AND_ASSIGN(LogEntry);
};

// Comparators

inline bool operator==(const LogId& lhs, const LogId& rhs) {
    return lhs.index == rhs.index && lhs.term == rhs.term;
}

inline bool operator!=(const LogId& lhs, const LogId& rhs) {
    return !(lhs == rhs);
}

inline bool operator<(const LogId& lhs, const LogId& rhs) {
    if (lhs.term == rhs.term) {
        return lhs.index < rhs.index;
    }
    return lhs.term < rhs.term;
}

inline bool operator>(const LogId& lhs, const LogId& rhs) {
    if (lhs.term == rhs.term) {
        return lhs.index > rhs.index;
    }
    return lhs.term > rhs.term;
}

inline bool operator<=(const LogId& lhs, const LogId& rhs) {
    return !(lhs > rhs);
}

inline bool operator>=(const LogId& lhs, const LogId& rhs) {
    return !(lhs < rhs);
}

struct LogIdHasher {
    size_t operator()(const LogId& id) const {
        return butil::fmix64(id.index) ^ butil::fmix64(id.term);
    }
};

inline std::ostream& operator<<(std::ostream& os, const LogId& id) {
    os << "(index=" << id.index << ",term=" << id.term << ')';
    return os;
}

}  // namespace mraft

#endif  // BRAFT_LOG_ENTRY_H

#endif /* E05C25F5_F414_49B9_B18A_B95665B23715 */
