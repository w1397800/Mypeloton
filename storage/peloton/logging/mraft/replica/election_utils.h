#ifndef _ONCE_
#define _ONCE_

#include <string>
#include "rpc/raft.pb.h"
#include "common/configuration.h"

#define ELECT_MAX_COST_T 10000L // 10000ms
#define TIME_WINDOW_SPAN 2000L
#define MAX_TXT 1000L //最大单程延迟 1000ms
#define PREPARE_TIMER_INIT_T 3000L
#define RENEW_LEASE_TIMER_INIT_T 500L
#define TIME_WINDOW_INIT_T 1000L
#define CACHE_EXPIRATION_TIME 5000 // cached highest accept_resp will expire in 5s

namespace mraft {

    constexpr int64_t INVALID_VALUE = 0;// 所有int64_t变量的初始默认无效值

    class Lease {
    public:
        Lease() : _lease_end_ts(INVALID_VALUE), _ballot_number(INVALID_VALUE) {}

        Lease(const Lease &rhs);

        Lease &operator=(const Lease &rhs);

        bool is_expired() const;

        void reset();

        bool is_empty() const;

        const mraft::PeerId &get_owner() const;

        int64_t get_ballot_number() const;

        int64_t get_lease_end_ts() const;

        void get_owner_and_ballot(mraft::PeerId &owner, int64_t &ballot) const;

        void update_from(const ElectionAcceptRequest *accept_req);


    private:
        mraft::PeerId _owner; // 发出Lease的Proposer
        int64_t _lease_end_ts; // 即Lease End
        int64_t _ballot_number; // 即paxos中的proposal id
    };

    enum NodeRole {
        INVALID_ROLE = 0,

        // Election Leader, supports strong consistent reading and writing, and supports member changes
        LEADER = 1,

        FOLLOWER = 2,

    };

    enum ChangeMemberListType{
        ADD_NODE = 0,
        REMOVE_NODE =1,
    };



}
#endif
