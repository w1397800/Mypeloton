#ifndef CONCENSUS_BENCHMARK_ELECTION_PROPOSER_H
#define CONCENSUS_BENCHMARK_ELECTION_PROPOSER_H

#include <vector>
#include "repeat_timer_task.h"
#include <butil/memory/ref_counted.h>
#include "election_utils.h"

#define RENEW_LEASE_INTERVAL 500 //500ms

/* Pause instruction to prevent excess processor bus usage */
#if defined(__x86_64__) || defined(__x86__)
#define CPU_RELAX() asm volatile("pause\n" : : : "memory")
#else // SG taken from compiler.hh;; some web says yield:::memory
#define CPU_RELAX() asm volatile("" : : : "memory") // equivalent to "rep; nop"
#endif


namespace mraft {
    class ElectionModule;

    class ElectionProposer;

    class ElectionProposer{
        friend class ElectionModule;

    public:

        ElectionProposer(NodeRole role, ElectionModule *node, mraft::PeerId addr, int64_t priority,
                         std::vector<mraft::PeerId> peer_list,int64_t ballot_number = INVALID_VALUE,
                      int64_t log_index = INVALID_VALUE);

        ElectionProposer(std::vector<mraft::PeerId> peer_list);

        void init(std::vector<mraft::PeerId> peer_list);

        void set_member_list(std::vector<mraft::PeerId>& new_member_list, int64_t ballot_number,int64_t log_index);

        void start();

        void stop();

        void prepare(NodeRole role);

        void broadcast_prepare();

        void propose(bool is_renew_lease = false);

        void change_leader_to(mraft::PeerId dest_addr);

        void inner_change_leader_to(mraft::PeerId dest_addr);

        NodeRole get_role() { return _role; }

        void set_role(NodeRole role) { _role = role; }

        int64_t get_ballot_number() { return _ballot_number; }

        int64_t get_prepare_success_ballot() { return _prepare_success_ballot; }

        int64_t get_origin_priority() { return _origin_priority; }

        int64_t get_member_list_version_ballot(){ return _member_list_version_ballot;}

        int64_t get_member_list_version_index(){ return _member_list_version_index;}

        void change_member_list(mraft::PeerId delta_addr,ChangeMemberListType change_type);

        // 会同时推大raft node的term
        void advance_ballot_number_and_reset_states(int64_t ballot_number);

        void reset_prepare_and_accept_states();

        std::vector<mraft::PeerId> get_member_list() { return _member_list; }

        void renew_last_prepare_time(int64_t new_ts);

        bool check_leader(int64_t *epoch = nullptr);

        bool record_prepare_ok(mraft::PeerId sender);

        bool record_accept_ok(mraft::PeerId sender, int64_t lease_end);

        void reset_lease_and_epoch();

        void inform_raft_node_new_term(int64_t new_term);

        int64_t proposer_get_calculated_priority();

        int64_t get_majority_promised_not_vote_ts();

        ElectionModule *get_node() { return _node; }

        void check_accept_resp_expired();

        bool check_new_resp_if_higher_than_leader(ElectionAcceptResponse resp);

        bool check_new_resp_if_higher_than_cache(ElectionAcceptResponse resp);

        bool check_if_member_list_version_is_correct(int64_t ballot_number,int64_t log_index);

    private:

        void register_renew_lease_task(int64_t ms);

    private:
        NodeRole _role;
        int64_t _ballot_number;
        std::vector<mraft::PeerId> _member_list;
        std::map<mraft::PeerId, bool> _member_list_state;
        std::map<mraft::PeerId, int64_t> _member_list_lease_end;

        int64_t _member_list_version_ballot;
        int64_t _member_list_version_index;

        int64_t _prepare_success_ballot;
        mraft::PeerId _leader_addr;
        mraft::PeerId _self_addr;

        int64_t _last_do_prepare_ts;
        ElectionModule *_node;
        int64_t _origin_priority;

        int64_t _switch_source_leader_ballot;
        mraft::PeerId _switch_source_leader_addr;

        struct LeaderLeaseAndEpoch {
            LeaderLeaseAndEpoch()
                    : _lease(-1), _epoch(-1), _seq(-1) {} // invalid value = -1
            void set_lease_and_epoch_if_lease_expired_or_just_set_lease(
                    const int64_t lease, const int64_t epoch) {
                int64_t old_lease;
                ++_seq;
                __sync_synchronize();
                old_lease = _lease;
                if (butil::monotonic_time_ms() < old_lease) { // lease没过期的时候，试图只set lease，不推进epoch
                    _lease = lease;
                    if (butil::monotonic_time_ms() >= old_lease) { // double check,
                        // 此时lease过期了，但该状态不会被外界读取到，必须要推进epoch
                        _epoch = epoch;
                    }
                } else { // lease过期的时候，两个值一起修改
                    _lease = lease;
                    _epoch = epoch;
                }
                __sync_synchronize();
                ++_seq;
            }

            void get(int64_t &lease, int64_t &epoch) const {
                int64_t seq = -1;
                do {
                    if (seq != -1) {
                        ({
                            (void) (0);
                            CPU_RELAX();
                        });
                    }
                    seq = _seq;
                    __sync_synchronize();
                    if ((seq & 1) != 0) {
                        lease = _lease;
                        epoch = _epoch;
                    }
                    __sync_synchronize();
                } while ((seq != _seq) || ((seq & 1) == 0));
            }

            bool is_valid() const {
                int64_t lease = -1;
                int64_t epoch = -1;
                get(lease, epoch);
                return lease != -1 && epoch != -1;
            }

            void reset() {
                ++_seq;
                __sync_synchronize();
                _lease = -1;
                _epoch = -1;
                __sync_synchronize();
                ++_seq;
            }

            int64_t _lease;
            int64_t _epoch;
            mutable int64_t _seq; // 与memory barrier配合，实现sequence lock，避免原子操作的开销
        };

        struct HighestPriorityMsgCache {
            HighestPriorityMsgCache() : _cached_ts(INVALID_VALUE),_is_msg_valid(false) {}
            void set(const ElectionAcceptResponse &rhs) {
                _cached_msg = rhs;
                _cached_ts = butil::monotonic_time_ms();
                _is_msg_valid = true;
            }
            void check_expired() {
                if (_cached_ts != INVALID_VALUE && (butil::monotonic_time_ms() - _cached_ts > CACHE_EXPIRATION_TIME)) {
                    reset();
                }
            }
            void reset() {
                _cached_ts = INVALID_VALUE;
                _is_msg_valid = false;
            }
            ElectionAcceptResponse
                _cached_msg;// 在Leader上缓存优先级最高的副本的响应，在第二次收到该响应时触发切主，避免多次切主
            int64_t
                _cached_ts;// 缓存消息的时间点，缓存的消息具有时效性，需要在缓存失效前完成与优先级最高副本的第二次交互，超时清空
            bool _is_msg_valid;
        } _highest_priority_cache;
        LeaderLeaseAndEpoch _leader_lease_and_epoch;


    };

}


#endif //CONCENSUS_BENCHMARK_ELECTION_PROPOSER_H
