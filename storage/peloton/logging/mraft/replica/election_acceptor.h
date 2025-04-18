
#ifndef CONCENSUS_BENCHMARK_ELECTION_ACCEPTOR_H
#define CONCENSUS_BENCHMARK_ELECTION_ACCEPTOR_H
#include <vector>
#include "election_utils.h"
#include <butil/memory/ref_counted.h>
#include "repeat_timer_task.h"

namespace mraft {
    class ElectionModule;

    class ElectionAcceptor;

    class Lease;

    class ElectionAcceptor{
        friend class ElectionModule;

    public:
        ElectionAcceptor();

        ElectionAcceptor(mraft::PeerId addr, ElectionModule* node);

        void start();

        void stop();

        int64_t get_ballot_number() { return _ballot_number; }

        int64_t get_ballot_of_time_window() { return _ballot_of_time_window; }

        Lease get_lease() { return _lease; };

        void renew_highest_req(ElectionPrepareRequest req);

        bool check_time_window() { return _is_time_window_opened; }

        void set_time_window_flag(bool flag) { _is_time_window_opened = flag; }

        void set_ballot_of_time_window(int64_t ballot_of_tw) { _ballot_of_time_window = ballot_of_tw; }

        void set_highest_req_valid(bool flag) { _is_highest_req_valid = flag; }

        bool is_highest_req_valid() { return _is_highest_req_valid; }

        void set_last_time_window_open_ts(int64_t ts) { _last_time_window_open_ts = ts; }

        bool is_new_req_higher(ElectionPrepareRequest req);

        int64_t acceptor_get_calculated_priority();

        int64_t acceptor_get_origin_priority();

    private:
        int64_t _ballot_number;
        int64_t _ballot_of_time_window;
        Lease _lease;
        ElectionPrepareRequest _highest_priority_prepare_req;
        bool _is_time_window_opened = false;
        bool _is_highest_req_valid = false;
        mraft::PeerId _self_addr;
        ElectionModule* _node;
        int64_t _last_time_window_open_ts;


    };

}

#endif //CONCENSUS_BENCHMARK_ELECTION_ACCEPTOR_H
