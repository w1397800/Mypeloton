#pragma once


#include <butil/memory/ref_counted.h>
#include "election_proposer.h"
#include "election_acceptor.h"
#include "raft_node.h"

// check silence to avoid acceptor voting many times
#define CHECK_SILENCE()\
do {\
  if (_init_ts < 0) {\
    LOG(ERROR) << "INIT_TS is less than 0, ELECTION MODULE IS NOT INIT YET!";\
    return;\
  } else if(butil::monotonic_time_ms() < _init_ts + 0){\
    LOG(INFO) << "keep silence for safety, will not send resp";\
    }\
} while(0)


namespace mraft{
class Lease;
class RaftNode;

class ElectionModule{
    friend class ElectionProposer;
    friend class ElectionAcceptor;
public:
    ElectionModule(PeerId addr,int64_t priority,std::vector<mraft::PeerId> peer_list,RaftNode* raft_node);

    ElectionModule() = default;
    ~ElectionModule() = default;

    void init(int64_t priority,std::vector<mraft::PeerId> peer_list);

    void start();

    void shutdown();

    void handle_prepare_request(const ElectionPrepareRequest* request);

    void handle_prepare_request_by_proposer(const ElectionPrepareRequest* request,
                                            bool *need_register_prepare_task);

    void handle_prepare_request_by_acceptor(const ElectionPrepareRequest* request);

    void handle_prepare_response(const ElectionPrepareResponse* resp);

    void handle_accept_request(const ElectionAcceptRequest* request);

    void handle_accept_response(const ElectionAcceptResponse* resp);

    void handle_change_leader(const ElectionChangeLeaderRequest* request);

    void inform_on_node_become_leader();

    void inform_node_step_down();

    void apply_config_change(std::vector<mraft::PeerId>& new_member_list
                             ,int64_t ballot_number,int64_t log_index);

    void reschedule_or_register_prepare_task(int64_t ms);

    void register_renew_lease_task(int64_t ms = RENEW_LEASE_INTERVAL);

    void time_window_run();

    void start_time_window(int64_t timeout);

    void close_time_window();



    std::unique_ptr<ElectionProposer> _proposer;
    std::unique_ptr<ElectionAcceptor> _acceptor;
    RaftNode *_raft_node;
    butil::Mutex _mutex;

    struct PrepareTimer : public RepeatedTimerTask {
        friend class ElectionModule;

        PrepareTimer():_election_module(NULL){}

    public:
        int init(ElectionModule* election_module,int timeout_ms){
            LOG(INFO) << "initialize prepare timer";
            if((RepeatedTimerTask::init(timeout_ms) != 0)){
                return -1;
            }
            _election_module = election_module;
            return 0;
        }

    protected:
        void run(){
            std::unique_lock<butil::Mutex> lck(_election_module->_mutex);
            if (_election_module->_proposer->check_leader()) {
                LOG(ERROR) << "leader is not allowed to  do prepare in timer task";
            } else {
                if (_election_module->_proposer->get_role() == NodeRole::LEADER) {
                    _election_module->_proposer->set_role(NodeRole::FOLLOWER);
                    _election_module->_proposer->get_node()->inform_node_step_down();
                }
                _election_module->_proposer->prepare(_election_module->_proposer->get_role());
            }
        }

        void on_destroy(){}

        ElectionModule* _election_module;
    };

    struct LeaderRenewTimer : public RepeatedTimerTask {
        friend class ElectionModule;
        LeaderRenewTimer():_election_module(NULL){}

    public:
        int init(ElectionModule* election_module,int timeout_ms){
            if((RepeatedTimerTask::init(timeout_ms) != 0)){
                return -1;
            }
            _election_module = election_module;
            return 0;
        }

    protected:
        void run(){
            std::unique_lock<butil::Mutex> lck(_election_module->_mutex);

            if (_election_module->_proposer->get_role() == NodeRole::FOLLOWER) {
                this->reset();
                return;
            }

            if (_election_module->_proposer->get_role() == NodeRole::LEADER
            && !_election_module->_proposer->check_leader()) {
                _election_module->_proposer->set_role(NodeRole::FOLLOWER);
                _election_module->_proposer->reset_prepare_and_accept_states();
                _election_module->_proposer->reset_lease_and_epoch();
                _election_module->_proposer->get_node()->inform_node_step_down();
                LOG(INFO) << "leader lease expired => step down";
                this->reset();
                return;
            }

            if (_election_module->_proposer->get_prepare_success_ballot()
            == _election_module->_proposer->get_ballot_number()) {
                _election_module->_proposer->propose(true);
            }
            this->reset();
        }

        void on_destroy(){}
        ElectionModule* _election_module;
    };

    struct TimeWindowTimer : RepeatedTimerTask {
        friend class ElectionAcceptor;
    public:
        TimeWindowTimer() : _election_module(NULL) {}

        int init(ElectionModule *election_module, int timeout_ms){
            if((RepeatedTimerTask::init(timeout_ms) != 0)){
                return -1;
            }
            _election_module = election_module;
            return 0;
        }

    protected:
        void run();

        void on_destroy(){}

        ElectionModule *_election_module;
    };

public:
    PrepareTimer _prepare_timer;
    LeaderRenewTimer _leader_renew_timer;
    TimeWindowTimer _time_window_timer;


private:
    mraft::PeerId _addr;
    int64_t _init_ts = -1; // election module init ts
};

}


