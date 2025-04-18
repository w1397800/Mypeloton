#include "election_proposer.h"
#include <brpc/errno.pb.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include "rpc/raft.pb.h"

namespace mraft {

    ElectionProposer::ElectionProposer(std::vector<mraft::PeerId> peer_list)
            : _role(NodeRole::FOLLOWER) {
        init(peer_list);
    }

    ElectionProposer::ElectionProposer(NodeRole role, ElectionModule *node, mraft::PeerId addr, int64_t priority,
                                       std::vector<mraft::PeerId> peer_list,int64_t ballot_number,int64_t log_index)
            : _role(role), _ballot_number(INVALID_VALUE), _prepare_success_ballot(INVALID_VALUE), _self_addr(addr),
              _last_do_prepare_ts(INVALID_VALUE),
              _node(node), _origin_priority(priority),_member_list_version_ballot(ballot_number),
            _member_list_version_index(log_index){
        init(peer_list);
    }


    void ElectionProposer::init(std::vector<mraft::PeerId> peer_list) {
        LOG(INFO) << "initialize proposer";
        for (mraft::PeerId peerId: peer_list) {
            _member_list.emplace_back(peerId);
            _member_list_state.insert(std::pair<mraft::PeerId, bool>(peerId, false));
            _member_list_lease_end.insert(std::pair<mraft::PeerId, int64_t>(peerId, INVALID_VALUE));
        }
    }

    void ElectionProposer::set_member_list(std::vector<mraft::PeerId>& new_member_list,int64_t ballot_number,
                                           int64_t log_index) {
        if(ballot_number >_ballot_number ||
            (ballot_number == _ballot_number && log_index > _member_list_version_index)){
            ChangeMemberListType changeType;
            mraft::PeerId delta_addr;
            if(new_member_list.size() > _member_list.size()){
                changeType = ADD_NODE;
                for(auto iter : new_member_list){
                    if(std::find(_member_list.begin(), _member_list.end(), iter)
                        != _member_list.end()){
                        continue ;
                    }else{
                        delta_addr = iter;
                        break;
                    }
                }
            }else{
                changeType = REMOVE_NODE;
                for(auto iter : _member_list){
                    if(std::find(new_member_list.begin(), new_member_list.end(), iter)
                        != new_member_list.end()){
                        continue ;
                    }else{
                        delta_addr = iter;
                        break;
                    }
                }
            }

            if(delta_addr.is_empty()){
                LOG(ERROR) << "delta addr is empty";
                return ;
            }else{
                change_member_list(delta_addr,changeType);
            }
            _member_list_version_ballot = ballot_number;
            _member_list_version_index = log_index;
            LOG(INFO)<< "set member list success, ballot number is " << ballot_number<< " log index is " << log_index;
        }else{
            LOG(ERROR) << "old member list version, refuse to set new member list";
        }
    }

    void ElectionProposer::change_member_list(mraft::PeerId delta_addr, mraft::ChangeMemberListType change_type) {
        if(change_type == ADD_NODE){
            _member_list.emplace_back(delta_addr);
            _member_list_state.insert(std::pair<mraft::PeerId, bool>(delta_addr, false));
            _member_list_lease_end.insert(std::pair<mraft::PeerId, int64_t>(delta_addr, INVALID_VALUE));
        }else if(change_type == REMOVE_NODE){
            std::remove(_member_list.begin(), _member_list.end(), delta_addr);
            _member_list_state.erase(delta_addr);
            _member_list_lease_end.erase(delta_addr);
        }
    }

    bool ElectionProposer::check_if_member_list_version_is_correct(int64_t ballot_number, int64_t log_index) {
        if(ballot_number < _member_list_version_ballot
            || (ballot_number == _member_list_version_ballot && log_index < _member_list_version_index)){
            return false;
        }
        return true;
    }

    void ElectionProposer::change_leader_to(mraft::PeerId dest_addr) {
        mraft::PeerId redirect_addr = dest_addr;
        if (std::find(_member_list.begin(), _member_list.end(), dest_addr) == _member_list.end()) {
            redirect_addr = _self_addr;
            LOG(INFO) << "change leader dest addr invalid";
        }
        if (!check_leader()) {
            LOG(INFO) << "follower can not change leader";
            return;
        }
        inner_change_leader_to(redirect_addr);
    }

    void ElectionProposer::inner_change_leader_to(mraft::PeerId dest_addr) {
        LOG(INFO) << "inner change leader to addr:" << dest_addr.to_string();
        int64_t switch_source_leader_ballot = _ballot_number;
        // old leader need to advance ballot to reject all accept_res on the way
        advance_ballot_number_and_reset_states(_ballot_number + 1);
        _leader_lease_and_epoch.reset();

        if (_role == NodeRole::LEADER && !check_leader()) {
            _role = NodeRole::FOLLOWER;
            reset_prepare_and_accept_states();
            _leader_lease_and_epoch.reset();
            LOG(INFO) << "change leader, old leader revoke";
        }

        //fixme: why changing leader needs success of revoking old leader?

        std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
        std::unique_ptr<ElectionChangeLeaderRequest> req(new ElectionChangeLeaderRequest);
        std::unique_ptr<EmptyResponse> resp(new EmptyResponse);

        req->set_sender(_self_addr.to_string());
        req->set_receiver(dest_addr.to_string());
        req->set_ballot_number(_ballot_number);
        req->set_switch_source_leader_ballot_(switch_source_leader_ballot);


        brpc::ChannelOptions options;
        options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
        options.max_retry = 0;
        options.connect_timeout_ms = 200; // 200ms
        brpc::Channel channel;

        auto pos = dest_addr.to_string().find_last_of(':');
        std::string addr = dest_addr.to_string().substr(0, pos);
        if (0 != channel.Init(addr.c_str(), &options)) {
            LOG(WARNING) << "node channel init failed, addr " << dest_addr.to_string();
        }
        LogService_Stub stub(&channel);
        LOG(INFO) << "send to " << dest_addr.to_string() << " from " << _self_addr.to_string();
        stub.change_leader(cntl.release(), req.release(), resp.release(), brpc::DoNothing());


    }

    void ElectionProposer::advance_ballot_number_and_reset_states(int64_t new_ballot_number) {
        assert(new_ballot_number >= _ballot_number);
        LOG(INFO) << " bigger ballot number, ballot number = " << new_ballot_number;
        _ballot_number = new_ballot_number;
        for (auto iter = _member_list_state.begin(); iter != _member_list_state.end(); iter++) {
            iter->second = false;
        }
        _switch_source_leader_ballot = INVALID_VALUE;
        _switch_source_leader_addr.reset();

        inform_raft_node_new_term(new_ballot_number); // advance raft node's term
    }




    void ElectionProposer::reset_prepare_and_accept_states() {
        LOG(INFO) << "reset_prepare_and_accept_states";
        for (auto iter = _member_list_state.begin(); iter != _member_list_state.end(); iter++) {
            iter->second = false;
        }
        for (auto iter = _member_list_lease_end.begin(); iter != _member_list_lease_end.end(); iter++) {
            iter->second = 0;
        }
    }

    void ElectionProposer::renew_last_prepare_time(int64_t new_ts) {
        _last_do_prepare_ts = new_ts;
    }

    bool ElectionProposer::check_leader(int64_t *epoch) {
        int ret = false;
        int64_t current_ts = butil::monotonic_time_ms();
        int64_t lease;
        int64_t exposed_epoch;
        _leader_lease_and_epoch.get(lease, exposed_epoch);
        if (current_ts < lease) {
            ret = true;
            if (epoch != nullptr) {
                *epoch = exposed_epoch;
            }
        }
        LOG(INFO) << "check leader res = " << ret << " cur_ts is " << current_ts << " lease is " << lease;
        return ret;
    }

    void ElectionProposer::start() {
    }

    void ElectionProposer::stop() {
    }

    void ElectionProposer::prepare(NodeRole role) {
        LOG(INFO) << "do prepare";
        int64_t cur_ts = butil::monotonic_time_ms();

        if (_member_list.empty()) {
            LOG(INFO) << "member list is empty";
            return;
        }

        //todo :: need to judge if self is in member list?

        if (role == NodeRole::FOLLOWER && cur_ts - _last_do_prepare_ts < 5000) { // 一呼百应间隔 ELECT_MAX_COST/2
            LOG(INFO) << "prepare action just happened, wait next time"
                      << " cur_ts = " << cur_ts << " last_do_prepare_ts = " << _last_do_prepare_ts;
            return;
        }
        _last_do_prepare_ts = cur_ts;

        if (role == NodeRole::FOLLOWER) {
            advance_ballot_number_and_reset_states(
                    _ballot_number + 1); // Follower prepare需要推大自己的ballot number再进行，Leader prepare不推ballot number
        }

        //广播
        broadcast_prepare();

    }

    void ElectionProposer::broadcast_prepare() {
        // broadcast
        LOG(INFO) << "broadcast prepare";
        LOG(INFO) << "member list size is " << _member_list.size();
        for (std::vector<mraft::PeerId>::const_iterator iter = _member_list.begin();
             iter != _member_list.end(); ++iter) {
            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionPrepareRequest> req(new ElectionPrepareRequest);
            std::unique_ptr<EmptyResponse> resp(new EmptyResponse);

            req->set_sender(_self_addr.to_string());
            req->set_receiver(iter->to_string());
            req->set_ballot_number(_ballot_number);
            req->set_role(_role);
            req->set_origin_priority(_origin_priority);
            req->set_calculated_priority(proposer_get_calculated_priority());
            req->set_member_list_version_index(_member_list_version_index);
            req->set_member_list_version_ballot(_member_list_version_ballot);


            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            auto pos = iter->to_string().find_last_of(':');
            std::string addr = iter->to_string().substr(0, pos);
            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << iter->to_string();
                continue;
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "send to " << iter->to_string() << " from " << _self_addr.to_string();
            stub.prepare_request(cntl.release(), req.release(), resp.release(), brpc::DoNothing());
        }
    }

    bool ElectionProposer::record_prepare_ok(mraft::PeerId sender) {
        // fixme: need time guard?
        auto idx = std::find(_member_list.begin(), _member_list.end(), sender);
        if (idx == _member_list.end()) {
            LOG(INFO) << "peer id: " << sender << " does not exist";
            return false;
        } else {
            if (!_member_list_state[sender]) {
                _member_list_state[sender] = true;
            }

            int64_t ok_count = 0;
            for (auto iter: _member_list_state) {
                if (iter.second) ok_count++;
            }
            if (ok_count > _member_list.size() / 2 + 1) {
                LOG(INFO) << "OK COUNT OVER MAJORITY ERROR";
                return false;
            } else {
                if (ok_count == _member_list.size() / 2 + 1) {
                    LOG(INFO) << "reach majority";
                    return true;
                } else {
                    LOG(INFO) << "below majority";
                }
            }
        }
        return false;
    }

    bool ElectionProposer::record_accept_ok(mraft::PeerId sender, int64_t lease_end) {

        LOG(INFO) << "record ok, sender is " << sender.to_string() << " lease end is " << lease_end;
        if(_member_list_lease_end.count(sender) == 0){
            LOG(ERROR) << "not find server" << sender.to_string() << " in member list";
        }else{
            _member_list_lease_end[sender] = std::max(_member_list_lease_end[sender], lease_end);
        }

        int64_t ok_count = 0;
        for (auto iter: _member_list_lease_end) {
            if (iter.second != 0) ok_count++;
        }

        return ok_count >= _member_list.size() / 2 + 1;

    }

    int64_t ElectionProposer::get_majority_promised_not_vote_ts() {
        std::vector<int64_t> temp_ts;
        for (auto iter: _member_list_lease_end) {
            temp_ts.push_back(iter.second);
        }
        std::sort(temp_ts.begin(), temp_ts.end());
        return temp_ts.at(temp_ts.size() / 2);
    }

    void ElectionProposer::propose(bool is_renew_lease) {

        LOG(INFO) << "do propose";
        int64_t current_ts = butil::monotonic_time_ms();

        int64_t new_lease_interval = 4 * MAX_TXT; // 4* MAX_TXT

        // broadcast
        for (std::vector<mraft::PeerId>::const_iterator iter = _member_list.begin();
             iter != _member_list.end(); ++iter) {

            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionAcceptRequest> req(new ElectionAcceptRequest);
            std::unique_ptr<EmptyResponse> resp(new EmptyResponse);

            req->set_sender(_self_addr.to_string());
            req->set_receiver(iter->to_string());
            req->set_ballot_number(_prepare_success_ballot);
            req->set_lease_started_ts_on_proposer(current_ts);
            req->set_lease_interval(new_lease_interval);
            req->set_is_renew_lease(is_renew_lease);
            req->set_member_list_version_ballot(_member_list_version_ballot);
            req->set_member_list_version_index(_member_list_version_index);
            // fixme : proposer 是否需要record lease interval


            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            auto pos = iter->to_string().find_last_of(':');
            std::string addr = iter->to_string().substr(0, pos);
            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << iter->to_string();
                continue;
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "accept req send to " << iter->to_string() << " from " << _self_addr.to_string();
            stub.accept_request(cntl.release(), req.release(), resp.release(), brpc::DoNothing());
        }

    }

    void ElectionProposer::reset_lease_and_epoch() {
        _leader_lease_and_epoch.reset();
    }

    bool ElectionProposer::check_new_resp_if_higher_than_leader(ElectionAcceptResponse resp) {
        if(resp.calculated_priority() > proposer_get_calculated_priority()
            ||(resp.calculated_priority() == proposer_get_calculated_priority()
                && resp.origin_priority() > _origin_priority)){
            return true;
        }
        return false;
    }

    bool ElectionProposer::check_new_resp_if_higher_than_cache(mraft::ElectionAcceptResponse resp) {
        _highest_priority_cache.check_expired();
        if(!_highest_priority_cache._is_msg_valid){
            return true;
        }else{
            if(resp.calculated_priority() > _highest_priority_cache._cached_msg.calculated_priority()
                || (resp.calculated_priority() == _highest_priority_cache._cached_msg.calculated_priority()
                    && resp.origin_priority() > _highest_priority_cache._cached_msg.origin_priority())){
                return true;
            }
        }
        return false;
    }





}