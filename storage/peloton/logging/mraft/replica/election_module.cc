
#include <google/protobuf/stubs/callback.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include "election_module.h"

namespace mraft {

    ElectionModule::ElectionModule(PeerId addr, int64_t priority, std::vector<mraft::PeerId> peer_list,
                                   RaftNode* raft_node) {
        _addr = addr;
        _raft_node = raft_node;
        init(priority, peer_list);
        _init_ts = butil::monotonic_time_ms();
    }

    void ElectionModule::init(int64_t priority, std::vector<mraft::PeerId> peer_list) {
        LOG(INFO) << "election module initialize";
        _proposer.reset(new ElectionProposer(NodeRole::FOLLOWER, this, _addr, priority, peer_list));
        _acceptor.reset(new ElectionAcceptor(_addr,this));

        LOG(INFO) << "initialize timers";
        _prepare_timer.init(this, PREPARE_TIMER_INIT_T); // start with 3s
        _leader_renew_timer.init(this, RENEW_LEASE_TIMER_INIT_T); //start with 500ms
        _time_window_timer.init(this, TIME_WINDOW_INIT_T); // 后面启动window的时候会改 现在是1000ms不影响
        LOG(INFO) << "finish initialize timers";
    }

    void ElectionModule::start() {
        LOG(INFO) << "start election module";
        LOG(INFO) << "start timers";
        _prepare_timer.start();
        _leader_renew_timer.start();
    }

    void ElectionModule::shutdown() {
        LOG(INFO) << "do election module shutdown";
        std::unique_lock<butil::Mutex> lck(_mutex);
        _prepare_timer.destroy();
        _leader_renew_timer.destroy();
        _time_window_timer.destroy();
    }

    void ElectionModule::handle_prepare_request(const ElectionPrepareRequest *request) {

        bool flag = false;
        {
            std::unique_lock<butil::Mutex> lck(_mutex);
            if (request->sender() != _addr) handle_prepare_request_by_proposer(request, &flag);

            handle_prepare_request_by_acceptor(request);
        }

        if (flag) {
            //重置prepare timer
            reschedule_or_register_prepare_task(ELECT_MAX_COST_T);
        }
    }

    void ElectionModule::handle_prepare_request_by_proposer(const ElectionPrepareRequest *request,
                                                            bool *need_register_prepare_task) {

        LOG(INFO) << "handle_prepare_request_by_proposer from " << request->sender();

        if (request->ballot_number() <= _proposer->get_ballot_number()) {
            if (request->ballot_number() < _proposer->get_ballot_number()) {
                std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
                std::unique_ptr<ElectionPrepareResponse> prepare_resp(new ElectionPrepareResponse);
                std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);

                prepare_resp->set_receiver(request->sender());
                prepare_resp->set_is_send_by_proposer(true);
                prepare_resp->set_accept(false);
                prepare_resp->set_ballot_number(_proposer->_ballot_number);
                prepare_resp->set_member_list_version_ballot(_proposer->_member_list_version_ballot);
                prepare_resp->set_member_list_version_index(_proposer->_member_list_version_index);

                brpc::ChannelOptions options;
                options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
                options.max_retry = 0;
                options.connect_timeout_ms = 200; // 200ms
                brpc::Channel channel;

                int pos = request->sender().find_last_of(':');
                std::string addr = request->sender().substr(0, pos);

                if (0 != channel.Init(addr.c_str(), &options)) {
                    LOG(WARNING) << "node channel init failed, addr " << _addr.to_string();
                }
                LogService_Stub stub(&channel);
                LOG(INFO) << "prepare response send to " << request->sender()
                          << " from proposer:" << _proposer->_self_addr.to_string();
                stub.prepare_response(cntl.release(), prepare_resp.release(), emtpy_resp.release(), brpc::DoNothing());
            } else {
                // == 本轮次消息不用处理
                return;
            }
        } else {
            // todo : need check self in member list?

            //leader prepare也在这里
            // 对于新的消息，推大本机选举轮次（只有 ballot_number 更大的消息会走到这里）
            _proposer->advance_ballot_number_and_reset_states(request->ballot_number());

            // 1. 忽略leader prepare消息，不触发一呼百应，相当于只是推大了选举轮次
            if (request->role() == NodeRole::LEADER) {

                return;
            }

            // todo: 刷新优先级

            // 尝试一呼百应
            _proposer->broadcast_prepare();
            _proposer->renew_last_prepare_time(butil::monotonic_time_ms());
            *need_register_prepare_task = true;

        }
    }

    void ElectionModule::handle_prepare_request_by_acceptor(const ElectionPrepareRequest *request) {
        // 启动后的要维持一段静默时间，acceptor假装看不到任何消息，以维护lease的正确语义
        CHECK_SILENCE();
        LOG(INFO) << "handle_prepare_request_by_acceptor from " << request->sender();

        if(!_proposer->check_if_member_list_version_is_correct(request->member_list_version_ballot(),
                                                               request->member_list_version_index())){
            LOG(ERROR) << "member list version has some problem!";
            return;
        }

        // 对ballot number较小的req回复reject
        if (request->ballot_number() < _acceptor->get_ballot_number()) {
            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionPrepareResponse> prepare_resp(new ElectionPrepareResponse);
            std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);
            prepare_resp->set_sender(_acceptor->_self_addr.to_string());
            prepare_resp->set_receiver(request->sender());
            prepare_resp->set_is_send_by_proposer(false);
            prepare_resp->set_accept(false);
            prepare_resp->set_member_list_version_ballot(_proposer->_member_list_version_ballot);
            prepare_resp->set_member_list_version_index(_proposer->_member_list_version_index);

            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            int pos = request->sender().find_last_of(':');
            std::string addr = request->sender().substr(0, pos);

            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << _addr.to_string();
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "prepare reject response send to " << request->sender()
                      << " from acceptor:" << _addr.to_string();
            stub.prepare_response(cntl.release(), prepare_resp.release(), emtpy_resp.release(), brpc::DoNothing());

            return;
        }

        // leader prepare 无需比较优先级 直接返回结果
        if (request->role() == NodeRole::LEADER) {
            if (request->ballot_number() > _acceptor->get_ballot_number()) {
                // 关闭时间窗口并回复accept

                LOG(INFO) << "leader prepare to close time window";
                close_time_window();

                _acceptor->_ballot_number = request->ballot_number();
                _acceptor->_ballot_of_time_window = _acceptor->_ballot_number;

                //发送accept消息
                std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
                std::unique_ptr<ElectionPrepareResponse> prepare_resp(new ElectionPrepareResponse);
                std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);

                prepare_resp->set_receiver(request->sender());
                prepare_resp->set_is_send_by_proposer(false);
                prepare_resp->set_accept(true);
                prepare_resp->set_ballot_number(_acceptor->_ballot_number);

                prepare_resp->set_lease_ballot_number(_acceptor->_lease.get_ballot_number());
                prepare_resp->set_lease_owner(_acceptor->_lease.get_owner().to_string());
                prepare_resp->set_lease_end_ts(_acceptor->_lease.get_lease_end_ts());
                prepare_resp->set_is_lease_valid(true);

                brpc::ChannelOptions options;
                options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
                options.max_retry = 0;
                options.connect_timeout_ms = 200; // 200ms
                brpc::Channel channel;

                int pos = request->sender().find_last_of(':');
                std::string addr = request->sender().substr(0, pos);

                if (0 != channel.Init(addr.c_str(), &options)) {
                    LOG(WARNING) << "node channel init failed, addr " << _addr.to_string();
                }
                LogService_Stub stub(&channel);
                LOG(INFO) << "answer leader's prepare response send to "
                          << request->sender() << " from acceptor:" << request->sender();
                stub.prepare_response(cntl.release(), prepare_resp.release(), emtpy_resp.release(), brpc::DoNothing());


            }
        } else {
            // 遇到比时间窗口更大的ballot number 需要关掉当前时间窗口
            if (_acceptor->_is_time_window_opened && request->ballot_number() > _acceptor->_ballot_of_time_window) {
                //关闭当前时间窗口
                LOG(INFO) << "bigger ballot to close time window";
                close_time_window();

            }

            // 时间窗口未开启 则开启
            if (!_acceptor->_is_time_window_opened && request->ballot_number() > _acceptor->_ballot_of_time_window) {

                _acceptor->_ballot_of_time_window = request->ballot_number();
                _acceptor->_is_highest_req_valid = false;

                int64_t time_window_span;   // time window span
                if (!_acceptor->_lease.is_expired()) {
                    time_window_span =
                            std::max(_acceptor->_lease.get_lease_end_ts() - butil::monotonic_time_ms(), 1000L);
                } else {
                    time_window_span = TIME_WINDOW_SPAN;
                }
                LOG(INFO) << "start time window because req from " << request->sender()
                          << " ballot:" << request->ballot_number();
                start_time_window(time_window_span);

            }

            if (_acceptor->_is_time_window_opened) {
                if (request->ballot_number() != _acceptor->_ballot_of_time_window) {
                    LOG(INFO) << "req ballot is " << request->ballot_number()
                              << " mismatch acceptor's window ballot " << _acceptor->_ballot_of_time_window;
                    return; // 忽略
                } else {
                    if (_acceptor->is_new_req_higher(*request)) {
                        LOG(INFO) << "get a higher prepare req from " << request->sender();
                        _acceptor->_highest_priority_prepare_req = *request;
                        _acceptor->_is_highest_req_valid = true;
                    }
                }

                //todo : if need to add judgement of member version and to judge if prepare request is valid
            }

        }
    }

    void ElectionModule::handle_prepare_response(const ElectionPrepareResponse *resp) {
        std::unique_lock<butil::Mutex> lck(_mutex);
        LOG(INFO) << "handle prepare response from " << resp->sender();
        //检查消息有效性
        if (resp->ballot_number() < _proposer->_ballot_number) {
            return;
        }
        //ballot number更大的时候更新ballot number，若自己仍然是leader，触发leader prepare
        // fixme: ballot number更大，proposer还是leader的情况何时发生？
        if (resp->ballot_number() > _proposer->_ballot_number) {
            assert(!resp->accept());
            _proposer->advance_ballot_number_and_reset_states(resp->ballot_number());

            if (_proposer->check_leader()) {
                // 在check leader后可能卡住，做leader prepare时就已经不再是leader了
                // 但是没有关系，正确性是由prepare阶段保证的，check_leader的意义在于尽量避免无谓的leader prepare流程

                _proposer->advance_ballot_number_and_reset_states(_proposer->_ballot_number + 1);//retry leader prepare
                _proposer->prepare(NodeRole::LEADER);
            }
            return;
        }

        // 检查是否因其他原因rejected
        if (!resp->accept()) {
            LOG(INFO) << "receive reject msg";
            return;
        }

        // 检查是否具备进入accept阶段的条件 下列条件满满足一个就可以进入
        if (!((!resp->is_lease_valid()) || //1. 对方目前没有lease
              resp->lease_owner() == _proposer->_self_addr.to_string() //2. lease是本proposer发出的
                || (resp->lease_owner() == _proposer->_switch_source_leader_addr &&
                  resp->lease_ballot_number() == _proposer->_switch_source_leader_ballot ) //3. 旧主的ballot内
                ) ){
            return;
        }

        //记录应答

        LOG(INFO) << "record prepare from " << resp->sender();
        bool reach_majority = _proposer->record_prepare_ok(resp->sender());

        if (!reach_majority) {
            return;
        }

        // reach majority
        _proposer->_prepare_success_ballot = std::max(_proposer->_ballot_number, _proposer->_prepare_success_ballot);
        _proposer->propose();


    }

    void ElectionModule::handle_accept_request(const ElectionAcceptRequest *request) {

        CHECK_SILENCE();

        LOG(INFO) << "handle accept request from " << request->sender();
        int64_t us_to_expired = 0;

        std::unique_lock<butil::Mutex> lck(_mutex);
        if (request->ballot_number() > _proposer->_ballot_number) {
            // 推大 proposer ballot number
            LOG(INFO) << "receive bigger accept request, advance proposer's ballot number";
            _proposer->advance_ballot_number_and_reset_states(request->ballot_number());
        }

        if (request->ballot_number() < _acceptor->get_ballot_number()) {

            //reject
            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionAcceptResponse> accept_reject_resp(new ElectionAcceptResponse);
            std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);

            accept_reject_resp->set_receiver(request->sender());
            accept_reject_resp->set_sender(_acceptor->_self_addr.to_string());
            accept_reject_resp->set_ballot_number(request->ballot_number());
            accept_reject_resp->set_accepted(false);

            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            int pos = request->sender().find_last_of(':');
            std::string addr = request->sender().substr(0, pos);

            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << _addr.to_string();
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "accept reject response send to " << request->sender()
                      << " from acceptor:" << request->sender();
            stub.accept_response(cntl.release(), accept_reject_resp.release(),
                                 emtpy_resp.release(), brpc::DoNothing());

        } else {

            // 推大ballot number & close time window
            if (request->ballot_number() > _acceptor->_ballot_number) {
                LOG(INFO) << "bigger ballot to close time window, local ballot number is "
                          << _acceptor->_ballot_number << " req ballot number is " << request->ballot_number();
                _acceptor->_ballot_number = request->ballot_number();
                _acceptor->_ballot_of_time_window = _acceptor->_ballot_number;
                close_time_window();
            }

            LOG(INFO) << "acceptor update lease: new ballot number is " << request->ballot_number()
                      << " old bn is " << _acceptor->_lease.get_ballot_number();
            _acceptor->_lease.update_from(request);

            if(request->is_renew_lease()){
                if(_raft_node->get_leader_term() < request->ballot_number()){
                    _raft_node->reset_leader_id_term(request->ballot_number());
                    if(request->sender()!=_acceptor->_self_addr){
                        _raft_node->on_node_become_follower(request->sender());
                    }else{
                        inform_on_node_become_leader();
                    }
                }
            }


            us_to_expired = _acceptor->_lease.get_lease_end_ts() - butil::monotonic_time_ms();

            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionAcceptResponse> accept_resp(new ElectionAcceptResponse);
            std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);

            accept_resp->set_receiver(request->sender());
            accept_resp->set_sender(_acceptor->_self_addr.to_string());
            accept_resp->set_ballot_number(_acceptor->_ballot_number);
            accept_resp->set_accepted(true);
            accept_resp->set_lease_started_ts_on_proposer(request->lease_started_ts_on_proposer());
            accept_resp->set_lease_interval(request->lease_interval());
            accept_resp->set_origin_priority(_acceptor->acceptor_get_origin_priority());
            accept_resp->set_calculated_priority(_acceptor->acceptor_get_calculated_priority());
            accept_resp->set_member_list_version_ballot(_proposer->_member_list_version_ballot);
            accept_resp->set_member_list_version_index(_proposer->_member_list_version_index);


            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            int pos = request->sender().find_last_of(':');
            std::string addr = request->sender().substr(0, pos);

            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << _addr.to_string();
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "accept response send to " << request->sender()
                      << " from acceptor:" << request->sender();
            stub.accept_response(cntl.release(), accept_resp.release(),
                                 emtpy_resp.release(), brpc::DoNothing());


        }
        lck.unlock();

        if (us_to_expired - std::max(MAX_TXT,1000L) > 0) {  //CALCULATE_TRIGGER_ELECT_WATER_MARK = std::max(MAX_TXT,1s)

            reschedule_or_register_prepare_task(us_to_expired);
        }

        return;

    }

    void ElectionModule::handle_accept_response(const ElectionAcceptResponse *resp) {

        std::unique_lock<butil::Mutex> lck(_mutex);
        LOG(INFO) << "handle accept response from " << resp->sender();
        //检查消息有效性
        if (resp->ballot_number() < _proposer->_ballot_number) {
            return;
        }
        //ballot number更大的时候更新ballot number，若自己仍然是leader，触发leader prepare
        // fixme: ballot number更大，proposer还是leader的情况何时发生？
        if (resp->ballot_number() > _proposer->_ballot_number) {
            LOG(INFO) << "resp->ballot_number() > _proposer->_ballot_number";
            assert(!resp->accepted());
            _proposer->advance_ballot_number_and_reset_states(resp->ballot_number());

            if (_proposer->check_leader()) {
                // 在check leader后可能卡住，做leader prepare时就已经不再是leader了
                // 但是没有关系，正确性是由prepare阶段保证的，check_leader的意义在于尽量避免无谓的leader prepare流程

                _proposer->advance_ballot_number_and_reset_states(_proposer->_ballot_number + 1);//retry leader prepare
                _proposer->prepare(NodeRole::LEADER);
            }
        }

        // 检查是否因其他原因rejected
        if (!resp->accepted()) {
            LOG(INFO) << "receive reject msg";
            return;
        }

        // record majority
        bool reach_majority = _proposer->record_accept_ok(resp->sender(),
                                                          resp->lease_started_ts_on_proposer() +
                                                          resp->lease_interval());

        if (!reach_majority) return;

        LOG(INFO) << "accept_ok reach or over majority";

        int64_t new_lease_end = _proposer->get_majority_promised_not_vote_ts();

        //1. check lease if expired => step down
        if (_proposer->_role == NodeRole::LEADER && (!_proposer->check_leader())) {
            _proposer->_role = NodeRole::FOLLOWER;
            _proposer->reset_prepare_and_accept_states();
            _proposer->reset_lease_and_epoch();

            _raft_node->step_down();
            LOG(INFO) << "lease expired => step down";
        }

        //2. renew lease
        int64_t record_leader_lease_end;
        int64_t exposed_epoch;// not used
        _proposer->_leader_lease_and_epoch.get(record_leader_lease_end, exposed_epoch);

        LOG(INFO) << " record_leader_lease_end is " << record_leader_lease_end
                  << " exposed_epoch is " << exposed_epoch;
        if (new_lease_end > record_leader_lease_end) {// 需要更新
            LOG(INFO) << "renew proposer's lease & epoch with new_lease_end " << new_lease_end;
            _proposer->_leader_lease_and_epoch.
                    set_lease_and_epoch_if_lease_expired_or_just_set_lease(new_lease_end,
                                                                           _proposer->_prepare_success_ballot);
        }


        // check lease if valid => take over
        if(_proposer->_switch_source_leader_ballot != INVALID_VALUE){
            if(_proposer->_switch_source_leader_ballot != _proposer->_ballot_number - 1){
                LOG(ERROR) << "self ballot number is advanced, change leader failed";
            }else{
                // change leader
                if(_proposer->_role != NodeRole::LEADER && _proposer->check_leader()){
                    _proposer->_role = NodeRole::LEADER;
                    _proposer->_switch_source_leader_ballot = INVALID_VALUE;
                    _proposer->_switch_source_leader_addr.reset();
                    LOG(INFO)<< " change leader success, now become leader, next do propose again";
                    _proposer->propose(true);
                }
            }
        }else{
            // voted to be leader
            if (_proposer->_role != NodeRole::LEADER && _proposer->check_leader()) {
                _proposer->_role = NodeRole::LEADER;
                LOG(INFO) << " now voted to be leader, next do propose again";
                _proposer->propose(true);
            }
        }

        // if follower's priority is higher than leader's, try to change leader
        if(_proposer->_role == NodeRole::LEADER && resp->sender() != _proposer->_self_addr){
            if(_proposer->check_new_resp_if_higher_than_leader(*resp)){
                _proposer->_highest_priority_cache.check_expired();

                if(_proposer->_highest_priority_cache._is_msg_valid){
                    if(_proposer->_highest_priority_cache._cached_msg.sender()
                        == resp->sender()){
                        // cached msg is not expired && leader has received its resp twice, try to change leader
                        LOG(INFO) << "leader has received higher accept response from " <<resp->sender()
                                     << " twice, try to change leader";
                        _proposer->change_leader_to(resp->sender());
                    }else{
                        if(_proposer->check_new_resp_if_higher_than_cache(*resp)){
                            _proposer->_highest_priority_cache.set(*resp);
                            LOG(INFO) << "cache higher accept resp from" << resp->sender();
                        }
                    }
                }else{
                    _proposer->_highest_priority_cache.set(*resp);
                    LOG(INFO) << "cache higher accept resp from" << resp->sender();
                }
            }
        }



    }

    void ElectionModule::handle_change_leader(const ElectionChangeLeaderRequest *request) {

        std::unique_lock<butil::Mutex> lck(_mutex);
        bool accept = false;
        if (request->sender() == _proposer->_self_addr) { // self to self
            if (request->ballot_number() == _proposer->_ballot_number) {
                accept = true;
            }else{
                LOG(ERROR) <<"change leader to self msg's ballot number not expected";
            }
        } else { // another one to self
            if (request->ballot_number() > _proposer->_ballot_number) {
                accept = true;
            }else{
                LOG(ERROR)<<"change leader msg's ballot number is too small";
            }
        }
        if (!accept) {
            LOG(INFO) << "change leader msg not accepted";
        } else {//todo: if add member version, here need to judge member version
            if(request->member_list_version_index() > _proposer->get_member_list_version_index()){
                LOG(ERROR) << "change leader msg's member list is newer than leader's";
                return ;
            }

            _proposer->advance_ballot_number_and_reset_states(request->ballot_number());
            _proposer->_switch_source_leader_ballot = request->ballot_number();
            _proposer->_switch_source_leader_addr = request->sender();
            LOG(INFO) << "receive change leader msg, do leader prepare";
            _proposer->prepare(NodeRole::LEADER);

        }
    }

    void ElectionModule::inform_on_node_become_leader() {
        _raft_node->on_node_become_leader();
    }

    void ElectionModule::inform_node_step_down() {
        _raft_node->step_down();
    }

    void ElectionModule::apply_config_change(std::vector<mraft::PeerId>& new_member_list,
                                             int64_t ballot_number, int64_t log_index) {
        _proposer->set_member_list(new_member_list,ballot_number,log_index);
    }

    void ElectionProposer::inform_raft_node_new_term(int64_t new_term) {
        _node->_raft_node->TEST_unsafe_reset_term(new_term);
    }

    int64_t ElectionProposer::proposer_get_calculated_priority() {
        return _node->_raft_node->get_calculated_priority();
    }

    int64_t ElectionAcceptor::acceptor_get_calculated_priority() {
        return _node->_raft_node->get_calculated_priority();
    }

    int64_t ElectionAcceptor::acceptor_get_origin_priority() {
        return _node->_proposer->get_origin_priority();
    }

    void ElectionModule::reschedule_or_register_prepare_task(int64_t ms) {
        LOG(INFO) << "prepare time reset to " << ms << " ms ";
        _prepare_timer.reset(ms);
    }

    void ElectionModule::register_renew_lease_task(int64_t ms) {
        _leader_renew_timer.reset(ms);
    }

    void ElectionModule::time_window_run() {

        _acceptor->set_time_window_flag(false);

        // todo： 关闭时间窗口的动作
        if (!_acceptor->_is_highest_req_valid) {
            LOG(INFO) << "NO VALID REQ";
        } else {
            std::unique_ptr<brpc::Controller> cntl(new brpc::Controller);
            std::unique_ptr<ElectionPrepareResponse> prepare_resp(new ElectionPrepareResponse);
            std::unique_ptr<EmptyResponse> emtpy_resp(new EmptyResponse);

            prepare_resp->set_receiver(_acceptor->_highest_priority_prepare_req.sender());
            prepare_resp->set_sender(_acceptor->_self_addr.to_string());
            prepare_resp->set_is_send_by_proposer(false);
            prepare_resp->set_accept(true);
            prepare_resp->set_ballot_number(_acceptor->_highest_priority_prepare_req.ballot_number());

            brpc::ChannelOptions options;
            options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
            options.max_retry = 0;
            options.connect_timeout_ms = 200; // 200ms
            brpc::Channel channel;

            int pos = _acceptor->_highest_priority_prepare_req.sender().find_last_of(':');
            std::string addr = _acceptor->_highest_priority_prepare_req.sender().substr(0, pos);
            if (0 != channel.Init(addr.c_str(), &options)) {
                LOG(WARNING) << "node channel init failed, addr " << _acceptor->_self_addr.to_string();
            }
            LogService_Stub stub(&channel);
            LOG(INFO) << "answer highest prepare response send to "
                      << _acceptor->_highest_priority_prepare_req.sender() << " from acceptor:"
                      << _acceptor->_self_addr.to_string();
            stub.prepare_response(cntl.release(), prepare_resp.release(), emtpy_resp.release(), brpc::DoNothing());

            _acceptor->_is_highest_req_valid = false;

            LOG(INFO) << "timer run to close time window ";
            close_time_window();
        }
    }

    void ElectionModule::TimeWindowTimer::run() {
        _election_module->time_window_run();
    }

    void ElectionModule::start_time_window(int64_t timeout) {
        LOG(INFO) << "acceptor start time window with span: " << timeout << " ms";
        _time_window_timer.reset(timeout);
        _time_window_timer.start();
        _acceptor->set_time_window_flag(true); // _is_time_window_opened = true;
        _acceptor->set_last_time_window_open_ts(butil::monotonic_time_ms());
    }

    void ElectionModule::close_time_window() {
        //LOG(INFO) << "acceptor close time window";
        _time_window_timer.stop();
        _acceptor->set_time_window_flag(false);
        _acceptor->set_highest_req_valid(false);
    }

}