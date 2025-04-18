#include <brpc/errno.pb.h>
#include <brpc/controller.h>
#include <brpc/channel.h>
#include "election_acceptor.h"

namespace mraft {

    ElectionAcceptor::ElectionAcceptor(mraft::PeerId addr,ElectionModule* node) :  _ballot_number(INVALID_VALUE),
                                                                                   _ballot_of_time_window(INVALID_VALUE),
                                                                                   _self_addr(addr),
                                                                                   _node(node),
                                                                                   _last_time_window_open_ts(INVALID_VALUE){
        LOG(INFO) << "initialize acceptor";
    }

    void ElectionAcceptor::start() {
    }

    void ElectionAcceptor::stop() {
    }


    void ElectionAcceptor::renew_highest_req(ElectionPrepareRequest req) {
        _highest_priority_prepare_req = req;
    }

    bool ElectionAcceptor::is_new_req_higher(ElectionPrepareRequest req) {
        if (!_is_highest_req_valid) return true;
        if(req.calculated_priority() > _highest_priority_prepare_req.calculated_priority()){
            return true;
        }else{
            if(req.calculated_priority() == _highest_priority_prepare_req.calculated_priority()){
                return req.origin_priority() > _highest_priority_prepare_req.origin_priority();
            }
        }
        return false;
    }

}