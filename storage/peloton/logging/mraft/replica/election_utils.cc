#include "election_utils.h"


namespace mraft {
    Lease &Lease::operator=(const Lease &rhs) {
        new(this) Lease(rhs);
        return *this;
    }


    bool Lease::is_expired() const { // is_expired()只支持在本地判断
        return butil::monotonic_time_ms() > _lease_end_ts;
    }

    void Lease::reset() {
        _owner.reset();
        _lease_end_ts = -1;
    }

    bool Lease::is_empty() const {
        return _owner.is_empty() && _lease_end_ts == -1;
    }

    Lease::Lease(const Lease &rhs) {
        _owner = rhs.get_owner();
        _lease_end_ts = rhs.get_lease_end_ts();
        _ballot_number = rhs.get_ballot_number();
    };


    void Lease::update_from(const ElectionAcceptRequest *accept_req) {
        _owner = accept_req->sender();
        _lease_end_ts = std::max(_lease_end_ts,
                                 int64_t(butil::monotonic_time_ms() + accept_req->lease_interval()));
        assert(accept_req->ballot_number() >= _ballot_number);
        _ballot_number = accept_req->ballot_number();
    }

    const mraft::PeerId &Lease::get_owner() const {
        return _owner;
    }

    int64_t Lease::get_ballot_number() const {
        return _ballot_number;
    }

    int64_t Lease::get_lease_end_ts() const {
        return _lease_end_ts;
    }

    void Lease::get_owner_and_ballot(mraft::PeerId &owner, int64_t &ballot) const {
        owner = _owner;
        ballot = _ballot_number;
    }

}