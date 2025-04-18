// file: throughput_server.cpp
#pragma once
#include <brpc/server.h>

#include "rpc/throughput_test.pb.h"

class ThroughputServiceImpl : public example::ThroughputTestService {
   public:
    virtual void TestThroughput(google::protobuf::RpcController* cntl_base,
                                const example::AppendEntriesRequest* request,
                                example::AppendEntriesResponse* response,
                                google::protobuf::Closure* done) {
        (void)request;
        (void)cntl_base;
        brpc::ClosureGuard done_guard(done);

        response->set_term(123);
        response->set_success(true);
        response->set_last_log_index(456);
        response->set_readonly(false);
    }
};
