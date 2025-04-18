// file: throughput_server_main.cpp

#include <brpc/server.h>
#include <butil/logging.h>

#include "throughput_server.h"
#include "rpc/throughput_test.pb.h"
#include "common/common.h"

int main() {
  ParseFlags({"-bthread_concurrency=18", "-max_body_size=134217728"});

  brpc::Server server;

  ThroughputServiceImpl service;
  if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }

  brpc::ServerOptions options;
  options.idle_timeout_sec = -1;

  if (server.Start(8412, &options) != 0) {
    LOG(ERROR) << "Fail to start server";
    return -1;
  }

  server.RunUntilAskedToQuit();

  return 0;
}
