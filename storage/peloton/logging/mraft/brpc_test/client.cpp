#include <brpc/channel.h>
#include <brpc/options.pb.h>
#include <bthread/countdown_event.h>
#include <butil/logging.h>
#include <butil/time.h>

#include <condition_variable>  // 需要这两个头文件
#include <cstdint>
#include <mutex>

#include "common/common.h"
#include "rpc/throughput_test.pb.h"

DEFINE_string(ip_port, "localhost:8412",
              "The address of the server to connect to");

int64_t max_latanecy = 0;
int64_t min_latency = INT64_MAX;
double avg_latency = 0;
int64_t cur_count = 1;

class Semaphore {
   public:
    Semaphore(int count) : count_(count) {}

    void acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (count_ == 0) {
            cv_.wait(lock);
        }
        --count_;
    }

    void release() {
        std::unique_lock<std::mutex> lock(mutex_);
        ++count_;
        cv_.notify_one();
    }

   private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int count_;
};

void RpcCallback(brpc::Controller* cntl, example::AppendEntriesRequest* request,
                 example::AppendEntriesResponse* response,
                 bthread::CountdownEvent* counter, Semaphore* sem) {
    // Check the RPC response here.
    if (cntl->Failed()) {
        LOG(ERROR) << "Fail to send AppendEntriesRequest, "
                   << cntl->ErrorText();
    }
    int64_t latency_in_us = cntl->latency_us();  // 获取延迟
    if (latency_in_us > max_latanecy) {
        max_latanecy = latency_in_us;
    }
    if (latency_in_us < min_latency) {
        min_latency = latency_in_us;
    }

    avg_latency = (avg_latency * (cur_count - 1) + latency_in_us) / cur_count;
    cur_count++;

    delete cntl;
    delete request;
    delete response;
    counter->signal();
    sem->release();  // 释放一个信号量
}

void FillCommenFields(example::AppendEntriesRequest* request,
                      int request_size) {
    // Set up the request here.
    request->set_group_id("group");
    request->set_server_id("server");
    request->set_peer_id("peer");
    request->set_term(0);
    request->set_prev_log_term(0);
    request->set_prev_log_index(0);
    request->set_committed_index(0);

    example::EntryMeta* entry = request->add_entries();
    entry->set_term(0);
    entry->set_type(example::ENTRY_TYPE_DATA);
    entry->add_peers("peer1");
    entry->add_peers("peer2");
    entry->set_data_len(request_size);
    entry->add_old_peers("old_peer");
}

bool InitChannel(brpc::Channel& channel) {
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_BAIDU_STD;
    options.connection_type = brpc::CONNECTION_TYPE_SINGLE;
    options.max_retry = 3;
    return channel.Init(FLAGS_ip_port.c_str(), &options) == 0;
}

void SendRequest(example::ThroughputTestService_Stub& stub, int request_size,
                 bthread::CountdownEvent& counter, Semaphore& sem) {
    sem.acquire();

    brpc::Controller* cntl = new brpc::Controller();
    cntl->set_timeout_ms(200000);

    example::AppendEntriesRequest* request =
        new example::AppendEntriesRequest();
    FillCommenFields(request, request_size);
    cntl->request_attachment().append(std::string(request_size, 'a'));

    example::AppendEntriesResponse* response =
        new example::AppendEntriesResponse();
    google::protobuf::Closure* done =
        brpc::NewCallback(RpcCallback, cntl, request, response, &counter, &sem);
    stub.TestThroughput(cntl, request, response, done);
}

std::string GetSizeString(int size) {
    if (size < 1024) {
        return std::to_string(size) + "B";
    } else if (size < 1024 * 1024) {
        return std::to_string(size / 1024.0) + "KB";
    } else {
        return std::to_string(size / 1024.0 / 1024.0) + "MB";
    }
}

void LogResults(double elapsed_time, int num_requests, int request_size) {
    double total_request_size_mb =
        num_requests * request_size / 1024.0 / 1024.0;
    double throughput =
        total_request_size_mb / (elapsed_time / 1000.0);  // throughput in MB/s

    LOG(INFO) << "----------------- TEST finished -----------------";
    LOG(INFO) << "Total request size: " << total_request_size_mb << "MB";
    LOG(INFO) << "Request size: " << GetSizeString(request_size);
    LOG(INFO) << "Elapsed time: " << elapsed_time << "ms";
    LOG(INFO) << "Throughput: " << throughput << "MB/s\n";
    LOG(INFO) << "Max latency: " << max_latanecy << "us";
    LOG(INFO) << "Min latency: " << min_latency << "us";
    LOG(INFO) << "Avg latency: " << avg_latency << "us\n";
}

void BrpcTest(int num_requests, int request_size, int max_batch) {
    // Initialize the channel with the given options
    brpc::Channel channel;
    if (!InitChannel(channel)) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }

    example::ThroughputTestService_Stub stub(&channel);
    bthread::CountdownEvent counter(num_requests);
    Semaphore sem(max_batch);  // Create a semaphore

    // Measure the time taken for all requests
    butil::Timer timer;
    timer.start();

    // Send the requests
    for (int i = 0; i < num_requests; ++i) {
        // std::cout << "i: " << i << std::endl;
        SendRequest(stub, request_size, counter, sem);
    }

    counter.wait();
    timer.stop();

    // Calculate and log the results
    LogResults(timer.m_elapsed(), num_requests, request_size);
}

void UpdateLatencyStats(int64_t latency_in_us) {
    if (latency_in_us > max_latanecy) {
        max_latanecy = latency_in_us;
    }
    if (latency_in_us < min_latency) {
        min_latency = latency_in_us;
    }
    avg_latency = (avg_latency * (cur_count - 1) + latency_in_us) / cur_count;
    cur_count++;
}

void IOTest(int num_requests, int request_size) {
    const char* filename = "test_io.bin";
    FILE* file = fopen(filename, "wb");
    if (!file) {
        LOG(ERROR) << "Failed to open file for writing: " << filename
                   << std::endl;
        return;
    }

    // Prepare data to be written
    char* data = new char[request_size];
    memset(data, 'a', request_size);

    // Measure the time taken for all requests
    butil::Timer timer;
    timer.start();
    max_latanecy = 0;
    min_latency = INT64_MAX;
    avg_latency = 0;
    cur_count = 1;

    // Write data to the file
    for (int i = 0; i < num_requests; ++i) {
        auto write_start_time = std::chrono::high_resolution_clock::now();

        size_t written = fwrite(data, 1, request_size, file);
        if (written != request_size) {
            std::cerr << "Failed to write data to the file." << std::endl;
            delete[] data;
            fclose(file);
            return;
        }
        fsync(fileno(file));

        auto write_end_time = std::chrono::high_resolution_clock::now();
        int64_t latency_in_us = std::chrono::duration<double, std::micro>(
                                    write_end_time - write_start_time)
                                    .count();

        UpdateLatencyStats(latency_in_us);
    }

    timer.stop();

    // Close file and cleanup
    fclose(file);
    delete[] data;

    // Delete the file
    if (remove(filename) != 0) {
        std::cerr << "Error deleting the file: " << filename << std::endl;
    }

    // Log the results
    LogResults(timer.m_elapsed(), num_requests, request_size);
}

int main(int argc, char* argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    ParseFlags({"-bthread_concurrency=18", "-max_body_size=134217728"});
    int max_batch = 1;

    // request size: 1KB
    // BrpcTest(1024 * 1024, 1024, max_batch);

    int totol_bytes = 1024 * 1024 * 1024;  // 1GB
    int max_bytes = 1024 * 1024 * 4;           // 1MB
    for (int request_size = 1024 * 32; request_size <= max_bytes;
         request_size *= 2) {
        // IOTest(totol_bytes / request_size, request_size);
        BrpcTest(totol_bytes / request_size, request_size, max_batch);
    }

    return 0;
}
