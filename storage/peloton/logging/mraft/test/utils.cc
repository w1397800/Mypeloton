#include "utils.h"

#include <butil/status.h>

namespace mraft {

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

std::string generateNodeConfig(int nodeIndex, int streamNumber,
                               int start_port = 8660) {
    std::string nodeConfig = "{\n";
    nodeConfig +=
        "            \"name\": \"node" + std::to_string(nodeIndex) + "\",\n";
    nodeConfig += "            \"addresses\": [";

    for (int j = 0; j < streamNumber; ++j) {
        nodeConfig +=
            "\"127.0.0.1:" + std::to_string(start_port + nodeIndex * 10 + j) +
            "\"";
        if (j < streamNumber - 1) {
            nodeConfig += ", ";
        }
    }

    nodeConfig += "],\n";
    nodeConfig += "            \"priority\": " + std::to_string(nodeIndex) +
                  "\n        }";
    return nodeConfig;
}

std::string generateConfig(int nodeNumber, int streamNumber, int nodeIndex,
                           const std::string& groupId = "666") {
    std::string config = "{\n";
    config += "    \"node_number\": " + std::to_string(nodeNumber) + ",\n";
    config += "    \"stream_number\": " + std::to_string(streamNumber) + ",\n";
    config += "    \"group_id\": \"" + groupId + "\",\n";
    config += "    \"name\": \"node" + std::to_string(nodeIndex) + "\",\n";
    config += "    \"node_configs\": [\n";

    for (int i = 0; i < nodeNumber; ++i) {
        config += generateNodeConfig(i, streamNumber);
        if (i < nodeNumber - 1) {
            config += ",\n";
        }
    }

    config += "\n    ]\n}";
    return config;
}

// nodeNumber: 加入当前节点后，集群中节点的数量
std::string generateLearnerConfig(int nodeNumber, int streamNumber,
                                  int nodeIndex,
                                  const std::string& groupId = "666") {
    std::string config = "{\n";
    config += "    \"node_number\": " + std::to_string(nodeNumber) + ",\n";
    config += "    \"stream_number\": " + std::to_string(streamNumber) + ",\n";
    config += "    \"group_id\": \"" + groupId + "\",\n";
    config += "    \"name\": \"node" + std::to_string(nodeIndex) + "\",\n";
    config += "    \"node_configs\": [\n";

    config += generateNodeConfig(nodeIndex, streamNumber);

    config += "\n    ]\n}";
    return config;
}

std::string formatFileName(int nodeNumber, int streamNumber, int nodeIndex) {
    return std::to_string(nodeNumber) + "node" + std::to_string(streamNumber) +
           "stream" + std::to_string(nodeIndex) + ".json";
}

butil::Status config_generator(int nodeNumber, int streamNumber,
                               std::string path) {
    if (path.empty() || path[path.size() - 1] != '/') {
        LOG(ERROR) << "Invalid path: " << path;
        return butil::Status(EINVAL, "Invalid path");
    }
    for (int i = 0; i < nodeNumber; ++i) {
        std::string config = generateConfig(nodeNumber, streamNumber, i);
        std::string fileName =
            path + formatFileName(nodeNumber, streamNumber, i);

        FILE* outFile = fopen(fileName.c_str(), "w");
        if (outFile == nullptr) {
            butil::Status st;
            st.set_error(EPERM,
                         "Unable to open file '" + fileName + "' for writing.");
            return st;
        }

        // Check the result of fprintf for error
        if (fprintf(outFile, "%s", config.c_str()) < 0) {
            butil::Status st;
            st.set_error(EPERM, "Unable to write to file '" + fileName + "'.");
            return st;
        }

        // fclose(outFile);
        if (fclose(outFile) != 0) {
            butil::Status st;
            st.set_error(EPERM, "Unable to close file '" + fileName + "'.");
            return st;
        }

        LOG(INFO) << "Config for node" << i << " has been saved to '"
                  << fileName << "'\n";
    }
    return butil::Status::OK();
}

// nodeNumber: 加入当前节点后，集群中节点的数量
butil::Status learner_config_generator(int nodeNumber, int streamNumber,
                                       std::string path) {
    if (path.empty() || path[path.size() - 1] != '/') {
        LOG(ERROR) << "Invalid path: " << path;
        return butil::Status(EINVAL, "Invalid path");
    }

    std::string config =
        generateConfig(nodeNumber, streamNumber, nodeNumber - 1);
    std::string fileName =
        path + formatFileName(nodeNumber, streamNumber, nodeNumber - 1);

    FILE* outFile = fopen(fileName.c_str(), "w");
    if (outFile == nullptr) {
        butil::Status st;
        st.set_error(EPERM,
                     "Unable to open file '" + fileName + "' for writing.");
        return st;
    }

    // Check the result of fprintf for error
    if (fprintf(outFile, "%s", config.c_str()) < 0) {
        butil::Status st;
        st.set_error(EPERM, "Unable to write to file '" + fileName + "'.");
        return st;
    }

    // fclose(outFile);
    if (fclose(outFile) != 0) {
        butil::Status st;
        st.set_error(EPERM, "Unable to close file '" + fileName + "'.");
        return st;
    }

    LOG(INFO) << "Config for node" << nodeNumber - 1 << " has been saved to '"
              << fileName << "'\n";

    return butil::Status::OK();
}

butil::Status send_one_log(RaftNode& node, int64_t log_size) {
    std::unique_ptr<char[]> data(new char[log_size]);

    int streamNums = node.stream_nums();

    // A vector of vector tasks to hold tasks for each stream
    std::vector<std::vector<std::shared_ptr<Task>>> tasks(streamNums);

    // A vector of IOBuf logs to hold log for each stream
    std::vector<butil::IOBuf> logs(streamNums);

    // Populate the logs and tasks based on the number of streams
    for (int i = 0; i < streamNums; ++i) {
        logs[i].append(data.get(), log_size);

        tasks[i].emplace_back(new Task);
        tasks[i].back()->data = &logs[i];

        auto st = node.append_log_async(tasks[i], i);
        if (!st.ok()) {
            return st;
        }
    }
    return butil::Status::OK();
}
}  // namespace mraft