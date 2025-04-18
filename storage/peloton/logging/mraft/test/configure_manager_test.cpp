#include "gtest/gtest.h"
#include "replica/configuration_manager.h"

namespace mraft {

TEST(ConfigureManager, ConfigInitTest) {
    mraft::ConfigurationManager manager;
    mraft::ConfigurationManagerOptions options;
    options.config_file_path = "/home/lyf/mraft/test.json";
    manager.init(std::move(options));
}

TEST(ConfigureManager, ConfigTostringTest) {
    mraft::ConfigurationManager manager;
    mraft::ConfigurationManagerOptions options;
    options.config_file_path = "/home/lyf/mraft/test.json";
    manager.init(std::move(options));
    LOG(INFO) << manager.to_string();
}

TEST(ConfigureManager, GetConfigAtIndexTest) {
    int64_t index = 0;
    mraft::ConfigurationManager manager;
    mraft::ConfigurationManagerOptions options;
    options.config_file_path = "/home/lyf/mraft/test.json";
    manager.init(std::move(options));
    std::cout << "Cluster at index " << index << "is ["
              << manager.get_config_at_index(index) << "]";
}

TEST(ConfigureManager, AddOnePeerTest) {
    std::string name = "node4";
    mraft::NodeConfiguration node_config;
    node_config.origin_priority = 10;
    node_config.addresses.push_back(mraft::PeerId("127.0.0.10:8000"));
    node_config.addresses.push_back(mraft::PeerId("127.0.0.20:8000"));
    mraft::ConfigurationManager manager;
    mraft::ConfigurationManagerOptions options;
    options.config_file_path = "/home/lyf/mraft/test.json";
    manager.init(std::move(options));
    manager.add_one_peer(name, node_config);
    std::cout << manager.to_string();
    std::cout << "Cluster at index 0 is [" << manager.get_config_at_index(0)
              << "]";
}

TEST(ConfigureManager, RemoveOnePeerTest) {
    std::string name = "node4";
    mraft::ConfigurationManager manager;
    mraft::ConfigurationManagerOptions options;
    options.config_file_path = "/home/lyf/mraft/test.json";
    manager.init(std::move(options));
    manager.remove_one_peer(name);
    std::cout << manager.to_string();
    std::cout << "Cluster at index 0 is [" << manager.get_config_at_index(0)
              << "]";
}

}  // namespace mraft

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}