#pragma once
#include <gflags/gflags.h>
#include <butil/logging.h>



#define MLOG(level) LOG(level) << "[" << __FUNCTION__ << "]: "

inline void ParseFlags(std::vector<std::string> flags) {
    int argc = flags.size() + 1;  // +1 是为了包括程序名
    char** argv = new char*[argc];

    // 设置第一个参数为程序名
    argv[0] = const_cast<char*>("main");

    // 设置其它的参数
    for (size_t i = 1; i < static_cast<size_t>(argc); i++) {
        argv[i] = const_cast<char*>(flags[i - 1].c_str());
    }

    google::ParseCommandLineFlags(&argc, &argv, true);

    // delete[] argv;
}
