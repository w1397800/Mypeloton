//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.cpp
//
// Identification: src/main/logger/logger_configuration.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iomanip>
#include <algorithm>
#include <sys/stat.h>

#include "storage/peloton/common/exception.h"
#include "storage/peloton/store/storage_manager.h"
#include "logger_configuration.h"

extern CheckpointType peloton_checkpoint_mode;

void set_log_state(configuration& state) {
  state.default_log_file_dir = "./";

  // 开：SSD_WAL
  // 关：INVALID
  state.logging_type = LoggingType::SSD_WAL;

  // 开：NORMAL
  // 关：INVALID
  state.checkpoint_type = CheckpointType::INVALID;

  // 可以替换成绝对路径，日志文件会均衡放到这些位置
  state.log_file_dirs = {
      "./",
      "./"
  };
}
