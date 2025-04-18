//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logger_configuration.h
//
// Identification: src/include/benchmark/logger/logger_configuration.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <getopt.h>
#include <vector>
#include <sys/time.h>
#include <iostream>

#include "storage/peloton/common/internal_types.h"

class configuration {
 public:
  // logging type
  LoggingType logging_type;

  // checkpoint type
  CheckpointType checkpoint_type;

  // log file dir
  std::string default_log_file_dir;

  std::vector<std::string> log_file_dirs;
};

void Usage(FILE *out);

void set_log_state(configuration &state);
