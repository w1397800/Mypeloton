#pragma once
#include <glog/logging.h>

namespace mraft {

#define MLOG(level) LOG(level) << "[" << __FUNCTION__ << "]: "

}  // namespace mraft