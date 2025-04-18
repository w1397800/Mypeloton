//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// exception.cpp
//
// Identification: src/common/exception.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "storage/peloton/common/exception.h"


std::ostream &operator<<(std::ostream &os, const Exception &e) {
  os << e.exception_message_.c_str();
  return os;
}

