//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// printable.cpp
//
// Identification: src/common/printable.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "storage/peloton/common/printable.h"



// Get a string representation for debugging
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo();
  return os;
};
