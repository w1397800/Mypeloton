//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functions.h
//
// Identification: src/include/function/functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <vector>

#include "storage/peloton/type/value.h"



struct BuiltInFuncType {
  typedef Value (*Func)(const std::vector<Value> &);

  OperatorId op_id;
  Func impl;

  BuiltInFuncType(OperatorId _op_id, Func _impl) : op_id(_op_id), impl(_impl) {}
  BuiltInFuncType() : BuiltInFuncType(OperatorId::Invalid, nullptr) {}
};

class BuiltInFunctions {
 private:
  // Map the function name in C++ source (should be unique) to the actual
  // function implementation
  static std::unordered_map<std::string, BuiltInFuncType> kFuncMap;

 public:
  static void AddFunction(const std::string &func_name, BuiltInFuncType func);

  // Get the function from the name in C++ source code
  static BuiltInFuncType GetFuncByName(const std::string &func_name);
};

