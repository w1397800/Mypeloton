//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_functions.h
//
// Identification: src/include/function/numeric_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <vector>
#include "storage/peloton/type/type.h"
#include "storage/peloton/type/value.h"
class Type;

class Value;


class NumericFunctions {
 public:
  // Abs
  static double Abs(double arg);
  static Value _Abs(const std::vector<Value> &args);

  // Sqrt
  static double ISqrt(uint32_t num);
  static double DSqrt(double num);
  static Value Sqrt(const std::vector<Value> &args);

  // Floor
  static double Floor(double val);
  static Value _Floor(const std::vector<Value> &args);

  // Round
  static double Round(double arg);
  static Value _Round(const std::vector<Value> &args);

  // Ceil
  static double Ceil(double args);
  static Value _Ceil(const std::vector<Value> &args);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Input functions
  ///
  //////////////////////////////////////////////////////////////////////////////

  static bool InputBoolean(const Type &type, const char *ptr,
                           uint32_t len);

  static int8_t InputTinyInt(const Type &type, const char *ptr,
                             uint32_t len);

  static int16_t InputSmallInt(const Type &type, const char *ptr,
                               uint32_t len);

  static int32_t InputInteger(const Type &type, const char *ptr,
                              uint32_t len);

  static int64_t InputBigInt(const Type &type, const char *ptr,
                             uint32_t len);

  static double InputDecimal(const Type &type, const char *ptr,
                             uint32_t len);
};

