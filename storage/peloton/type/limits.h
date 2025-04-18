//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limits.h
//
// Identification: src/include/type/limits.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cfloat>
#include<limits>

using namespace std;
static const double DBL_LOWEST = std::numeric_limits<double>::lowest();
  static const double FLT_LOWEST = std::numeric_limits<float>::lowest();

static const int8_t PELOTON_INT8_MIN = (-__SCHAR_MAX__ );
static const int16_t PELOTON_INT16_MIN = (-__SHRT_MAX__);
static const int32_t PELOTON_INT32_MIN = (-__INT_MAX__);
static const int64_t PELOTON_INT64_MIN = (-__LONG_MAX__);
static const double PELOTON_DECIMAL_MIN = FLT_LOWEST;
static const uint64_t PELOTON_TIMESTAMP_MIN = 0;
static const int32_t PELOTON_DATE_MIN = (-__INT_MAX__ + 1);
static const int8_t PELOTON_BOOLEAN_MIN = 0;

static const int8_t PELOTON_INT8_MAX = __SCHAR_MAX__;
static const int16_t PELOTON_INT16_MAX = __SHRT_MAX__;
static const int32_t PELOTON_INT32_MAX = __INT_MAX__;
static const int64_t PELOTON_INT64_MAX = __LONG_MAX__;
static const uint64_t PELOTON_UINT64_MAX = __LONG_MAX__ - 1;
static const double PELOTON_DECIMAL_MAX = __DBL_MAX__;
static const uint64_t PELOTON_TIMESTAMP_MAX = 11231999986399999999U; // ???
static const int32_t PELOTON_DATE_MAX = __INT_MAX__;
static const int8_t PELOTON_BOOLEAN_MAX = 1;

static const uint32_t PELOTON_VALUE_NULL = __INT_MAX__;
static const int8_t PELOTON_INT8_NULL = -__SCHAR_MAX__;
static const int16_t PELOTON_INT16_NULL = -__SHRT_MAX__;
static const int32_t PELOTON_INT32_NULL = -__INT_MAX__;
static const int64_t PELOTON_INT64_NULL = -__LONG_MAX__;
static const int32_t PELOTON_DATE_NULL = -__INT_MAX__;
static const uint64_t PELOTON_TIMESTAMP_NULL = __LONG_MAX__;
static const double PELOTON_DECIMAL_NULL = DBL_LOWEST;
static const int8_t PELOTON_BOOLEAN_NULL = -__SCHAR_MAX__;

static const uint32_t PELOTON_VARCHAR_MAX_LEN = __INT_MAX__;

// Use to make TEXT type as the alias of VARCHAR(TEXT_MAX_LENGTH)
static const uint32_t PELOTON_TEXT_MAX_LEN = 1000000000;

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
#define OBJECTLENGTH_NULL -1
