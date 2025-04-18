//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numeric_value.cpp
//
// Identification: src/backend/numeric_value.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "integer_parent_type.h"

#include <cmath>
#include <iostream>
#include "boolean_type.h"
#include "decimal_type.h"
#include "varlen_type.h"

IntegerParentType::IntegerParentType(TypeId type) : NumericType(type) {}

Value IntegerParentType::Min(const Value& left, const Value& right) const {
  //PELOTON_ASSERT(left.CheckInteger());
  //PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);

  if (left.CompareLessThan(right) == CmpBool::CmpTrue) return left.Copy();
  return right.Copy();
}

Value IntegerParentType::Max(const Value& left, const Value& right) const {
  //PELOTON_ASSERT(left.CheckInteger());
  //PELOTON_ASSERT(left.CheckComparable(right));
  if (left.IsNull() || right.IsNull()) return left.OperateNull(right);

  if (left.CompareGreaterThanEquals(right) == CmpBool::CmpTrue) return left.Copy();
  return right.Copy();
}
