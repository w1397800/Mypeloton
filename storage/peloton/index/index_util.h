//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_util.h
//
// Identification: src/include/index/index_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>

#include "storage/peloton/type/value.h"
#include "index.h"


class IndexMetadata;
class Index;

class IndexUtil {
 public:
  /*
   * DefinesLowerBound() - Returns true if the expression is > or >= or ==
   *
   * Note: Since we consider equality (==) as both forward and backward
   * expression, when dealing with expression type both could be true
   *
   * NOTE 2: We put equality at the first to compare since we want a fast result
   * when it is equality (lazy evaluation)
   */
  static inline bool DefinesLowerBound(ExpressionType e) {
    // To reduce branch misprediction penalty
    return e == ExpressionType::COMPARE_EQUAL ||
        e == ExpressionType::COMPARE_GREATERTHAN ||
        e == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
  }

  /*
   * DefinesUpperBound() - Returns true if the expression is < or <= or ==
   *
   * Please refer to DefinesLowerBound() for more information
   */
  static inline bool DefinesUpperBound(ExpressionType e) {
    return e == ExpressionType::COMPARE_EQUAL ||
        e == ExpressionType::COMPARE_LESSTHAN ||
        e == ExpressionType::COMPARE_LESSTHANOREQUALTO;
  }

  static bool ValuePairComparator(const std::pair<Value, int> &i,
                                  const std::pair<Value, int> &j);

  static void ConstructIntervals(uint leading_column_id,
                          const std::vector<Value> &values,
                          const std::vector<uint> &key_column_ids,
                          const std::vector<ExpressionType> &expr_types,
                          std::vector<std::pair<Value, Value>> &intervals);

  static void FindMaxMinInColumns(uint leading_column_id,
                           const std::vector<Value> &values,
                           const std::vector<uint> &key_column_ids,
                           const std::vector<ExpressionType> &expr_types,
                           std::map<uint, std::pair<Value, Value>> &non_leading_columns);

  static bool HasNonOptimizablePredicate(const std::vector<ExpressionType> &expr_types);

  static bool FindValueIndex(const IndexMetadata *metadata_p,
                      const std::vector<uint> &tuple_column_id_list,
                      const std::vector<ExpressionType> &expr_list,
                      std::vector<std::pair<uint, uint>> &value_index_list);

  /**
   * Generate a string with a list of the values in the index.
   * Note that this is will not print out the keys!.
   * @param index
   * @return
   */
  static std::string Debug(Index *index);

  /**
   * Return a string representation of an ItemPointer.
   * This is here because we don't want to add extra methods
   * to our ItemPointer since that takes up memory.
   * @param ptr
   * @return
   */
  static const std::string GetInfo(const ItemPointer *ptr);

};

