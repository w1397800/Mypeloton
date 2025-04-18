//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// generic_key.h
//
// Identification: src/include/index/generic_key.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>

#include "storage/peloton/type/type_util.h"



/*
 * class GenericKey - Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instanciated
 * with a template argument.
 */
template <std::size_t KeySize>
class GenericKey {
 public:
  inline void SetFromKey(const Tuple *tuple) {
    PELOTON_ASSERT(tuple);
    PELOTON_MEMCPY(data, tuple->GetData(), tuple->GetLength());
    schema = tuple->GetSchema();
  }

  const Tuple GetTupleForComparison(
      const Schema *key_schema) {
    return Tuple(key_schema, data);
  }

  inline Value ToValue(const Schema *schema,
                             int column_id) const {
    const TypeId column_type = schema->GetType(column_id);
    const char *data_ptr = &data[schema->GetOffset(column_id)];
    const bool is_inlined = schema->IsInlined(column_id);
    return Value::DeserializeFrom(data_ptr, column_type, is_inlined);
  }

  inline const char *GetRawData(const Schema *schema,
                                int column_id) const {
    const char *data_ptr = &data[schema->GetOffset(column_id)];
    return (data_ptr);
  }

  /**
   * Generate a human-readable version of this GenericKey.
   * This method is slow (lots of memory copying) so you
   * don't want to execute this on the critical path of
   * anything real in the system.
   *
   * IMPORTANT: The output is based on the original tuple
   * schema and not the key schema. So you may end up seeing
   * attributes values that are not actually used in the index.
   *
   * @return
   */
  const std::string GetInfo() const {
    Tuple tuple(schema, data);
    return (tuple.GetInfo());
  }


  // actual location of data, extends past the end.
  char data[KeySize];

  const Schema *schema;
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    Value lhs_value;
    Value rhs_value;

    for (uint col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++) {
      const Value lhs_value = (lhs.ToValue(schema, col_itr));
      const Value rhs_value = (rhs.ToValue(schema, col_itr));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue) return true;

      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue)
        return false;
    }

    return false;
  }

  GenericComparator(const GenericComparator &) {}
  GenericComparator() {}
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class FastGenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    Value lhs_value;
    Value rhs_value;

    for (uint col_itr = 0; col_itr < schema->GetColumnCount(); col_itr++) {
      const char *lhs_data = lhs.GetRawData(schema, col_itr);
      const char *rhs_data = rhs.GetRawData(schema, col_itr);
      Type type = schema->GetType(col_itr);
      bool inlined = schema->IsInlined(col_itr);

      if (TypeUtil::CompareLessThanRaw(type, lhs_data, rhs_data,
                                             inlined) == CmpBool::CmpTrue)
        return true;
      else if (TypeUtil::CompareGreaterThanRaw(type, lhs_data, rhs_data,
                                                     inlined) == CmpBool::CmpTrue)
        return false;
    }

    return false;
  }

  FastGenericComparator(const FastGenericComparator &) {}
  FastGenericComparator() {}
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparatorRaw {
 public:
  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    for (uint column_itr = 0; column_itr < schema->GetColumnCount();
         column_itr++) {
      const Value lhs_value = (lhs.ToValue(schema, column_itr));
      const Value rhs_value = (rhs.ToValue(schema, column_itr));

      if (lhs_value.CompareLessThan(rhs_value) == CmpBool::CmpTrue)
        return VALUE_COMPARE_LESSTHAN;

      if (lhs_value.CompareGreaterThan(rhs_value) == CmpBool::CmpTrue)
        return VALUE_COMPARE_GREATERTHAN;
    }

    /* equal */
    return VALUE_COMPARE_EQUAL;
  }

  GenericComparatorRaw(const GenericComparatorRaw &) {}
  GenericComparatorRaw() {}
};

/**
 * Equality-checking function object
 */
template <std::size_t KeySize>
class GenericEqualityChecker {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  GenericEqualityChecker(const GenericEqualityChecker &) {}
  GenericEqualityChecker() {}
};

/**
 * Hash function object for an array of SlimValues
 */
template <std::size_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const {
    auto schema = p.schema;

    Tuple pTuple(schema);
    pTuple.MoveToTuple(reinterpret_cast<const void *>(&p));
    return pTuple.HashCode();
  }

  GenericHasher(const GenericHasher &) {}
  GenericHasher(){};
};

