//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// container_tuple.h
//
// Identification: src/include/common/container_tuple.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <numeric>
#include <sstream>
#include <vector>

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/abstract_tuple.h"
#include "storage/peloton/common/exception.h"
#include "storage/peloton/common/macros.h"
#include "storage/peloton/store/abstract_table.h"
#include "storage/peloton/store/tile_group.h"
#include "internal_types.h"
#include "storage/peloton/type/value.h"

/**
 * A generic wrapper around a templated "tuple" type
 *
 * @tparam T The actual container used to retrieve column values
 */
template <class T>
class ContainerTuple : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(T *container, uint tuple_id)
      : container_(container), tuple_id_(tuple_id) {}

  ContainerTuple(T *container, uint tuple_id,
                 const std::vector<uint> *column_ids)
      : container_(container), tuple_id_(tuple_id), column_ids_(column_ids) {}

  // Accessors
  T *GetContainer() const { return container_; }

  uint GetTupleId() const { return tuple_id_; }

  void SetValue(UNUSED_ATTRIBUTE uint column_id,
                UNUSED_ATTRIBUTE const Value &value) override {}

  Value GetValue(uint column_id) const override {
    //PELOTON_ASSERT(container_ != nullptr);
    return container_->GetValue(tuple_id_, column_id);
  }

  char *GetData() const override {
     throw NotImplementedException(
         "GetData() not supported for container tuples.");
  }

  /**
   * @brief Compute the hash value based on all valid columns and a given seed.
   *
   * @param seed The seed value to use as part of the hash computation
   */
  size_t HashCode(size_t seed = 0) const {
    if (column_ids_) {
      for (auto &column_itr : *column_ids_) {
        Value value = GetValue(column_itr);
        value.HashCombine(seed);
      }
    } else {
      uint column_count = container_->GetColumnCount();
      for (size_t column_itr = 0; column_itr < column_count; column_itr++) {
        Value value = GetValue(column_itr);
        value.HashCombine(seed);
      }
    }
    return seed;
  }

  /**
   * Compare whether this tuple equals to other value-wise. Assume the schema of
   * other tuple. Is the same as this. No check.
   *
   * @param other The tuple we're comparing against
   *
   * @return True if this tuple is equal to the provided tuple
   */
  bool EqualsNoSchemaCheck(const ContainerTuple<T> &other) const {
    if (column_ids_) {
      if (column_ids_->size() != other.column_ids_->size()) return false;
      for (size_t idx = 0; idx < column_ids_->size(); idx++) {
        Value lhs = (GetValue(column_ids_->at(idx)));
        Value rhs = (other.GetValue(other.column_ids_->at(idx)));
        if (lhs.CompareNotEquals(rhs) == CmpBool::CmpTrue) {
          return false;
        }
      }
    } else {
      uint column_count = container_->GetColumnCount();
      for (size_t column_itr = 0; column_itr < column_count; column_itr++) {
        Value lhs = (GetValue(column_itr));
        Value rhs = (other.GetValue(column_itr));
        if (lhs.CompareNotEquals(rhs) == CmpBool::CmpTrue) return false;
      }
    }
    return true;
  }

  const std::string GetInfo() const /*override*/ {
    std::stringstream os;
    os << "(";
    bool first = true;
    if (column_ids_) {
      for (const auto col_idx : *column_ids_) {
        if (!first) os << ",";
        first = false;
        os << GetValue(col_idx).GetInfo();
      }
    } else {
      uint column_count = container_->GetColumnCount();
      for (size_t column_itr = 0; column_itr < column_count; column_itr++) {
        if (!first) os << ",";
        first = false;
        os << GetValue(column_itr).GetInfo();
      }
    }

    os << ")";
    return os.str();
  }

 private:
  /// Underlying container behind this tuple interface
  T *container_;

  /// ID of tuple in tile group that this wrapper is pretending to be
  const uint tuple_id_;

  /// The ids of column that we're projecting in this tuple
  const std::vector<uint> *column_ids_ = nullptr;
};

/**
 * @brief A hashing functor for the ContainerTuple abstraction
 */
template <class T>
struct ContainerTupleHasher
    : std::unary_function<ContainerTuple<T>, std::size_t> {
  size_t operator()(const ContainerTuple<T> &tuple) const {
    return tuple.HashCode();
  }
};

/**
 * @brief Comparator functor for the ContainerTuple abstraction
 */
template <class T>
class ContainerTupleComparator {
 public:
  bool operator()(const ContainerTuple<T> &lhs,
                  const ContainerTuple<T> &rhs) const {
    return lhs.EqualsNoSchemaCheck(rhs);
  }
};

/**
 * @brief A convenient wrapper to interpret a vector of values as a tuple. No
 * need to construct a schema. The caller should make sure there's no
 * out-of-bound calls.
 */
template <>
class ContainerTuple<std::vector<Value>> : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(std::vector<Value> *container) : container_(container) {}

  Value GetValue(uint column_id) const /*override*/ {
    //PELOTON_ASSERT(container_ != nullptr);
    //PELOTON_ASSERT(column_id < container_->size());

    return (*container_)[column_id];
  }

  void SetValue(UNUSED_ATTRIBUTE uint column_id,
                UNUSED_ATTRIBUTE const Value &value) /*override*/ {}

//  char *GetData() const /*override*/ {
  char *GetData() const {
    // NOTE: We can't.Get a table tuple from a tilegroup or logical tile
    // without materializing it. So, this must not be used.
     throw NotImplementedException(
         "GetData() not supported for container tuples.");
  }

  size_t HashCode(size_t seed = 0) const {
    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      const Value value = GetValue(column_itr);
      value.HashCombine(seed);
    }
    return seed;
  }

  bool EqualsNoSchemaCheck(
      const ContainerTuple<std::vector<Value> > &other) const {
    //PELOTON_ASSERT(container_->size() == other.container_->size());

    for (size_t column_itr = 0; column_itr < container_->size(); column_itr++) {
      Value lhs = GetValue(column_itr);
      Value rhs = other.GetValue(column_itr);
      if (lhs.CompareNotEquals(rhs) == CmpBool::CmpTrue) return false;
    }
    return true;
  }

//  const std::string GetInfo() const /*override*/ {
  const std::string GetInfo() const {
    std::stringstream os;
    os << "(";
    bool first = true;
    for (const auto &val : *container_) {
      if (!first) os << ",";
      first = false;
      os << val.GetInfo();
    }
    os << ")";
    return os.str();
  }

 private:
  /// Underlying container behind this tuple interface
  const std::vector<Value> *container_ = nullptr;
};

template <>
class ContainerTuple<TileGroup> : public AbstractTuple {
 public:
  ContainerTuple(const ContainerTuple &) = default;
  ContainerTuple &operator=(const ContainerTuple &) = default;
  ContainerTuple(ContainerTuple &&) = default;
  ContainerTuple &operator=(ContainerTuple &&) = default;

  ContainerTuple(TileGroup *container, uint tuple_id)
      : container_(container), tuple_id_(tuple_id), column_ids_(nullptr) {}

  ContainerTuple(TileGroup *container, uint tuple_id,
                 const std::vector<uint> *column_ids)
      : container_(container), tuple_id_(tuple_id), column_ids_(column_ids) {}

  Value GetValue(uint column_id) const /*override*/ {
    //PELOTON_ASSERT(container_ != nullptr);
    auto col_id =
        (column_ids_ != nullptr ? column_ids_->at(column_id) : column_id);
    return container_->GetValue(tuple_id_, col_id);
  }

  void SetValue(uint column_id, const Value &value) /*override*/ {
    //PELOTON_ASSERT(container_ != nullptr);
    Value val = value.Copy();
    auto col_id =
        (column_ids_ != nullptr ? column_ids_->at(column_id) : column_id);
    container_->SetValue(val, tuple_id_, col_id);
  }

  void SetValueAllNew(Tuple *tuple, uint curr_thd_id, ItemPointer old_location) /*override*/ {
    uint tuple_id = (uint)tuple_id_;

    container_->CopyTupleNewForUpdate(tuple, tuple_id, curr_thd_id, old_location);//3.14

  }

  char *GetData() const /*override*/ {
    // NOTE: We can't get a tuple from a tilegroup or logical tile without
    //       materializing it. So, this must not be used.
     throw NotImplementedException(
         "GetData() not supported for container tuples.");
  }

  const std::string GetInfo() const /*override*/ {
    std::stringstream os;
    os << "(";
    bool first = true;
    if (column_ids_ != nullptr) {
      // Only print out columns we've projected
      for (const auto col_idx : *column_ids_) {
        if (!first) os << ",";
        first = false;
        os << container_->GetValue(tuple_id_, col_idx).GetInfo();
      }
    } else {
      const auto *schema = container_->GetAbstractTable()->GetSchema();
      for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount();
           col_idx++) {
        if (!first) os << ",";
        first = false;
        os << container_->GetValue(tuple_id_, col_idx).GetInfo();
      }
    }
    os << ")";
    return os.str();
  }

 private:
  /// Underlying container behind this tuple interface
  TileGroup *container_;

  /// ID of tuple in tile group that this wrapper is pretending to be
  const uint tuple_id_;

  /// The ids of column that we're projecting in this tuple
  const std::vector<uint> *column_ids_;
};

