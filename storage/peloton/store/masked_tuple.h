//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// masked_tuple.h
//
// Identification: src/include/storage/masked_tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <sstream>

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/abstract_tuple.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/type/value.h"


//===--------------------------------------------------------------------===//
// MaskedTuple class
//===--------------------------------------------------------------------===//

/**
 * A 'MaskedTuple' is just a thin wrapper around a regular Tuple but it is
 * able to map columns to new offsets. This is to avoid having to make
 * copies of tuples just for index probes.
 */
class MaskedTuple : public AbstractTuple {
 public:
  inline MaskedTuple(AbstractTuple *rhs, const std::vector<uint> &mask)
      : tuple_(rhs), mask_(mask) {}

  ~MaskedTuple() {
      // We don't want to delete the AbstractTuple that we're pointing to
      // We don't own it. That's not on us!
  }

  inline void SetMask(const std::vector<uint> &mask) {
    // PELOTON_ASSERT(mask_ == nullptr);
    mask_ = mask;
  }

  inline Value GetValue(uint column_id) const {
    return (tuple_->GetValue(mask_[column_id]));
  }

  inline void SetValue(uint column_id, const Value &value) {
    // Not sure if we want to support this...
    tuple_->SetValue(mask_[column_id], value);
  }

  inline char *GetData() const { return (tuple_->GetData()); }

  const std::string GetInfo() const {
    std::stringstream os;
    os << "**MaskedTuple** ";
//    os << tuple_->GetInfo();
    return os.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // The real tuple that we are masking
  AbstractTuple *tuple_;

  // The length of this array has to be the same as the # of columns
  // in the underlying tuple schema.
  // MaskOffset -> RealOffset
  std::vector<uint> mask_;
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

