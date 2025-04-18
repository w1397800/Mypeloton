//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ephemeral_pool.h
//
// Identification: src/include/type/ephemeral_pool.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>
#include <memory>

#include "storage/peloton/common/macros.h"
#include "storage/peloton/common/synchronization/spin_latch.h"
#include "storage/peloton/type/abstract_pool.h"


//===----------------------------------------------------------------------===//
//
// 原本在日志中，tuple 的构建靠的是 ephemeral_pool
// 不知道为啥 ephemeral_pool 里面的部分内容被注释掉了，导致日志和检查点内存泄漏
// 取消掉注释后，性能下降，且有其他地方发生了内存泄漏
// 所以这个 pool 其实就是 ephemeral_pool 的取消注释版，用于日志中正确的内存释放，
//===----------------------------------------------------------------------===//
class TupleValuePoll : public AbstractPool {
 public:
  TupleValuePoll() = default;

  ~TupleValuePoll();

  void *Allocate(size_t size) override;

  void Free(void *ptr) override;

 public:
  // Location list
  std::vector<std::unique_ptr<char>> locations_;

  // Spin lock protecting location list
  SpinLatch pool_lock_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

inline TupleValuePoll::~TupleValuePoll() {
//  pool_lock_.Lock();
locations_.clear();
//  pool_lock_.Unlock();
}

inline void *TupleValuePoll::Allocate(size_t size) {

//  pool_lock_.Lock();
  locations_.emplace_back(new char[size]);
  auto location = locations_.back().get();
//  pool_lock_.Unlock();

  return location;
}

inline void TupleValuePoll::Free(void *ptr) {
  //  pool_lock_.Lock();
  locations_.clear();
  //  pool_lock_.Unlock();
}
