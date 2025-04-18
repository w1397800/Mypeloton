//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// platform.h
//
// Identification: src/include/common/platform.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <thread>
#include <atomic>

#include <pthread.h>
//#include <immintrin.h>

#include "macros.h"


//===--------------------------------------------------------------------===//
// Platform-Specific Code
//===--------------------------------------------------------------------===//



template <typename T>
inline bool atomic_cas(T *object, T old_value, T new_value) {
  return __sync_bool_compare_and_swap(object, old_value, new_value);
}

template <typename T>
inline T atomic_add(T *object, T delta) {
  return __sync_fetch_and_add(object, delta);
}

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")


//===--------------------------------------------------------------------===//
// Alignment
//===--------------------------------------------------------------------===//

#define CACHELINE_SIZE 64  // XXX: don't assume x86

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))

//===--------------------------------------------------------------------===//
// Count the number of leading zeroes in a given 64-bit unsigned number
//===--------------------------------------------------------------------===//
static inline uint64_t CountLeadingZeroes(uint64_t i) {
#if defined __GNUC__ || defined __clang__
  return __builtin_clzl(i);
#else
#error get a better compiler
#endif
}

//===--------------------------------------------------------------------===//
// Find the next power of two higher than the provided value
//===--------------------------------------------------------------------===//
static inline uint64_t NextPowerOf2(uint64_t n) {
#if defined __GNUC__ || defined __clang__
  PELOTON_ASSERT(n > 0);
  return 1ul << (64 - CountLeadingZeroes(n - 1));
#else
  n--;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  return ++n;
#endif
}

//===--------------------------------------------------------------------===//
// Spinlock
//===--------------------------------------------------------------------===//

//enum LockState : bool { Unlocked = 0, Locked };
//
//class Spinlock {
// public:
//  Spinlock() : spin_lock_state(Unlocked) {}
//
//  inline void Lock() {
//    while (!TryLock()) {
//      _mm_pause();  // helps the cpu to detect busy-wait loop
//    }
//  }
//
//  bool IsLocked() { return spin_lock_state.load() == Locked; }
//
//  inline bool TryLock() {
//    // exchange returns the value before locking, thus we need
//    // to make sure the lock wasn't already in Locked state before
//    return spin_lock_state.exchange(Locked, std::memory_order_acquire) !=
//           Locked;
//  }
//
//  inline void Unlock() {
//    spin_lock_state.store(Unlocked, std::memory_order_release);
//  }
//
// private:
//  /*the exchange method on this atomic is compiled to a lockfree xchgl
//   * instruction*/
//  std::atomic<LockState> spin_lock_state;
//};
//

