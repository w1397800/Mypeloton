//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// spin_latch.h
//
// Identification: src/include/common/synchronization/spin_latch.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

//#include "common/platform.h"
#include "storage/peloton/common/macros.h"
//#include <xmmintrin.h>

//======================================
#ifndef RW_LOCK_H
#define RW_LOCK_H

#define ATOMIC_XADD(P, V) __sync_fetch_and_add((P), (V))
#define CMPXCHG(P, O, N) __sync_val_compare_and_swap((P), (O), (N))
#define ATOMIC_INC(P) __sync_add_and_fetch((P), 1)
#define ATOMIC_DEC(P) __sync_add_and_fetch((P), -1)
#define ATOMIC_ADD(P, V) __sync_add_and_fetch((P), (V))
#define ATOMIC_SET_BIT(P, V) __sync_or_and_fetch((P), 1 << (V))
#define ATOMIC_CLEAR_BIT(P, V) __sync_and_and_fetch((P), ~(1 << (V)))
const int ME_BUSY = 1;
/* Pause instruction to prevent excess processor bus usage */
#if defined(__x86_64__) || defined(__x86__)
#define CPU_RELAX() asm volatile("pause\n" : : : "memory")
#else                                                // SG taken from compiler.hh;; some web says yield:::memory
#define CPU_RELAX() asm volatile("" : : : "memory")  // equivalent to "rep; nop"
#endif

/* Compile read-write barrier */
#define BARRIER() asm volatile("" : : : "memory")
//======================================
//===--------------------------------------------------------------------===//
// Cheap & Easy Spin Latch
//===--------------------------------------------------------------------===//


enum class LatchState : bool { Unlocked = 0, Locked };

class SpinLatch {
 public:
  SpinLatch() : state_(LatchState::Unlocked) {}

//  void Lock() {
//    while (!TryLock()) {
////      _mm_pause();  // helps the cpu to detect busy-wait loop
//      _mm_pause();  // helps the cpu to detect busy-wait loop
////      __yield();
//    }
//  }
//
//  bool IsLocked() { return state_.load() == LatchState::Locked; }
//
//  bool TryLock() {
//    // exchange returns the value before locking, thus we need
//    // to make sure the lock wasn't already in Locked state before
//    return state_.exchange(LatchState::Locked, std::memory_order_acquire) !=
//           LatchState::Locked;
//  }
//
//  void Unlock() {
//    state_.store(LatchState::Unlocked, std::memory_order_release);
//  }



//======================================


  unsigned LockXchg32(void* ptr, unsigned x)
  {
#if defined(__x86_64__) || defined(__x86__)
    __asm__ __volatile__("xchgl %0,%1" : "=r"((unsigned)x) : "m"(*(volatile unsigned*)ptr), "0"(x) : "memory");
#else  // SG
    x = __atomic_exchange_n((unsigned*)ptr, x, __ATOMIC_SEQ_CST);
#endif
    return x;
  }


  void Lock()
  {
    while (1) {
      if (!LockXchg32(&m_lock, ME_BUSY)) {
        return;
      }

      while (m_lock) {
        CPU_RELAX();
      }
    }
  }

  void Unlock()
  {
    BARRIER();
    m_lock = 0;
  }

  int TryLock()
  {
    return LockXchg32(&m_lock, ME_BUSY);
  }
  //======================================

 private:
  /*the exchange method on this atomic is compiled to a lockfree xchgl
   * instruction*/
  std::atomic<LatchState> state_;

  //=========================
  typedef unsigned Spinlock;
  Spinlock m_lock = 0;
};

#endif /* RW_LOCK_H */
