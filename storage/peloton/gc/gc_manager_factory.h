//
// Created by root on 2022/9/12.
//

#pragma once

#include "storage/peloton/gc/gc_manager.h"
#include "storage/peloton/gc/transaction_level_gc_manager.h"


class GCManagerFactory {
 public:
  static GCManager &GetInstance() {
    switch (gc_type_) {

      case GarbageCollectionType::ON:
        return TransactionLevelGCManager::GetInstance(gc_thread_count_);

      default:
        return GCManager::GetInstance();
    }
  }

  static void Configure(const int thread_count = 1) {
    if (thread_count == 0) {
      gc_type_ = GarbageCollectionType::OFF;
    } else {
      gc_type_ = GarbageCollectionType::ON;
      gc_thread_count_ = thread_count;
    }
  }

  static GarbageCollectionType GetGCType() { return gc_type_; }

 private:
  // GC type
  static GarbageCollectionType gc_type_;

  static int gc_thread_count_;
};
