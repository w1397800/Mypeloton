//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// epoch_manager_factory.cpp
//
// Identification: src/concurrency/epoch_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "storage/peloton/concurrency/transaction_manager_factory.h"
#include "storage/peloton/concurrency/epoch_manager_factory.h"

EpochType EpochManagerFactory::epoch_ = EpochType::DECENTRALIZED_EPOCH;

