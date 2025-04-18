//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager_factory.cpp
//
// Identification: src/concurrency/transaction_manager_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "storage/peloton/concurrency/transaction_manager_factory.h"
ProtocolType TransactionManagerFactory::protocol_ =
    ProtocolType::TIMESTAMP_ORDERING;
IsolationLevelType TransactionManagerFactory::isolation_level_ =
    IsolationLevelType::SERIALIZABLE;
ConflictAvoidanceType TransactionManagerFactory::conflict_avoidance_ =
    ConflictAvoidanceType::ABORT;
