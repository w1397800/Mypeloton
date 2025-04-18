//
// Created by root on 2022/9/12.
//
#include "storage/peloton/gc/gc_manager_factory.h"


GarbageCollectionType GCManagerFactory::gc_type_ = GarbageCollectionType::ON;

int GCManagerFactory::gc_thread_count_ = 1;


