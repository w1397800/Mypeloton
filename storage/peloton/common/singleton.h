//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// singleton.h
//
// Identification: src/include/common/singleton.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

//#include "storage/peloton/codegen/codegen.h"
#include "storage/peloton/common/macros.h"



template<typename T>
class Singleton {
 public:
  static T &Instance() {
    static T instance;
    return instance;
  }

 protected:
  Singleton() {}
  ~Singleton() {}

 private:
  DISALLOW_COPY_AND_MOVE(Singleton);
};
















