//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_key.h
//
// Identification: src/include/index/index_key.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

//#include "storage/peloton/common/logger.h"
//#include "storage/peloton/common/macros.h"
#include "index.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/type/value_peeker.h"

#include <boost/functional/hash.hpp>

// There is nothing else to put in here expect the includes for
// the different index key implementations that we have

#include "compact_ints_key.h"
#include "generic_key.h"
#include "tuple_key.h"
