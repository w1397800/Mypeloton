//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.h
//
// Identification: src/include/function/string_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include "storage/peloton/type/type.h"
#include "storage/peloton/type/abstract_pool.h"
class Type;

class AbstractPool;


class StringFunctions {
 public:
  struct StrWithLen {
    // Constructor
    StrWithLen(const char *str, uint32_t length) : str(str), length(length) {}

    // Member variables
    const char *str;
    uint32_t length;
  };


  /**
   * Compare two (potentially empty) strings returning an integer value
   * indicating their sort order.
   *
   * @param str1 A pointer to the first string
   * @param len1 The length of the first string
   * @param str2 A pointer to the second string
   * @param len2 The length of the second string
   * @return -1 if the first string is strictly less than the second; 0 if the
   * two strings are equal; 1 if the second string is strictly greater than the
   * second.
   */
  static int32_t CompareStrings(const char *str1, uint32_t len1,
                                const char *str2, uint32_t len2);

  /**
   * Write the provided variable length object into the target buffer.
   *
   * @param data The bytes we wish to serialize
   * @param len The length of the byte array
   * @param buf The target position we wish to write to
   * @param pool A memory pool to source memory from
   */
  static void WriteString(const char *data, uint32_t len, char *buf,
                          AbstractPool &pool);

};
