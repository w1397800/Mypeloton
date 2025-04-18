//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// old_engine_string_functions.h
//
// Identification: src/include/function/old_engine_string_functions.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "storage/peloton/type/value.h"


class OldEngineStringFunctions {
 public:
  // ASCII code of the first character of the argument.
  static Value Ascii(const std::vector<Value> &args);

  // Like
  static Value Like(const std::vector<Value> &args);

  // Get Character from integer
  static Value Chr(const std::vector<Value> &args);

  // substring
  static Value Substr(const std::vector<Value> &args);

  // Number of characters in string
  static Value CharLength(const std::vector<Value> &args);

  // Concatenate two strings
  static Value Concat(const std::vector<Value> &args);

  // Number of bytes in string
  static Value OctetLength(const std::vector<Value> &args);

  // Repeat string the specified number of times
  static Value Repeat(const std::vector<Value> &args);

  // Replace all occurrences in string of substring from with substring to
  static Value Replace(const std::vector<Value> &args);

  // Remove the longest string containing only characters from characters
  // from the start of string
  static Value LTrim(const std::vector<Value> &args);

  // Remove the longest string containing only characters from characters
  // from the end of string
  static Value RTrim(const std::vector<Value> &args);

  static Value Trim(const std::vector<Value> &args);

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static Value BTrim(const std::vector<Value> &args);

  // Length will return the number of characters in the given string
  static Value Length(const std::vector<Value> &args);

  // Upper, Lower
  static Value Upper(const std::vector<Value> &args);
  static Value Lower(const std::vector<Value> &args);
};

