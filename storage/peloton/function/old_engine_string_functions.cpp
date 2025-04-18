//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// old_engine_string_functions.cpp
//
// Identification: src/function/old_engine_string_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/function/old_engine_string_functions.h"

#include <algorithm>
#include <cctype>
#include <string>

//#include "executor/executor_context.h"
#include "storage/peloton/function/string_functions.h"
#include "storage/peloton/type/value_factory.h"


// ASCII code of the first character of the argument.
//Value OldEngineStringFunctions::Ascii(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 1);
//  if (args[0].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::INTEGER);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  uint32_t ret = StringFunctions::Ascii(ctx, args[0].GetAs<const char *>(),
//                                        args[0].GetLength());
//  return ValueFactory::GetIntegerValue(ret);
//}
//
//Value OldEngineStringFunctions::Like(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 2);
//  if (args[0].IsNull() || args[1].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::INTEGER);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  bool ret = StringFunctions::Like(
//      ctx, args[0].GetAs<const char *>(), args[0].GetLength(),
//      args[1].GetAs<const char *>(), args[1].GetLength());
//  return ValueFactory::GetBooleanValue(ret);
//}

// Get Character from integer
Value OldEngineStringFunctions::Chr(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
  }
  int32_t val = args[0].GetAs<int32_t>();
  std::string str(1, static_cast<char>(val));
  return ValueFactory::GetVarcharValue(str);
}

// substring
Value OldEngineStringFunctions::Substr(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 3);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  int32_t from = args[1].GetAs<int32_t>() - 1;
  int32_t len = args[2].GetAs<int32_t>();

  return ValueFactory::GetVarcharValue(str.substr(from, len));
}

// Number of characters in string
Value OldEngineStringFunctions::CharLength(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::INTEGER);
  }
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (ValueFactory::GetIntegerValue(len));
}

// Concatenate two strings
Value OldEngineStringFunctions::Concat(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
  }
  std::string str = args[0].ToString() + args[1].ToString();
  return (ValueFactory::GetVarcharValue(str));
}

// Number of bytes in string
Value OldEngineStringFunctions::OctetLength(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 1);
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (ValueFactory::GetIntegerValue(len));
}

// Repeat string the specified number of times
Value OldEngineStringFunctions::Repeat(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  int32_t num = args[1].GetAs<int32_t>();

  std::string ret = "";

  while (num > 0) {
    if (num % 2) {
      ret += str;
    }
    if (num > 1) {
      str += str;
    }
    num >>= 1;
  }
  return (ValueFactory::GetVarcharValue(ret));
}

// Replace all occurrences in string of substring from with substring to
Value OldEngineStringFunctions::Replace(
    const std::vector<Value> &args) {
  PELOTON_ASSERT(args.size() == 3);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  std::string from = args[1].ToString();
  std::string to = args[2].ToString();
  size_t pos = 0;
  while ((pos = str.find(from, pos)) != std::string::npos) {
    str.replace(pos, from.length(), to);
    pos += to.length();
  }
  return (ValueFactory::GetVarcharValue(str));
}

// Remove the longest string containing only characters from characters
// from the start of string
//Value OldEngineStringFunctions::LTrim(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 2);
//  if (args[0].IsNull() || args[1].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  auto ret = StringFunctions::LTrim(
//      ctx, args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
//      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);
//
//  std::string str(ret.str, ret.length - 1);
//  return ValueFactory::GetVarcharValue(str);
//}
//
//// Remove the longest string containing only characters from characters
//// from the end of string
//Value OldEngineStringFunctions::RTrim(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 2);
//  if (args[0].IsNull() || args[1].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  auto ret = StringFunctions::RTrim(
//      ctx, args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
//      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);
//
//  std::string str(ret.str, ret.length - 1);
//  return ValueFactory::GetVarcharValue(str);
//}
//
//Value OldEngineStringFunctions::Trim(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 1);
//  return BTrim({args[0], ValueFactory::GetVarcharValue(" ")});
//}
//
//// Remove the longest string consisting only of characters in characters from
//// the start and end of string
//Value OldEngineStringFunctions::BTrim(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 2);
//  if (args[0].IsNull() || args[1].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::VARCHAR);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  auto ret = StringFunctions::BTrim(
//      ctx, args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
//      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);
//
//  std::string str(ret.str, ret.length - 1);
//  return ValueFactory::GetVarcharValue(str);
//}
//
//// The length of the string
//Value OldEngineStringFunctions::Length(
//    const std::vector<Value> &args) {
//  PELOTON_ASSERT(args.size() == 1);
//  if (args[0].IsNull()) {
//    return ValueFactory::GetNullValueByType(TypeId::INTEGER);
//  }
//
//  executor::ExecutorContext ctx{nullptr};
//  uint32_t ret = StringFunctions::Length(ctx, args[0].GetAs<const char *>(),
//                                         args[0].GetLength());
//  return ValueFactory::GetIntegerValue(ret);
//}

//Value OldEngineStringFunctions::Upper(
//    UNUSED_ATTRIBUTE const std::vector<Value> &args) {
////  throw Exception{"Upper not implemented in old engine"};
//}

//Value OldEngineStringFunctions::Lower(
//    UNUSED_ATTRIBUTE const std::vector<Value> &args) {
////  throw Exception{"Lower not implemented in old engine"};
//}

