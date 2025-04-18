//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// type_id.h
//
// Identification: src/include/type/type_id.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


// Every possible SQL type ID
enum class TypeId {
  INVALID = 0,
  PARAMETER_OFFSET,
  BOOLEAN,
  TINYINT,
  SMALLINT,
  INTEGER,
  BIGINT,
  DECIMAL,
  TIMESTAMP,
  DATE,
  CHAR,
  VARCHAR,
  VARBINARY,
  ARRAY,
  UDT
};

