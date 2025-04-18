//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.h
//
// Identification: src/include/storage/database.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>

#include "storage/peloton/common/printable.h"
#include "storage/peloton/store/data_table.h"

struct ExecuteResult;
struct dirty_table_info;
struct dirty_index_info;


//===--------------------------------------------------------------------===//
// DATABASE
//===--------------------------------------------------------------------===//

class Database : public Printable {
 public:
  Database() = delete;
  Database(Database const &) = delete;

  Database(const uint database_oid);

  ~Database();

  //===--------------------------------------------------------------------===//
  // OPERATIONS
  //===--------------------------------------------------------------------===//

  uint GetOid() const { return database_oid; }

  //===--------------------------------------------------------------------===//
  // TABLE
  //===--------------------------------------------------------------------===//

  void AddTable(DataTable *table, bool is_catalog = false);

  DataTable *GetTable(const uint table_offset) const;

  // Throw CatalogException if such table is not found
  DataTable *GetTableWithOid(const uint table_oid) const;
  DataTable *GetTableWithName(const std::string table_name) const;

  uint GetTableCount() const;

  void DropTableWithOid(const uint table_oid);

  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
  std::string GetDBName();
  void setDBName(const std::string &database_name);

 protected:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // database oid
  const uint database_oid;

  // database name
  // TODO: deprecated, use
  // catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
  std::string database_name;

  // TABLES
  std::vector<DataTable *> tables;

  std::mutex database_mutex;
};
