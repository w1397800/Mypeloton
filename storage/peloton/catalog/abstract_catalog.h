//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.h
//
// Identification: src/include/catalog/abstract_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "catalog_defaults.h"
#include "schema.h"
#include "storage/peloton/concurrency/transaction_context.h"

class TransactionContext;



class LogicalTile;



//class AbstractExpression;



class Database;
class DataTable;
class Tuple;




class AbstractCatalog {
 public:
  virtual ~AbstractCatalog() {}

 protected:
  /* For pg_database, pg_table, pg_index, pg_column */
  AbstractCatalog(Database *pg_catalog,
                  Schema *catalog_table_schema,
                  uint catalog_table_oid,
                  std::string catalog_table_name);

  /* For other catalogs */
  AbstractCatalog(TransactionContext *txn,
                  const std::string &catalog_table_ddl);

  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//
  bool InsertTuple(TransactionContext *txn,
                   std::unique_ptr<Tuple> tuple);

  bool DeleteWithIndexScan(TransactionContext *txn,
                           uint index_offset,
                           std::vector<Value> values);

  std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
  GetResultWithIndexScan(
      TransactionContext *txn,
      std::vector<uint> column_offsets,
      uint index_offset,
      std::vector<Value> values) const;

  std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
  GetResultWithSeqScan(
      TransactionContext *txn,
      AbstractExpression *predicate,
      std::vector<uint> column_offsets);

  bool UpdateWithIndexScan(TransactionContext *txn,
                           uint index_offset,
                           std::vector<Value> scan_values,
                           std::vector<uint> update_columns,
                           std::vector<Value> update_values);

  void AddIndex(const std::string &index_name,
                uint index_oid,
                const std::vector<uint> &key_attrs,
                IndexConstraintType index_constraint);

  //===--------------------------------------------------------------------===//
  // Members
  //===--------------------------------------------------------------------===//

  // Maximum column name size for catalog schemas
  static const size_t max_name_size_ = 64;
  // which database catalog table is stored int
  uint database_oid_;
  // Local oid (without catalog type mask) starts from START_OID + OID_OFFSET
  std::atomic<uint> oid_ = ATOMIC_VAR_INIT(START_OID + OID_OFFSET);

  DataTable *catalog_table_;
};


