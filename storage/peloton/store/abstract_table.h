//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.h
//
// Identification: src/include/storage/abstract_table.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef ABSTRACT_TABLE
//#define ABSTRACT_TABLE

#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <string>

//#include "internal_types.h"
#include "storage/peloton/common/item_pointer.h"
//#include "common/printable.h"
#include "layout.h"
//#include "schema.h"
#include "storage/peloton/concurrency/transaction_context.h"
//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


//}
//namespace concurrency {
//class TransactionContext;
//}
//
//namespace catalog {
//class Manager;
class Schema;


class Tuple;
class TileGroup;

/**
 * Base class for all tables
 */
class AbstractTable  {
 public:
  virtual ~AbstractTable();

 protected:
  // Table constructor
  AbstractTable(uint table_oid, Schema *schema, bool own_schema,
               LayoutType layout_type = LayoutType::ROW);

 public:
  //===--------------------------------------------------------------------===//
  // TUPLE OPERATIONS
  //===--------------------------------------------------------------------===//

  // insert tuple in table. the pointer to the index entry is returned as
  // index_entry_ptr.
  virtual ItemPointer InsertTuple(const Tuple *tuple,
                                  TransactionContext *transaction,
                                  ItemPointer **index_entry_ptr = nullptr,
                                  bool check_fk = true) = 0;

  // designed for tables without primary key. e.g., output table used by
  // aggregate_executor.
  virtual ItemPointer InsertTuple(const Tuple *tuple) = 0;

  //===--------------------------------------------------------------------===//
  // LAYOUT TYPE 
  //===--------------------------------------------------------------------===//

  void SetDefaultLayout(std::shared_ptr<const Layout> layout) {
    default_layout_ = layout;
  }

  std::shared_ptr<const Layout> GetDefaultLayout() { return default_layout_; }
  //===--------------------------------------------------------------------===//
  // TILE GROUP
  //===--------------------------------------------------------------------===//

  // Offset is a 0-based number local to the table
  virtual std::shared_ptr<TileGroup> GetTileGroup(
      const std::size_t &tile_group_offset) const = 0;

  // ID is the global identifier in the entire DBMS
  virtual std::shared_ptr<TileGroup> GetTileGroupById(
      const uint &tile_group_id) const = 0;

  // Number of TileGroups that the table has
  virtual size_t GetTileGroupCount() const = 0;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  uint GetOid() const { return table_oid; }

  void SetSchema(Schema *given_schema) { schema = given_schema; }

  Schema *GetSchema() const { return (schema); }

  virtual std::string GetName() const = 0;

  // Get a string representation for debugging
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  virtual void IncreaseTupleCount(const size_t &amount) = 0;

  virtual void DecreaseTupleCount(const size_t &amount) = 0;

  virtual void SetTupleCount(const size_t &num_tuples) = 0;

  virtual size_t GetTupleCount() const = 0;

 protected:
  //===--------------------------------------------------------------------===//
  // INTERNAL METHODS
  //===--------------------------------------------------------------------===//

  TileGroup *GetTileGroupWithLayout(uint database_id, uint tile_group_id,
                                    std::shared_ptr<const Layout> layout,
                                    const size_t num_tuples);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  uint table_oid;

  // table schema
  Schema *schema;

  /**
   * @brief Should this table own the schema?
   * Usually true.
   * Will be false if the table is for intermediate results within a query,
   * where the scheme may live longer.
   */
  bool own_schema_;

  // Default layout of the table
  std::shared_ptr<const Layout> default_layout_;
};

//#endif