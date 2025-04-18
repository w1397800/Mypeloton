//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_factory.h
//
// Identification: src/include/storage/table_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TABLE_FACTORY
//#define TABLE_FACTORY
#include <string>

#include "storage/peloton/catalog//manager.h"
//#include "internal_types.h"
#include "data_table.h"
//#include "storage/temp_table.h"


/**
 * Magic Table Factory!!
 */
class TableFactory {
 public:
  /**
   * For a given Schema, instantiate a DataTable object and return it
   */
  static DataTable *GetDataTable(uint database_id, uint table_id,
                                 Schema *schema,
                                 std::string table_name,
                                 size_t tuples_per_tile_group_count,
                                 bool own_schema, bool adapt_table,
                                 bool is_catalog = false,
                                 LayoutType layout_type = LayoutType::ROW);

//  static TempTable *GetTempTable(Schema *schema, bool own_schema);

  /**
   * For a given table name, drop the table from database
   */
  static bool DropDataTable(uint database_oid, uint table_oid);
};

//#endif