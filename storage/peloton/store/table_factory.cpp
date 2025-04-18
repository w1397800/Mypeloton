//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_factory.cpp
//
// Identification: src/storage/table_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "table_factory.h"

//#include "exception.h"
//#include "data_table.h"
#include "storage_manager.h"
//#include "storage/temp_table.h"



DataTable *TableFactory::GetDataTable(uint database_id, uint relation_id,
                                      Schema *schema,
                                      std::string table_name,
                                      size_t tuples_per_tilegroup_count,
                                      bool own_schema, bool adapt_table,
                                      bool is_catalog,
                                      LayoutType layout_type) {
  DataTable *table = new DataTable(schema, table_name, database_id, relation_id,
                                   tuples_per_tilegroup_count, own_schema,
                                   adapt_table, is_catalog, layout_type);

  return table;
}

//TempTable *TableFactory::GetTempTable(Schema *schema,
//                                      bool own_schema) {
//  TempTable *table = new TempTable(INVALID_OID, schema, own_schema);
//  return (table);
//}

bool TableFactory::DropDataTable(uint database_oid, uint table_oid) {
//  auto storage_manager = StorageManager::GetInstance();
//  try {
//    DataTable *table = (DataTable *)storage_manager->GetTableWithOid(
//        database_oid, table_oid);
//    delete table;
//  } catch (CatalogException &e) {
//    return false;
//  }
  (void)database_oid;
  (void)table_oid;
  return true;
}

