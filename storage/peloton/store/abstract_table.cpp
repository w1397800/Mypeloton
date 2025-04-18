//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_table.cpp
//
// Identification: src/storage/abstract_table.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_table.h"

//#include "catalog/manager.h"
#include "storage/peloton/catalog/schema.h"
//#include "common/exception.h"
//#include "common/logger.h"
//#include "index/index.h"
#include "tile_group.h"
#include "tile_group_factory.h"
//#include "util/stringbox_util.h"


AbstractTable::AbstractTable(uint table_oid, Schema *schema,
                             bool own_schema, LayoutType layout_type)
    : table_oid(table_oid), schema(schema), own_schema_(own_schema) {
  // The default Layout should always be ROW or COLUMN
//  PELOTON_ASSERT((layout_type == LayoutType::ROW) ||
//                 (layout_type == LayoutType::COLUMN));
  default_layout_ = std::shared_ptr<const Layout>(
      new Layout(schema->GetColumnCount(), layout_type));
}

AbstractTable::~AbstractTable() {
  // clean up schema
  if (own_schema_) delete schema;
}

TileGroup *AbstractTable::GetTileGroupWithLayout(
    uint database_id, uint tile_group_id,
    std::shared_ptr<const Layout> layout, const size_t num_tuples) {
  // Populate the schema for each tile
  std::vector<Schema> schemas = layout->GetLayoutSchemas(schema);

  TileGroup *tile_group = TileGroupFactory::GetTileGroup(
      database_id, GetOid(), tile_group_id, this, schemas, layout, num_tuples);

  return tile_group;
}

const std::string AbstractTable::GetInfo() const {
//  std::ostringstream inner;
//  uint tile_group_count = this->GetTileGroupCount();
//  uint tuple_count = 0;
//  for (uint tile_group_itr = 0; tile_group_itr < tile_group_count;
//       tile_group_itr++) {
//    if (tile_group_itr > 0) inner << std::endl;
//
//    auto tile_group = this->GetTileGroup(tile_group_itr);
//    auto tile_tuple_count = tile_group->GetNextTupleSlot();
//
//    std::string tileData = tile_group->GetInfo();
//
//    inner << tileData;
//    tuple_count += tile_tuple_count;
//  }
//
//  std::ostringstream output;
//  output << "Table '" << GetName() << "' [";
//  output << "OID= " << GetOid() << ", ";
//  output << "NumTuples=" << tuple_count << ", ";
//  output << "NumTiles=" << tile_group_count << "]" << std::endl;
//  output << inner.str();
//
//  return output.str();
  return "info";
}
