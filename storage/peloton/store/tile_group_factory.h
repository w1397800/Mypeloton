//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_factory.h
//
// Identification: src/include/storage/tile_group_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TILE_GROUP_FACTORY
//#define TILE_GROUP_FACTORY
//#include "catalog/manager.h"
#include "abstract_table.h"
#include "tile_group.h"

#include <string>


/**
 * Super Awesome TileGroupFactory!!
 */
class TileGroupFactory {
 public:
  TileGroupFactory();
  virtual ~TileGroupFactory();

  static TileGroup *GetTileGroup(uint database_id, uint table_id,
                                 uint tile_group_id, AbstractTable *table,
                                 const std::vector<Schema> &schemas,
                                 std::shared_ptr<const Layout> layout,
                                 int tuple_count);
};

//#endif