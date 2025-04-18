//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_factory.h
//
// Identification: src/include/executor/logical_tile_factory.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "storage/peloton/common/internal_types.h"

#include "storage/peloton/store/tile.h"
#include "storage/peloton/store/tile_group.h"

#include "storage/peloton/store/abstract_table.h"

class Tile;
class TileGroup;
class AbstractTable;

//===--------------------------------------------------------------------===//
// Logical Tile Factory
//===--------------------------------------------------------------------===//

class LogicalTile;

class LogicalTileFactory {
 public:
  static LogicalTile *GetTile();

  static LogicalTile *WrapTiles(
      const std::vector<std::shared_ptr<Tile>> &base_tile_refs);

  static LogicalTile *WrapTileGroup(
      const std::shared_ptr<TileGroup> &tile_group);
};
