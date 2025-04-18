//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logical_tile_factory.cpp
//
// Identification: src/executor/logical_tile_factory.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/peloton/store/logical_tile_factory.h"

#include "storage/peloton/store/logical_tile.h"
#include "storage/peloton/store/tile.h"
#include "storage/peloton/store/tile_group.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/store/tile_group_header.h"
#include "storage/peloton/common/internal_types.h"


/**
 * @brief Creates position list with the identity mapping.
 * @param size Size of position list.
 *
 * @return Position list.
 */
std::vector<uint> CreateIdentityPositionList(unsigned int size) {
  std::vector<uint> position_list(size);
  for (uint id = 0; id < size; id++) {
    position_list[id] = id;
  }
  return position_list;
}

/**
 * @brief Returns an empty logical tile.
 *
 * @return Pointer to empty logical tile.
 */
LogicalTile *LogicalTileFactory::GetTile() { return new LogicalTile(); }

/**
 * @brief Convenience method to construct a logical tile wrapping base tiles.
 * @param base_tiles Base tiles to be represented as a logical tile.
 *
 * @return Pointer to newly created logical tile.
 */
LogicalTile *LogicalTileFactory::WrapTiles(
    const std::vector<std::shared_ptr<Tile>> &base_tile_refs) {
  PELOTON_ASSERT(base_tile_refs.size() > 0);

  // TODO ASSERT all base tiles have the same height.
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  // First, we build a position list to be shared by all the tiles.
  const uint position_list_idx = 0;
  new_tile->AddPositionList(CreateIdentityPositionList(
      base_tile_refs[0].get()->GetActiveTupleCount()));

  for (unsigned int i = 0; i < base_tile_refs.size(); i++) {
    // Next, we construct the schema.
    int column_count = base_tile_refs[i].get()->GetColumnCount();
    for (int col_id = 0; col_id < column_count; col_id++) {
      new_tile->AddColumn(base_tile_refs[i], col_id, position_list_idx);
    }
  }

  // Drop reference because we created the base tile

  return new_tile.release();
}

/**
 * @brief Convenience method to construct a logical tile wrapping a tile group.
 * @param tile_group Tile group to be wrapped.
 *
 * @return Logical tile wrapping tile group.
 */
LogicalTile *LogicalTileFactory::WrapTileGroup(
    const std::shared_ptr<TileGroup> &tile_group) {
  std::unique_ptr<LogicalTile> new_tile(new LogicalTile());

  const int position_list_idx = 0;
  new_tile->AddPositionList(
      CreateIdentityPositionList(tile_group->GetActiveTupleCount()));

  // Construct schema.
  unsigned int num_tiles = tile_group->NumTiles();
  for (unsigned int i = 0; i < num_tiles; i++) {
    auto base_tile_ref = tile_group->GetTileReference(i);
    auto schema = base_tile_ref->GetSchema();
    for (uint col_id = 0; col_id < schema->GetColumnCount(); col_id++) {
      new_tile->AddColumn(base_tile_ref, col_id, position_list_idx);
    }
  }

  return new_tile.release();
}

