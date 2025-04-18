//
// Created by root on 2022/9/12.
//
#include "storage/peloton/gc/gc_manager.h"

#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/type/value.h"
#include "storage/peloton/type/abstract_pool.h"
#include "storage/peloton/store/tile.h"
#include "storage/peloton/store/tile_group.h"



// Check a tuple and reclaim all varlen field
void GCManager::CheckAndReclaimVarlenColumns(TileGroup *tile_group,
                                             uint tuple_id) {
  uint32_t tile_count = tile_group->GetTileCount();
  uint32_t tile_col_count;
  TypeId type_id;
  char *tuple_location;
  char *field_location;
  char *varlen_ptr;

  for (uint tile_itr = 0; tile_itr < tile_count; tile_itr++) {
    Tile *tile = tile_group->GetTile(tile_itr);
    PELOTON_ASSERT(tile);
    const Schema *schema = tile->GetSchema();
    tile_col_count = schema->GetColumnCount();
    for (uint tile_col_itr = 0; tile_col_itr < tile_col_count;
         ++tile_col_itr) {
      type_id = schema->GetType(tile_col_itr);

      if ((type_id != TypeId::VARCHAR &&
           type_id != TypeId::VARBINARY) ||
          (schema->IsInlined(tile_col_itr) == true)) {
        // Not of varlen type, or is inlined, skip
        continue;
      }
      // Get the raw varlen pointer
      tuple_location = tile->GetTupleLocation(tuple_id);
      field_location = tuple_location + schema->GetOffset(tile_col_itr);
      //varlen_ptr = Value::GetDataFromStorage(type_id, field_location);
      varlen_ptr = *reinterpret_cast<char **>(field_location);
//      // Call the corresponding varlen pool free
//      if (varlen_ptr != nullptr) {
//        //tile->GetPool()->Free(varlen_ptr);
//        //uint32_t actual_len = *reinterpret_cast<const uint32_t *>(varlen_ptr);
//        //if (actual_len != 0)
//          delete[] varlen_ptr;
//          varlen_ptr = nullptr;
//      }
    }
  }
}
