//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.h
//
// Identification: src/include/storage/tile_group.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TILE_GROUP
//#define TILE_GROUP
#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/type/abstract_pool.h"
#include "layout.h"
//#include "tuple.h"
//#include "common/printable.h"
//#include "planner/project_info.h"
//#include "type/value.h"


//===--------------------------------------------------------------------===//
// Tile Group
//===--------------------------------------------------------------------===//

class Tuple;
//
class Tile;
//
class TileGroupHeader;
//
class AbstractTable;
//
//class TileGroupIterator;
//
//class RollbackSegment;

/**
 * Represents a group of tiles logically horizontally contiguous.
 *
 * < <Tile 1> <Tile 2> .. <Tile n> >
 *
 * Look at TileGroupHeader for MVCC implementation.
 *
 * TileGroups are only instantiated via TileGroupFactory.
 */
class TileGroup  {
    friend class Tile;
    friend class TileGroupFactory;
//    friend class gc::GCManager;

    TileGroup() = delete;

    TileGroup(TileGroup const &) = delete;

public:
    // Tile group constructor
    TileGroup(BackendType backend_type, TileGroupHeader *tile_group_header,
              AbstractTable *table, const std::vector<Schema> &schemas,
              std::shared_ptr<const Layout> layout, int tuple_count);

    ~TileGroup();

    //===--------------------------------------------------------------------===//
    // Operations
    //===--------------------------------------------------------------------===//

    // copy tuple in place.
    void CopyTuple(const Tuple *tuple, const uint &tuple_slot_id);

    void CopyTupleFromRecovery(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id);
    void CopyTupleNew(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id);
    void CopyTupleNewForUpdate(Tuple *tuple, uint &tuple_slot_id, uint curr_thd_id, ItemPointer old_location);
//    void CopyTupleNew(const Tuple *tuple, uint &tuple_slot_id);
    // insert tuple at next available slot in tile if a slot exists
    uint InsertTuple(const Tuple *tuple);
    uint InsertTupleNew(Tuple *tuple, uint curr_thd_id);
    uint InsertTupleFromRecovery(Tuple *tuple, uint curr_thd_id);

    // insert tuple at specific tuple slot
    // used by recovery mode
    uint InsertTupleFromRecovery(cid_t commit_id, uint tuple_slot_id,
                                  const Tuple *tuple);

    // insert tuple at specific tuple slot
    // used by recovery mode
    uint DeleteTupleFromRecovery(cid_t commit_id, uint tuple_slot_id);

    // insert tuple at specific tuple slot
    // used by recovery mode
    uint UpdateTupleFromRecovery(cid_t commit_id, uint tuple_slot_id,
                                  ItemPointer new_location);

    uint InsertTupleFromCheckpoint(uint tuple_slot_id, const Tuple *tuple,
                                   cid_t commit_id);

    //===--------------------------------------------------------------------===//
    // Utilities
    //===--------------------------------------------------------------------===//

    // Get a string representation for debugging
    const std::string GetInfo() const;

    uint GetNextTupleSlot() const;

    // this function is called only when building tile groups for aggregation
    // operations.
    // FIXME: GC has recycled some of the tuples, so this count is not accurate
    uint32_t GetActiveTupleCount() const;

    uint32_t GetAllocatedTupleCount() const { return num_tuple_slots_; }

    TileGroupHeader *GetHeader() const { return tile_group_header; }

    void SetHeader(TileGroupHeader *header) { tile_group_header = header; }

    unsigned int NumTiles() const { return tiles.size(); }

    // Get the tile at given offset in the tile group
    inline Tile *GetTile(const uint tile_offset) const {
        PELOTON_ASSERT(tile_offset < tile_count_);
        Tile *tile = tiles[tile_offset].get();
        return tile;
    }

    // Get a reference to the tile at the given offset in the tile group
    std::shared_ptr<Tile> GetTileReference(const uint tile_offset) const;

    uint GetTileId(const uint tile_id) const;

    AbstractPool *GetTilePool(const uint tile_id) const;

    uint GetTileGroupId() const;

    uint GetDatabaseId() const { return database_id; }

    uint GetTableId() const { return table_id; }

    AbstractTable *GetAbstractTable() const { return table; }

    void SetTileGroupId(uint tile_group_id_) { tile_group_id = tile_group_id_; }

    size_t GetTileCount() const { return tile_count_; }

    Value GetValue(uint tuple_id, uint column_id);
//    void GetAllValueAsBuffer(uchar *buf, uint tuple_id);

    void SetValue(Value &value, uint tuple_id, uint column_id);

    // Sync the contents
    void Sync();

    // Get the layout of the TileGroup. Used to locate columns.
    const Layout &GetLayout() const { return *tile_group_layout_; }

protected:
    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    // Catalog information
    uint database_id;
    uint table_id;
    uint tile_group_id;

    // Backend type
    BackendType backend_type;

    // set of tiles
    std::vector<std::shared_ptr<Tile>> tiles;

    // associated tile group
    TileGroupHeader *tile_group_header;

    // associated table
    AbstractTable *table;  // this design is fantastic!!!

    // number of tuple slots allocated
    uint32_t num_tuple_slots_;

    // number of tiles
    uint32_t tile_count_;

    std::mutex tile_group_mutex;

    // Refernce to the layout of the TileGroup
    std::shared_ptr<const Layout> tile_group_layout_;
};

//#endif