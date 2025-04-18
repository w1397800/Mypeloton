//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile.h
//
// Identification: src/include/storage/tile.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
//#ifndef TILE
//#define TILE
#include <mutex>
#include "storage/peloton/catalog/schema.h"
#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/type/abstract_pool.h"
//#include "tuple.h"
#include "tile_group_header.h"
//#include "tuple_iterator.h"
#include "storage/peloton/catalog/manager.h"
//#include "common/printable.h"
//#include "type/serializeio.h"
//#include "type/serializer.h"
//#include "tile_group.h"


//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class Tuple;
class TileGroup;
class TileGroupHeader;
class TupleIterator;
/**
 * Represents a Tile.
 *
 * Tiles are only instantiated via TileFactory.
 *
 * NOTE: MVCC is implemented on the shared TileGroupHeader.
 */
class Tile {
  friend class TileFactory;
  friend class TileGroupHeader;
  friend class TupleIterator;
//  friend class gc::GCManager;

public:
    // Tile creator
    Tile(BackendType backend_type, TileGroupHeader *tile_header,
         const Schema &tuple_schema, TileGroup *tile_group,
         int tuple_count);

    virtual ~Tile();

    //===--------------------------------------------------------------------===//
    // Operations
    //===--------------------------------------------------------------------===//

    /**
     * Insert tuple at slot
     * NOTE : No checks, must be at valid slot.
     */
    void InsertTuple(const uint tuple_offset, Tuple *tuple);

    // allocated tuple slots
    uint GetAllocatedTupleCount() const { return num_tuple_slots; }

    // active tuple slots
    virtual uint GetActiveTupleCount() const;

    int GetTupleOffset(const char *tuple_address) const;

    int GetColumnOffset(const std::string &name) const;

    /**
     * Returns value present at slot
     */
    Value GetValue(const uint tuple_offset, const uint column_id);

    /*
     * Faster way to get value
     * By amortizing schema lookups
     */
    Value GetValueFast(const uint tuple_offset, const size_t column_offset,
                             const TypeId column_type,
                             const bool is_inlined);

    /**
     * Sets value at tuple slot.
     */
    void SetValue(const Value &value, const uint tuple_offset,
                  const uint column_id);

    /*
     * Faster way to set value
     * By amortizing schema lookups
     */
    void SetValueFast(const Value &value, const uint tuple_offset,
                      const size_t column_offset, const bool is_inlined,
                      const size_t column_length);

    // Get tuple at location
    static Tuple *GetTuple(Manager *catalog,
                           const ItemPointer *tuple_location);

    // Copy current tile in given backend and return new tile
    Tile *CopyTile(BackendType backend_type);

    //===--------------------------------------------------------------------===//
    // Size Stats
    //===--------------------------------------------------------------------===//

    // Only inlined data
    uint32_t GetInlinedSize() const { return tile_size; }

    int64_t GetUninlinedDataSize() const { return uninlined_data_size; }

    // Both inlined and uninlined data
    uint32_t GetSize() const { return tile_size + uninlined_data_size; }

    //===--------------------------------------------------------------------===//
    // Columns
    //===--------------------------------------------------------------------===//

    const Schema *GetSchema() const { return &schema; };

    const std::string GetColumnName(const uint column_index) const {
        return schema.GetColumn(column_index).GetName();
    }

    inline uint GetColumnCount() const { return column_count; };

    inline TileGroupHeader *GetHeader() const { return tile_group_header; }

    inline TileGroup *GetTileGroup() const { return tile_group; }

    uint GetTileId() const { return tile_id; }

    // Compare two tiles
    bool operator==(const Tile &other) const;

    bool operator!=(const Tile &other) const;

    TupleIterator GetIterator();

    // Get a string representation for debugging
    const std::string GetInfo() const;

    //===--------------------------------------------------------------------===//
    // Serialization/Deserialization
    //===--------------------------------------------------------------------===//

    bool SerializeTo(SerializeOutput &output, uint num_tuples);

    bool SerializeHeaderTo(SerializeOutput &output);

    bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples,
                           int num_tuples);

    void DeserializeTuplesFrom(SerializeInput &serialize_in,
                               AbstractPool *pool = nullptr);

    void DeserializeTuplesFromWithoutHeader(SerializeInput &input,
                                            AbstractPool *pool = nullptr);

    AbstractPool *GetPool() { return (pool); }

    char *GetTupleLocation(const uint tuple_offset) const;
    char *GetTupleLocation_nvm(const uint tuple_offset) const;

    // Sync the contents
    void Sync();
//wjh
//  std::vector<std::vector<std::string>> GetAllValuesAsStrings(
//      const std::vector<int> &result_format, bool use_to_string_null);
protected:
//   private:
    //===--------------------------------------------------------------------===//
    // Data members
    //===--------------------------------------------------------------------===//

    // Catalog information
    uint database_id;
    uint table_id;
    uint tile_group_id;
    uint tile_id;

    // backend type
    BackendType backend_type;

    // tile schema
    Schema schema;

    // set of fixed-length tuple slots
    char *data;
    char *data_nvm;

    // relevant tile group
    TileGroup *tile_group;

    // storage pool for uninlined data
    AbstractPool *pool;

    // number of tuple slots allocated
    uint num_tuple_slots;

    // number of columns
    uint column_count;

    // length of tile tuple
    size_t tuple_length;

    // space occupied by inlined data (tile size)
    size_t tile_size;

    // space occupied by uninlined data
    size_t uninlined_data_size;

    // Used for serialization/deserialization
    char *column_header;

    uint column_header_size;

    /**
     * NOTE : Tiles don't keep track of number of occupied slots.
     * This is maintained by shared Tile Header.
     */
    TileGroupHeader *tile_group_header;
};

// Returns a pointer to the tuple requested. No checks are done that the index
// is valid.
inline char *Tile::GetTupleLocation(const uint tuple_offset) const {
    char *tuple_location = data + (tuple_offset * tuple_length);

    return tuple_location;
}

inline char *Tile::GetTupleLocation_nvm(const uint tuple_offset) const {
    char *tuple_location = data_nvm + (tuple_offset * tuple_length);

    return tuple_location;
}

// Finds index of tuple for a given tuple address.
// Returns -1 if no matching tuple was found
inline int Tile::GetTupleOffset(const char *tuple_address) const {
    // check if address within tile bounds
    if ((tuple_address < data) || (tuple_address >= (data + tile_size)))
        return -1;

    int tuple_id = 0;

    // check if address is at an offset that is an integral multiple of tuple
    // length
    tuple_id = (tuple_address - data) / tuple_length;

    if (tuple_id * tuple_length + data == tuple_address) return tuple_id;

    return -1;
}

//===--------------------------------------------------------------------===//
// Tile factory
//===--------------------------------------------------------------------===//

class TileFactory {
public:
    TileFactory();

    virtual ~TileFactory();

    // Creates tile that is not attached to a tile group.
    // For use in the executor.
    static Tile *GetTempTile(const Schema &schema, int tuple_count) {
        // These temporary tiles don't belong to any tile group.
        TileGroupHeader *header = nullptr;
        TileGroup *tile_group = nullptr;

        Tile *tile = GetTile(BackendType::MM, INVALID_OID, INVALID_OID, INVALID_OID,
                             INVALID_OID, header, schema, tile_group, tuple_count);

        return tile;
    }

    static Tile *GetTile(BackendType backend_type, uint database_id,
                         uint table_id, uint tile_group_id, uint tile_id,
                         TileGroupHeader *tile_header,
                         const Schema &schema, TileGroup *tile_group,
                         int tuple_count) {
        Tile *tile =
                new Tile(backend_type, tile_header, schema, tile_group, tuple_count);

        TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
                                schema);

        return tile;
    }

private:
    static void InitCommon(Tile *tile, uint database_id, uint table_id,
                           uint tile_group_id, uint tile_id,
                           const Schema &schema) {
        tile->database_id = database_id;
        tile->table_id = table_id;
        tile->tile_group_id = tile_group_id;
        tile->tile_id = tile_id;
        tile->schema = schema;
    }
};

//#endif