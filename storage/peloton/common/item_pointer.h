
//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// item_pointer.h
//
// Identification: src/include/common/item_pointer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once
#include <cstdint>
//#ifndef ITEM_POINTER
//#define ITEM_POINTER
//#include "internal_types.h"
//#ifndef INTERNAL_TYPES_PELOTON
#include "internal_types.h"
//#endif

// logical physical location
class ItemPointer {
public:
    // block
    uint block;

    // 0-based offset within block
    uint offset;

    ItemPointer() : block(INVALID_OID), offset(INVALID_OID) {}

    ItemPointer(uint block, uint offset) : block(block), offset(offset) {}

    bool IsNull() const {
        return (block == INVALID_OID && offset == INVALID_OID);
    }

    bool operator<(const ItemPointer &rhs) const {
        if (block != rhs.block) {
            return block < rhs.block;
        } else {
            return offset < rhs.offset;
        }
    }

    bool operator==(const ItemPointer &rhs) const {
        return (block == rhs.block && offset == rhs.offset);
    }

} __attribute__((__aligned__(8))) __attribute__((__packed__));

extern ItemPointer INVALID_ITEMPOINTER;

class ItemPointerComparator {
public:
    bool operator()(ItemPointer *const &p1, ItemPointer *const &p2) const {
        return (p1->block == p2->block) && (p1->offset == p2->offset);
    }

    bool operator()(ItemPointer const &p1, ItemPointer const &p2) const {
        return (p1.block == p2.block) && (p1.offset == p2.offset);
    }

    ItemPointerComparator(const ItemPointerComparator &) {}

    ItemPointerComparator() {}
};

struct ItemPointerHasher {
    size_t operator()(const ItemPointer &item) const {
        // This constant is found in the CityHash code
        // [Source libcuckoo/default_hasher.hh]
        // std::hash returns the same number for unsigned int which causes
        // too many collisions in the Cuckoohash leading to too many collisions
        return (std::hash<uint>()(item.block) * 0x9ddfea08eb382d69ULL) ^
               std::hash<uint>()(item.offset);
    }
};

class ItemPointerHashFunc {
public:
    size_t operator()(ItemPointer *const &p) const {
        return (std::hash<uint>()(p->block) * 0x9ddfea08eb382d69ULL) ^
               std::hash<uint>()(p->offset);
    }

    ItemPointerHashFunc(const ItemPointerHashFunc &) {}

    ItemPointerHashFunc() {}
};

//bool AtomicUpdateItemPointer(ItemPointer *src_ptr, ItemPointer &value);
bool AtomicUpdateItemPointer(ItemPointer *src_ptr, const ItemPointer &value);

//#endif