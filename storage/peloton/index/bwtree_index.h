//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.h
//
// Identification: src/include/index/bwtree_index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "storage/peloton/catalog/manager.h"
//#include "storage/peloton/common/platform.h"
#include "storage/peloton/common/internal_types.h"
#include "index.h"

#include "bwtree.h"

#define BWTREE_INDEX_TYPE BWTreeIndex <KeyType, \
                                       ValueType, \
                                       KeyComparator, \
                                       KeyEqualityChecker, \
                                       KeyHashFunc, \
                                       ValueEqualityChecker, \
                                       ValueHashFunc>

/**
 * BW tree-based index implementation.
 *
 * NOTE: The template argument list has two more entries, since
 * BwTree is implemented as a multimap, potentially there could
 * be many values mapped by a single key, and we need to distinguish
 * between values.
 *
 * @see Index
 */
template <typename KeyType,
          typename ValueType,
          typename KeyComparator,
          typename KeyEqualityChecker,
          typename KeyHashFunc,
          typename ValueEqualityChecker,
          typename ValueHashFunc>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  using MapType = BwTree<KeyType,
                         ValueType,
                         KeyComparator,
                         KeyEqualityChecker,
                         KeyHashFunc,
                         ValueEqualityChecker,
                         ValueHashFunc>;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const Tuple *key, ItemPointer *value) override;

  bool DeleteEntry(const Tuple *key, ItemPointer *value) override;

  bool CondInsertEntry(const Tuple *key,
                       ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;

  bool CondInsertEntry(const Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate,
                       uint curr_thd_id) override;

  void Scan(const std::vector<Value> &values,
            const std::vector<uint> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p) override;

  void Scan(const std::vector<Value> &value_list,
            const std::vector<uint> &tuple_column_id_list,
            const std::vector<ExpressionType> &expr_list,
            ScanDirectionType scan_direction,
            std::vector<ItemPointer *> &result,
            const ConjunctionScanPredicate *csp_p,
            uint curr_thd_id) override;

  void ScanLimit(const std::vector<Value> &values,
                 const std::vector<uint> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ValueType> &result,
                 const ConjunctionScanPredicate *csp_p,
                 uint64_t limit,
                 uint64_t offset) override;

  void ScanAllKeys(std::vector<ValueType> &result) override;

  void ScanKey(const Tuple *key,
               std::vector<ValueType> &result) override;

  std::string GetTypeName() const override;

  // TODO: Implement this
  size_t GetMemoryFootprint() override { return 0; }
  
  bool NeedGC() override {
    return container.NeedGarbageCollection();
  }

  void PerformGC() override {
    // LOG_INFO("Bw-Tree Garbage Collection!");
    container.PerformGarbageCollection();
    
    return;
  }

 protected:
  // equality checker and comparator
  KeyComparator comparator;
  KeyEqualityChecker equals;
  KeyHashFunc hash_func;
  
  // container
  MapType container;
};