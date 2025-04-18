//masstree

#include "index.h"
#include "masstree/masstree.hh"
#include "masstree/kvthread.hh"
//#include "masstree/kvthread.cc"
#include "masstree/masstree_tcursor.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_get.hh"
#include "masstree/masstree_split.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_scan.hh"
#include "masstree/str.hh"

//===----------------------------------------------------------------------===//
//
// An Masstree based index.
//
//===----------------------------------------------------------------------===//
class MassTreeIndex : public Index {
  friend class IndexFactory;

 public:
  struct alignas(32) default_table_params : public Masstree::nodeparams<15, 15> {
    typedef uint64_t value_type;
    typedef threadinfo threadinfo_type;
    typedef uint64_t value_print_type;
  };

  explicit MassTreeIndex(IndexMetadata *metadata);

  // Forward declare iterator class for later
  class Iterator;

  bool InsertEntry(const Tuple *key, ItemPointer *value) override;

  bool DeleteEntry(const Tuple *key, ItemPointer *value) override;

  bool CondInsertEntry(const Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;
//with tid 传入mysql线程id
  bool CondInsertEntry(const Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate,
                       uint curr_thd_id) override;
  /**
   * ArtIndex throws away the first three arguments and only uses the conjuncts
   * from the scan predicate.
   *
   * @param scan_predicate The only predicate that's actually used.
   * @param[out] result Where the results of the scan are stored
   */
  void Scan(const std::vector<Value> &values,
            const std::vector<uint> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ItemPointer *> &result,
            const ConjunctionScanPredicate *scan_predicate) override;

  void Scan(const std::vector<Value> &values,
            const std::vector<uint> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ItemPointer *> &result,
            const ConjunctionScanPredicate *scan_predicate,
            uint curr_thd_id) override;

  void PointGet(const std::vector<Value> &value_list,
                std::vector<ItemPointer *> &result, uint key_length,
                uint curr_thd_id) override;

  /**
   * ArtIndex throws away the first three arguments and only uses the conjuncts
   * from the scan predicate.
   *
   * @param scan_predicate The only parameter that's used
   * @param[out] result Where the results of the scan are stored
   * @param limit How many results to actually return
   * @param offset How many items to exclude from the results
   */
  void ScanLimit(const std::vector<Value> &values,
                 const std::vector<uint> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ItemPointer *> &result,
                 const ConjunctionScanPredicate *scan_predicate, uint64_t limit,
                 uint64_t offset) override;

  void ScanAllKeys(std::vector<ItemPointer *> &result) override;

  void ScanKey(const Tuple *key,
               std::vector<ItemPointer *> &result) override;

//with tid 传入mysql线程id
  void ScanKey(const std::vector<Value> &values,
               std::vector<ItemPointer *> &result, uint curr_thd_id, uint key_length,
               std::vector<uint> key_column_ids);

  void GetKey(const std::vector<Value> &values,
               std::vector<ItemPointer *> &result, uint curr_thd_id, uint key_length);

  bool isExist(Masstree::tcursor<default_table_params>::value_type value, uint scancount, 
                              std::vector<const void*> key,
               std::vector<uint> key_column_ids, TypeId typeId, uint ken);

  /// Return the index type
  std::string GetTypeName() const override {
    return IndexTypeToString(GetIndexMethodType());
  }

  // TODO(pmenon): Implement me
  size_t GetMemoryFootprint() override { return 0; }

  // TODO(pmenon): Implement me
  bool NeedGC() override { return false; }

  // TODO(pmenon): Implement me
  void PerformGC() override {}
  

  void init(){}

  void Unpack(uint data, uintptr_t* dest);
  bool PackKey(uint8_t* dest, uintptr_t src);

  bool PackStr(uint8_t* dest, uintptr_t src, size_t len, uint offset);
  void UnpackStr(uint8_t* data, uintptr_t* dest, size_t& len, uint offset);
  int char2uint(const char *input, uint8_t *output);
  uint GetAlign8Len(uint a);
  void makeTcursor(){}

//  typedef typename std::conditional<true, Masstree::forward_scan_helper, Masstree::reverse_scan_helper>::type helper_type;
//  bool Begin(Masstree::scanstackelt<default_table_params> m_stack,threadinfo& ti )
//  {
//    bool found = false;
//    int m_state;
//    typedef typename Masstree::node_base<default_table_params>::key_type key_type;
//    typedef typename  Masstree::node_base<default_table_params>::leafvalue_type leafvalue_type;
//    helper_type helperType;
//
////    typedef Masstree::scanstackelt<default_table_params> mystack_type;
//
//    (void)found;
//    (void)m_state;
//    (void)helperType;
//    (void)m_stack;
//    (void)ti;
////    (void)mystack_type;
//
//
//    const char* keybuf = "123";
//    key_type ka(keybuf, 3);
//    leafvalue_type entry = leafvalue_type::make_empty();
//    (void)ka;
//    (void)entry;
//
//
//    while (true) {
//      m_state = m_stack.find_initial(helperType, ka, true, entry, ti);
////      if (m_state != mystack_type::scan_down)
////        break;
////      ka.shift();
//    }
////    Next();
//
//    return found;
//  }

  /**
   * @brief Find the next key\value that satisfies the criteria.
   * @detail If (m_state == mystack_type::scan_emit), stay on the current key\value.
   * @return Current iterator scan state.
   */
//  uint32_t Next(void)
//  {
//    while (true) {
//      switch (m_state) {
//        case mystack_type::scan_emit:
//          m_searchKey->SetKeyLen(m_key.full_length());
//          goto done;
//
//        case mystack_type::scan_find_next:
//        find_next:
//          m_state = m_stack.find_next(m_helper, m_key, m_entry);
//          break;
//
//        case mystack_type::scan_up:
//          do {
//            // the scan is finished when the stack is empty
//            if (m_stack.node_stack_.empty()) {
//              m_done = true;
//              goto done;
//            }
//            m_stack.n_ = static_cast<leaf<P>*>(m_stack.node_stack_.back());
//            m_stack.node_stack_.pop_back();
//            m_stack.root_ = m_stack.node_stack_.back();
//            m_stack.node_stack_.pop_back();
//            m_key.unshift();
//          } while (unlikely(m_key.empty()));
//
//          m_stack.v_ = m_helper.stable(m_stack.n_, m_key);
//          m_stack.perm_ = m_stack.n_->permutation();
//          m_stack.ki_ = m_helper.lower(m_key, &m_stack);
//          goto find_next;
//
//        case mystack_type::scan_down:
//          m_helper.shift_clear(m_key);
//          goto retry;
//
//        case mystack_type::scan_retry:
//        retry:
//          m_state = m_stack.find_retry(m_helper, m_key, *m_ti);
//          break;
//      }
//    }
//    done:
//    return m_state;
//  }



 private:
  // Masstree
  //////art::Tree container_;
  struct scan_tester {
    const char * const *vbegin_, * const *vend_;
    char key_[32];
    int keylen_;
    bool reverse_;
    bool first_;
    scan_tester(const char * const *vbegin, const char * const *vend,
                bool reverse = false)
        : vbegin_(vbegin), vend_(vend), keylen_(0), reverse_(reverse),
          first_(true) {
      if (reverse_) {
        memset(key_, 255, sizeof(key_));
        keylen_ = sizeof(key_);
      }
    }
    template <typename SS, typename K>
    void visit_leaf(const SS&, const K&, threadinfo&) {
    }
    bool visit_value(Masstree::Str key, long unsigned int, threadinfo&) {
      memcpy(key_, key.s, key.len);
      keylen_ = key.len;
      const char *pos = (reverse_ ? vend_[-1] : vbegin_[0]);
      if ((int) strlen(pos) != key.len || memcmp(pos, key.s, key.len) != 0) {
        fprintf(stderr, "%sscan encountered %.*s, expected %s\n", reverse_ ? "r" : "", key.len, key.s, pos);
        assert((int) strlen(pos) == key.len && memcmp(pos, key.s, key.len) == 0);
      }
      fprintf(stderr, "%sscan %.*s\n", reverse_ ? "r" : "", key.len, key.s);
      (reverse_ ? --vend_ : ++vbegin_);
      first_ = false;
      return vbegin_ != vend_;
    }
    template <typename T>
    int scan(T& table, threadinfo& ti) {
      return table.table().scan(Masstree::Str(key_, keylen_), first_, *this, ti);
    }
    template <typename T>
    int rscan(T& table, threadinfo& ti) {
      return table.table().rscan(Masstree::Str(key_, keylen_), first_, *this, ti);
    }
  };

  Masstree::basic_table<default_table_params> mcontainer;

  threadinfo *masstree_threadinfo_main_ = nullptr;
  threadinfo *masstree_threadinfo_process_ = nullptr;
//  std::unordered_map<uint , threadinfo *> mtps;
//  std::unordered_map<uint , threadinfo *> mtps_read;
//  tbb::concurrent_unordered_map <uint , threadinfo *> mtps;
//  tbb::concurrent_unordered_map <uint , threadinfo *> mtps_read;
  std::vector<threadinfo *> mtps;
  std::vector<threadinfo *> mtps_read;
};
