/* Copyright (c) 2004, 2021, Oracle and/or its affiliates.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file ha_peloton.h

    @brief
  The ha_peloton engine is a stubbed storage engine for peloton purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/peloton/ha_peloton.cc.

    @note
  Please read ha_peloton.cc before reading this file.
  Reminder: The peloton storage engine implements all methods that are
  *required* to be implemented. For a full list of all methods that you can
  implement, see handler.h.

   @see
  /sql/handler.h and /storage/peloton/ha_peloton.cc
*/
#pragma once
//#ifndef HA_PELOTON
//#define HA_PELOTON

//#include "table_factory.h"
#include <sys/types.h>

#include "my_base.h" /* ha_rows */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "sql/handler.h" /* handler */
#include "thr_lock.h"    /* THR_LOCK, THR_LOCK_DATA */
#include <unordered_map>
#include "storage/peloton/store/table_factory.h"
#include "storage/peloton/traffic_cop/traffic_cop.h"
#include "storage/peloton/common/container_tuple.h"
//#include "storage/peloton/common/thread_pool.h"
//extern ThreadPool thread_pool;
#include "storage/peloton/concurrency/epoch_manager_factory.h"
#include "storage/peloton/concurrency/transaction_manager_factory.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/store/storage_manager.h"

//jy ===================
#include "storage/peloton/index/index.h"
#include "storage/peloton/index/scan_optimizer.h"
#include "storage/peloton/store/logical_tile.h"
#include "storage/peloton/store/masked_tuple.h"
#include "storage/peloton/index/index_factory.h"

//jy ===================
//std::unordered_map<const char *, DataTable*> peloton_tables ;

std::unordered_map<std::string, DataTable*> peloton_tables ;
std::vector<std::string> all_index;

/** @brief
  Peloton_share is a class that will be shared among all open handlers.
  This peloton implements the minimum of what you will probably need.
*/
class Peloton_share : public Handler_share {
 public:
  THR_LOCK lock;
  Peloton_share();
  File table_file;
  ~Peloton_share() override { thr_lock_delete(&lock); }
};


/** @brief
  Class definition for the storage engine
*/
class ha_peloton : public handler {
  THR_LOCK_DATA lock;          ///< MySQL lock
  Peloton_share *share;        ///< Shared lock info
  Peloton_share *get_share();  ///< Get the share

  //wjh
  ulonglong int_table_flags;
  uint current_position = 0;
  char *data_file_name, *index_file_name;
  bool can_enable_indexes;
//  std::vector<std::vector<std::string>> current_tuples;//wjh
  //如果是主键, 则进行主键更新.
  uint table_tile_group_count_ = 0;
//  uint current_tile_group_offset_ = 0;
  DataTable *peloton_table;
  bool acquire_owner;
  bool for_update = false;
//  std::string curr_table_name;
//  TrafficCop *trafficCop;
//  bool is_first_stmt = true;
  //我发现, 静态函数和成员函数的ha_peloton实例是不一样的
  //因此我将以上两个变量删掉.
  //trafficCop放入(ha_peloton*)thd->get_ha_data(hton->slot)->ha_ptr;
  //is_first_stmt放入trafficCop
//  std::shared_ptr<TileGroup> current_tile_group;
  TileGroup *current_tile_group;
  uint current_tuple_id;
//  std::vector<uint> current_tuple_id_list;
  //wjh

  //jy ===================
//  uint index_result_pos;//记录索引查询结果 result_当前遍历的位置
//  std::vector<std::vector<std::string>> index_current_tuples; //

  /** @brief Result of index scan. */
//  std::vector<LogicalTile *> result_;

  std::vector<ItemPointer> visible_tuple_locations;//index_read, seq_scan

  /** @brief Result itr */
  //uint result_itr_ = INVALID_OID;

  /** @brief Computed the result */
  bool done_ = false;


  /** @brief index associated with index scan. */
  Index* index_;

  // the underlying table that the index is for
  DataTable *index_table_ = nullptr;

  // all columns to be returned as results
  std::vector<uint> column_ids_;

  // columns for key accesses.
  std::vector<uint> key_column_ids_;

  // expression types ( >, <, =, ...)
  std::vector<ExpressionType> expr_types_;

  // values for evaluation.
  std::vector<Value> values_;

  // // copy from underlying plan
  IndexScanPredicate index_predicate_;

  bool isFirstUseIndex = false;
//  bool isUseIndex = false;

//  std::vector<std::string> tuple_for_update;

  // std::vector<AbstractExpression *> runtime_keys_;

  bool key_ready_ = false;

  // whether the index scan range is left open
  bool left_open_ = false;

  // whether the index scan range is right open
  bool right_open_ = false;


  // whether it is an order by + limit plan
  bool limit_ = false;

  // how many tuples should be returned
  int64_t limit_number_ = 0;

  // offset means from which point
  int64_t limit_offset_ = 0;

  // whether order by is descending
  bool descend_ = false;

  bool is_idx_range = false;
  //jy
  //bool is_idx_pos = true;
  bool is_idx_pos = false;//是否是索引的点查询.
  bool is_seq_scan = false;//
  // jy ===================
// private:


 public:
  ha_peloton(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_peloton() override {}

  /** @brief
    The name that will be used for display purposes.
   */
  const char *table_type() const override { return "PELOTON"; }

  /**
    Replace key algorithm with one supported by SE, return the default key
    algorithm for SE if explicit key algorithm was not provided.

    @sa handler::adjust_index_algorithm().
  */
  enum ha_key_alg get_default_index_algorithm() const override {
//    return HA_KEY_ALG_HASH;
    //return HA_KEY_ALG_BTREE;
//jy
    return HA_KEY_ALG_SE_SPECIFIC;
  }
  bool is_index_algorithm_supported(enum ha_key_alg key_alg) const override {
    return key_alg == HA_KEY_ALG_HASH ||//wjh
           key_alg == HA_KEY_ALG_BTREE ||
           key_alg == HA_KEY_ALG_RTREE ||
           //jy
           key_alg == HA_KEY_ALG_SE_SPECIFIC
           ;
  }

  /** @brief
    This is a list of flags that indicate what functionality the storage engine
    implements. The current table flags are documented in handler.h
  */
  ulonglong table_flags() const override {
    /*
      We are saying that this engine is just statement capable to have
      an engine that can only handle statement-based logging. This is
      used in testing.
    */
    return int_table_flags;//wjh
  }

  /** @brief
    This is a bitmap of flags that indicates how the storage engine
    implements indexes. The current index flags are documented in
    handler.h. If you do not implement indexes, just return zero here.

      @details
    part is the key part to check. First key part is 0.
    If all_parts is set, MySQL wants to know the flags for the combined
    index, up to and including 'part'.
  */
  ulong index_flags(uint inx MY_ATTRIBUTE((unused)),
                    uint part MY_ATTRIBUTE((unused)),
                    bool all_parts MY_ATTRIBUTE((unused))) const override {

//    if (table_share->key_info[inx].algorithm == HA_KEY_ALG_FULLTEXT) return 0;

    ulong flags = HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE | HA_READ_ORDER |
                  HA_KEYREAD_ONLY | HA_DO_INDEX_COND_PUSHDOWN;

    // @todo: Check if spatial indexes really have all these properties
//    if (table_share->key_info[inx].flags & HA_SPATIAL)
//      flags |= HA_KEY_SCAN_NOT_ROR;
    return flags;
  }

  /** @brief
    unireg.cc will call max_supported_record_length(), max_supported_keys(),
    max_supported_key_parts(), uint max_supported_key_length()
    to make sure that the storage engine can handle the data it is about to
    send. Return *real* limits of your storage engine here; MySQL will do
    min(your_limits, MySQL_limits) automatically.
   */
  uint max_supported_record_length() const override {
    return HA_MAX_REC_LENGTH;
  }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_keys() const override { return 64; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.//最多可以有多少个列是索引.
   */
  uint max_supported_key_parts() const override { return 100; }

  /** @brief
    unireg.cc will call this to make sure that the storage engine can handle
    the data it is about to send. Return *real* limits of your storage engine
    here; MySQL will do min(your_limits, MySQL_limits) automatically.

      @details
    There is no need to implement ..._key_... methods if your engine doesn't
    support indexes.
   */
  uint max_supported_key_length() const override { return 1000; }
  uint max_supported_key_part_length(
      HA_CREATE_INFO *create_info MY_ATTRIBUTE((unused))) const override {
    return 1000;
  }
  /** @brief
    Called in test_quick_select to determine if indexes should be used.
  */
  double scan_time() override {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }

  /** @brief
    This method will never be called if you do not implement indexes.
  */
  double read_time(uint, uint, ha_rows rows) override {
    return (double)rows / 20.0 + 1;
  }

  /*
    Everything below are methods that we implement in ha_peloton.cc.

    Most of these methods are not obligatory, skip them and
    MySQL will treat them as not implemented
  */
  /** @brief
    We implement this in ha_peloton.cc; it's a required method.
  */
  int open(const char *name, int mode, uint test_if_locked,
           const dd::Table *table_def) override;  // required

  /** @brief
    We implement this in ha_peloton.cc; it's a required method.
  */
  int close(void) override;  // required

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int write_row(uchar *buf) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int update_row(const uchar *old_data, uchar *new_data) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int delete_row(const uchar *buf) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_next(uchar *buf) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_prev(uchar *buf) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_first(uchar *buf) override;

  /** @brief
    We implement this in ha_peloton.cc. It's not an obligatory method;
    skip it and and MySQL will treat it as not implemented.
  */
  int index_last(uchar *buf) override;

  int index_read_last_map(uchar *buf, const uchar *key,
                          key_part_map keypart_map) override;

  /** @brief
    Unlike index_init(), rnd_init() can be called two consecutive times
    without rnd_end() in between (it only makes sense if scan=1). In this
    case, the second call should prepare for the new table scan (e.g if
    rnd_init() allocates the cursor, the second call should position the
    cursor to the start of the table; no need to deallocate and allocate
    it again. This is a required method.
  */
  int start_stmt(THD *thd, thr_lock_type lock_type) override;
  int rnd_init(bool scan) override;  // required
  int rnd_end() override;
  int rnd_next(uchar *buf) override;             ///< required
  int rnd_pos(uchar *buf, uchar *pos) override;  ///< required
  void position(const uchar *record) override;   ///< required
  int info(uint) override;                       ///< required
  int extra(enum ha_extra_function operation) override;
//  int find_current_tile(std::vector<uint> column_ids_, std::vector<int> result_format);
  int find_current_tile();//wjh
  int external_lock(THD *thd, int lock_type) override;  ///< required
  int delete_all_rows(void) override;
  ha_rows records_in_range(uint inx, key_range *min_key,
                           key_range *max_key) override;
  int delete_table(const char *from, const dd::Table *table_def) override;
  int rename_table(const char *from, const char *to,
                   const dd::Table *from_table_def,
                   dd::Table *to_table_def) override;
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override;  ///< required

  THR_LOCK_DATA **store_lock(
      THD *thd, THR_LOCK_DATA **to,
      enum thr_lock_type lock_type) override;  ///< required

//  bool get_is_first_stmt(){return is_first_stmt;}
//  void set_is_first_stmt(bool flag){is_first_stmt = flag;}
//  TrafficCop *get_trafficCop(){return trafficCop;}
//  TrafficCop *set_trafficCop(TrafficCop *tr){return trafficCop = tr;}
  void set_update_value(AbstractTuple *dest, const AbstractTuple *old, bool setItself);
  void set_update_value_new(ContainerTuple<TileGroup> *dest, uchar* buf, ItemPointer old_location);

  int get_map_len(ulong map);

//jy ===================
  void create_index(bool is_primary, std::string table_name, std::string db_name);

  int index_init(uint keynr,  /*!< in: key (index) number */
                 bool sorted) override;/*!< in: 1 if result MUST be sorted
                                         according to index */
  int index_end() override;

  int index_next_same(uchar *buf, const uchar *key, uint keylen) override;

  int primary_index_scan();
//  int secondary_index_scan();
  uint lock_count(void) const override;
 

  void log_state_define();
  void run_log_module();
  bool prepare_log_file();
  void start_logging(/*std::thread &thread*/);
  void start_checkpoint(/*std::thread &thread*/);

//jy ===================

 private:
  bool PerformUpdatePrimaryKey(bool is_owner,
//                               TileGroup *tile_group,
                               ContainerTuple<TileGroup> old_tuple,
                               TileGroupHeader *tile_group_header,
                               uint current_tuple_id,
                               ItemPointer &old_location,
                               bool setItself);

  //用old_tuple里的主键值和table里的进行比较.
  bool IsPrimaryUpdate( const AbstractTuple *old);
  void GetAllValueAsBuffer(uchar *buf, ItemPointer current_location);

};
//#endif