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

/**
  @file ha_peloton.cc

  @brief
  The ha_peloton engine is a stubbed storage engine for peloton purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/peloton/ha_peloton.h.

  @details
  ha_peloton will let you create/open/delete tables, but
  nothing further (for peloton, indexes are not supported nor can data
  be stored in the table). Use this peloton as a template for
  implementing the same functionality in your own storage engine. You
  can enable the peloton storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-peloton-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE \<table name\> (...) ENGINE=PELOTON;

  The peloton storage engine is set up to use table locks. It
  implements an peloton "SHARE" that is inserted into a hash by table
  name. You can use this to store information of state that any
  peloton handler object will be able to see when it is using that
  table.

  Please read the object definition in ha_peloton.h before reading the rest
  of this file.

  @note
  When you create an PELOTON table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an peloton select that would do a scan of an entire
  table:

  @code
  ha_peloton::store_lock
  ha_peloton::external_lock
  ha_peloton::info
  ha_peloton::rnd_init
  ha_peloton::extra
  ha_peloton::rnd_next
  ha_peloton::extra
  ha_peloton::external_lock
  ha_peloton::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Here you see that the peloton storage engine has 9 rows called before
  rnd_next signals that it has reached the end of its data. Also note that
  the table in question was already opened; had it not been open, a call to
  ha_peloton::open() would also have been necessary. Calls to
  ha_peloton::extra() are hints as to what will be occurring to the request.

  A Longer Peloton can be found called the "Skeleton Engine" which can be
  found on TangentOrg. It has both an engine and a full build environment
  for building a pluggable storage engine.

  Happy coding!<br>
    -Brian
*/
#ifndef HA_PELOTON_CC
#define HA_PELOTON_CC

#include <math.h>
#include <stdio.h>

#include "storage/peloton/ha_peloton.h"
#include "my_dbug.h"
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin.h"
#include "typelib.h"
#include "sql/table.h"
#include "sql/field.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/type/value_factory.h"
#include "storage/peloton/store/logical_tile.h"
#include "storage/peloton/store/logical_tile_factory.h"
#include "sql/log.h"
#include "storage/peloton/logging/logging_util.h"
#include "storage/peloton/type/value_factory.h"
#include "storage/peloton/concurrency/transaction_manager_factory.h"
#include "storage/peloton/gc/gc_manager_factory.h"

#include "storage/peloton/common/exception.h"
#include "storage/peloton/logging/log_manager.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/logging/logger_configuration.h"
//#include "storage/peloton/common/timer.h"

//#include "storage/peloton/catalog/catalog.h"
#include "storage/peloton/logging/checkpoint_manager.h"
//=============


#endif
static handler *peloton_create_handler(handlerton *hton, TABLE_SHARE *table,
                                       bool partitioned, MEM_ROOT *mem_root);

handlerton *peloton_hton;
using TcopTxnState = std::pair<TransactionContext *, ResultType>;

//ThreadPool thread_pool;
/* Interface to mysqld, to check system tables supported by SE */
static bool peloton_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table);
static int peloton_rollback(handlerton *, THD * , bool rollback_trx);
static int peloton_commit(handlerton *, THD *thd, bool commit_trx);
// static int peloton_discover(handlerton *, THD *thd, const char *db,
//                                const char *name, uchar **frmblob,
//                                size_t *frmlen);

Peloton_share::Peloton_share() { thr_lock_init(&lock); }

int init_log_module();
static int peloton_init_func(void *p) {//@wjh
  DBUG_TRACE;

  peloton_hton = (handlerton *)p;
  peloton_hton->state = SHOW_OPTION_YES;
  peloton_hton->create = peloton_create_handler;
  peloton_hton->flags = HTON_CAN_RECREATE;
  peloton_hton->is_supported_system_table = peloton_is_supported_system_table;

  peloton_hton->commit = peloton_commit;
  peloton_hton->rollback = peloton_rollback;

  // peloton_hton->discover = peloton_discover;
//(void) peloton_commit;
//(void) peloton_rollback;
  //wjh
  /*
   * 初始化MyPeloton,
   * 初始化MyPeloton线程池
   * 启动epoch线程
   * 垃圾回收线程
   * handlerton函数指针的赋值
   */

  // start epoch.
  EpochManagerFactory::GetInstance().StartEpoch();

  for (size_t task_id = 0; task_id < TASK_NO; task_id++) {
    EpochManagerFactory::GetInstance().RegisterThread(task_id);
  }
  StorageManager::InitMemPool();

  // LOGGING (TODO: 这也太粗暴了
  Database *database = new Database(ABC_DATABASE_INDEX);
  auto storage_manager = StorageManager::GetInstance();
  database->setDBName("abc");//此为故障恢复的数据库.
  storage_manager->AddDatabaseToStorageManager(database);
  // Initialize catalog
  // auto pg_catalog = Catalog::GetInstance();
  // pg_catalog->Bootstrap();  // Additional catalogs
  // SettingsManager::GetInstance().InitializeCatalog();


  if (init_log_module() != 0) {
    sql_print_error("failed to init log module");
    return 1;
  }
  
  //======

  // start GC.
  // GCManagerFactory::Configure(settings::SettingsManager::GetInt(settings::SettingId::gc_num_threads));
  GCManagerFactory::Configure(GC_THD_COUNT);
  GCManagerFactory::GetInstance().StartGC();

  return 0;
}

static int peloton_discover(handlerton *, THD *thd, const char *db,
                               const char *name, uchar **frmblob,
                               size_t *frmlen) {
  (void) thd;
  (void) db;
  (void) name;

  *frmlen = 24;
  *frmblob = (uchar *)my_malloc(PSI_NOT_INSTRUMENTED, *frmlen, MYF(0));

  return 0;
}

//logger module ==========================================

int init_checkpoint_and_logger(LogManagerOptions& options) {
  auto& log_manager = LogManager::GetInstance();

  // start checkpoint mode
  if (options.enable_chkpt) {
    auto& checkpoint_manager = CheckpointManager::GetInstance();

    checkpoint_manager.StartStandbyMode();
    checkpoint_manager.WaitForModeTransition(CheckpointStatus::STANDBY, true);
  }

  // start logging mode
  CHECK(options.enable_logging);
  log_manager.StartStandbyMode(); //will create frontend logger
  log_manager.WaitForModeTransition(LoggingStatusType::STANDBY, true);

  return 0;
}

int do_recovery(LogManagerOptions& options) {
  auto& log_manager = LogManager::GetInstance();
  log_manager.PrepareRecovery();
  log_manager.RecoveryDDL();

  // checkpoint recovery
  sql_print_warning("Checkpoint recovery start");
  chrono::steady_clock::time_point start = chrono::steady_clock::now();
  if (options.enable_chkpt) {
    auto& checkpoint_manager = CheckpointManager::GetInstance();
    checkpoint_manager.StartRecoveryMode();
    checkpoint_manager.WaitForModeTransition(CheckpointStatus::DONE_RECOVERY, true);
  }
  chrono::steady_clock::time_point end = chrono::steady_clock::now();
  chrono::duration<double> dur = end - start;
  sql_print_warning("Checkpoint recovery Done, cost %lf", dur.count());

  // logging recovery
  start = end;
  sql_print_warning("Log recovery start");
  CHECK(options.enable_logging);
  log_manager.StartRecoveryMode();
  log_manager.WaitForModeTransition(LoggingStatusType::STANDBY, true);
  end = chrono::steady_clock::now();
  dur = end - start;
  sql_print_warning("Log Recovery Done, cost %lf", dur.count());

  // start checkpointing mode after recovery
  if (options.enable_chkpt) {
    auto& checkpoint_manager = CheckpointManager::GetInstance();

    if (!checkpoint_manager.IsInCheckpointingMode()) {
      checkpoint_manager.SetCheckpointStatus(CheckpointStatus::CHECKPOINTING);
    }
  }

  return 0;
}

// Main Entry Point
int init_log_module() {
  LogManagerOptions options;

  if (!options.enable_logging) {
    return 0;
  }

  auto& log_manager = LogManager::GetInstance();
  if (log_manager.Init(options) != 0) {
    return 1;
  }

  if (init_checkpoint_and_logger(options) != 0) {
    return 1;
  }

  return do_recovery(options);
}

//==end logger module ==========================================

// 当表被锁住的时候, 会走这里, 但是具体的逻辑还不太清晰.
int ha_peloton::start_stmt(THD *thd, thr_lock_type lock_type) {
  // start a tx @wjh
//  my_thread_id thread_id = thd->thread_id();
//  TransactionContext *txn;
//  auto &txn_manager = TransactionManagerFactory::GetInstance();
//  txn = txn_manager.BeginTransaction(thread_id);
//  (void)txn;
  (void)thd;
  (void)lock_type;

  return 0;
}

/**
  @brief
  Peloton of simple lock controls. The "share" it creates is a
  structure we will pass to each peloton handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Peloton_share *ha_peloton::get_share() {
  Peloton_share *tmp_share;

  DBUG_TRACE;

  lock_shared_ha_data();
  if (!(tmp_share = static_cast<Peloton_share *>(get_ha_share_ptr()))) {
    tmp_share = new Peloton_share;
    if (!tmp_share) goto err;

    set_ha_share_ptr(static_cast<Handler_share *>(tmp_share));
  }
  err:
  unlock_shared_ha_data();
  return tmp_share;
}

static handler *peloton_create_handler(handlerton *hton, TABLE_SHARE *table,
                                       bool, MEM_ROOT *mem_root) {
  return new (mem_root) ha_peloton(hton, table);
}
int count_a = 0;
vector<int> thd_b ;

ha_peloton::ha_peloton(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      int_table_flags(
          HA_NULL_IN_KEY | HA_CAN_FULLTEXT | HA_CAN_SQL_HANDLER |
          HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE | HA_DUPLICATE_POS |
          HA_CAN_INDEX_BLOBS | HA_AUTO_PART_KEY  |
          HA_CAN_GEOMETRY | HA_CAN_BIT_FIELD |
          HA_CAN_RTREEKEYS | HA_COUNT_ROWS_INSTANT | HA_STATS_RECORDS_IS_EXACT |
          HA_CAN_REPAIR | HA_GENERATED_COLUMNS | HA_ATTACHABLE_TRX_COMPATIBLE |
          HA_SUPPORTS_DEFAULT_EXPRESSION),
      can_enable_indexes(true) {

  if(current_thd->get_ha_data(hton->slot)->ha_ptr == 0x0){
    TrafficCop *trafficCop = new TrafficCop();
    //测试用
//    count_a++;
//    if(count_a == 15){
//      count_a = 15;
//    }
//    thd_b.push_back(current_thd->thread_id());
    //
    current_thd->get_ha_data(hton->slot)->ha_ptr = trafficCop;
  }

}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_handler_tablename ha_peloton_system_tables[] = {
    {(const char *)nullptr, (const char *)nullptr}};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @retval true   Given db.table_name is supported system table.
  @retval false  Given db.table_name is not a supported system table.
*/
static bool peloton_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table) {
  st_handler_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table) return false;

  // Check if this is SE layer system tables
  systab = ha_peloton_system_tables;
  while (systab && systab->db) {
    if (systab->db == db && strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}

/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_peloton::open(const char *name, int, uint, const dd::Table *) {
  DBUG_TRACE;
  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
  trafficCop->set_curr_table_name(name);//暂时只给创建二级索引的时候使用.

//  if (!(share = get_share())) return 1;
//  thr_lock_data_init(&share->lock, &lock, nullptr);

  return 0;
}

/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_peloton::close(void) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Peloton of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an peloton of extracting all of the data as strings.
  ha_berekly.cc has an peloton of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

//int ha_peloton::write_row(uchar *buf) {//wjh w
//  DBUG_TRACE;
//  /*
//    Peloton of a successful write_row. We don't store the data
//    anywhere; they are thrown away. A real implementation will
//    probably need to do something with 'buf'. We report a success
//    here, to pretend that the insert was successful.
//  */
//
//  //通过value的值和类型, 得到一个value
//  std::string table_name1(table->s->table_name.str);
//  std::string table_name("./abc/");
//  table_name = table_name+table_name1;
//  DataTable *pt = peloton_tables.at(table_name);//todo 如果为空则返回表不存在
//
//  if(pt->GetInit_buf() == 9999){
//    pt->SetInit_buf((uint)*buf);
//  }
//  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
//  auto schema = pt->GetSchema();
//  std::unique_ptr<Tuple> storage_tuple ;
//  storage_tuple.reset(new Tuple(schema, true));
//
//  uint16 i = pt->GetMove_pos();//先移动初始偏移量.
//
//  storage_tuple->SetDataAddress((char*)(buf+i));//1119
////  char *tuple_data = storage_tuple->GetData();
////  tuple_data = (char*)buf;
//
////==================old insert========================
////  char attribute_buffer[1024];
////  String attribute(attribute_buffer, sizeof(attribute_buffer), &my_charset_bin);
////  // Set tuple to point to temporary project tuple
////  int col_id = 0;
////  for (Field **field = table->field; *field; field++,col_id++) {
////    Value value;
////    String rowBuffer;
////    switch ((*field)->type()) {
//////      case MYSQL_TYPE_BOOL:
//////        Value value = ValueFactory::GetBooleanValue((*field).);
//////        break;
////      case MYSQL_TYPE_TINY:
////        value = ValueFactory::GetTinyIntValue((int8_t)(*field)->val_int());
////        break;
////      case MYSQL_TYPE_SHORT:
////        value = ValueFactory::GetSmallIntValue((int16_t)(*field)->val_int());
////        break;
////      case MYSQL_TYPE_INT24:
////        value = ValueFactory::GetIntegerValue((int)(*field)->val_int());
////        break;
////      case MYSQL_TYPE_LONG:
////        value = ValueFactory::GetBigIntValue((long)(*field)->val_int());
////        break;
////      case MYSQL_TYPE_NEWDECIMAL:
////      case MYSQL_TYPE_DOUBLE:
////        value = ValueFactory::GetDecimalValue((*field)->val_real());
////        break;
////      case MYSQL_TYPE_FLOAT:
////        value = ValueFactory::GetDecimalValue((*field)->val_real());
////        break;
////      case MYSQL_TYPE_TIMESTAMP:
////        //todo 时间戳类型的实现
//////        value = ValueFactory::GetTimestampValue((*field)->val_date_temporal());
//////        value = ValueFactory::GetTimestampValue(5783201669025000000);
//////        value = ValueFactory::GetTimestampValue(0);
////        value = ValueFactory::GetVarcharValue("2021-09-20 14:47:18");
////        break;
////      case MYSQL_TYPE_DATE:
////        value = ValueFactory::GetDateValue((*field)->val_date_temporal());
////        break;
////      case MYSQL_TYPE_STRING:
////      case MYSQL_TYPE_VARCHAR:{
////        (*field)->val_str(&attribute, &attribute);
////        value = ValueFactory::GetVarcharValue(attribute.ptr(),attribute.length()+1, false);
////        break;
////      }
////      case MYSQL_TYPE_INVALID:
////        break;
////      default:
////        break;
////    }
////
////    storage_tuple->SetValue(col_id, value,nullptr);
////  }
//  //==================old insert========================
//
//
////  uint column_count = schema->GetColumnCount();
////  uint offset = 0;
////  vector<uint> buf_offset;
////  buf_offset.push_back(0);//最开始的offset为0
////  for (uint column_id = 0; column_id < column_count; column_id++) {
//////      Value val = (tuple->GetValue(column_itr));
//////      tile_tuple.SetValue(tile_column_itr, val, tile->GetPool());
////
////    const Column &column = schema->GetColumn(column_id);
////    auto typeId = column.GetType();
////    //src是buf那边根据偏移量得到的值. int和float类型是4, double, char是8.
////    //varchar现有1位或2位, 然后是具体的值. src只需要传入具体的值即可
////
////    if (TypeId::VARCHAR == typeId) {
////      if(column.GetLength()<86){
////        offset+=1;
////      }else {
////        offset+=2;
////      }
////      offset += column.GetLength()*3;
////      buf_offset.push_back(offset);
////   } else if(TypeId::CHAR == typeId){
////
////      offset += column.GetLength()*3;
////      buf_offset.push_back(offset);
////  } else {
////      offset += column.GetLength();
////      buf_offset.push_back(offset);
////    }
////  }
////  storage_tuple->SetBuf_offset(buf_offset);
//
//
//
////  const Tuple *tuple = storage_tuple.get();
//  Tuple *tuple = storage_tuple.get();
//
//  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
//  auto &curr_state = tx_stack.top();
//  auto current_txn = curr_state.first;
//
//  ItemPointer *index_entry_ptr = nullptr;
////  ItemPointer location;
////  new std::thread(&running_test_thread1);
////  new std::thread(&test123);
//
//  (void)tuple;
//  (void)current_txn;
//  (void)index_entry_ptr;
//
//
////  ItemPointer location = pt->InsertTuple(tuple, current_txn, &index_entry_ptr);
//  ItemPointer location = pt->InsertTupleNew(tuple, current_txn, &index_entry_ptr);
//
//  auto &transaction_manager = TransactionManagerFactory::GetInstance();
//
//  if (location.block == INVALID_OID) {
//    transaction_manager.SetTransactionResult(current_txn,
//                                             ResultType::FAILURE);
//    return HA_ERR_FOUND_DUPP_KEY;//应该还有别的错误, 姑且先返回这个
//  }
//
//  transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);
//  (void)location;
//  (void)buf;
//  return 0;
//}


int ha_peloton::write_row(uchar *buf) {//wjh w
  DBUG_TRACE;
  /*
    Peloton of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */

  //通过value的值和类型, 得到一个value
  std::string db_name(table->s->db.str);
 std::string table_name1(table->s->table_name.str);

 std::string table_name("./"+db_name+"/");

 table_name = table_name+table_name1;
 //gsy ====
 // DataTable *pt = peloton_tables.at(table_name);//todo 如果为空则返回表不存在
 auto storageManager = StorageManager::GetInstance();
// std::string db_name = "abc";

 Database *db = storageManager->GetDatabaseWithName(db_name);
 DataTable *pt = db->GetTableWithName(table_name);
//=====
// //  通过value的值和类型, 得到一个value
//   std::string table_name1(table->s->table_name.str);
//   std::string table_name("./abc/");
//   table_name = table_name+table_name1;
//   DataTable *pt = peloton_tables.at(table_name);//todo 如果为空则返回表不存在

  if(pt->GetInit_buf() == 9999){
    pt->SetInit_buf((uint)*buf);
  }
  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
  auto schema = pt->GetSchema();
  std::unique_ptr<Tuple> storage_tuple ;
  storage_tuple.reset(new Tuple(schema, false));

  uint16 i = pt->GetMove_pos();//先移动初始偏移量.

  storage_tuple->SetDataAddress((char*)(buf+i));//1119

  Tuple *tuple = storage_tuple.get();

  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
  auto &curr_state = tx_stack.top();
  auto current_txn = curr_state.first;

  ItemPointer *index_entry_ptr = nullptr;

  (void)tuple;
  (void)current_txn;
  (void)index_entry_ptr;

  uint curr_thd_id = current_thd->thread_id();
//  ItemPointer location = pt->InsertTuple(tuple, current_txn, curr_thd_id, &index_entry_ptr);
  ItemPointer location = pt->InsertTupleNew(tuple, current_txn, curr_thd_id, &index_entry_ptr);

  auto &transaction_manager = TransactionManagerFactory::GetInstance();

  if (location.block == INVALID_OID) {
    transaction_manager.SetTransactionResult(current_txn,
                                             ResultType::FAILURE);
    return HA_ERR_FOUND_DUPP_KEY;//应该还有别的错误, 姑且先返回这个
  }
  storage_tuple = nullptr;
  tuple = nullptr;//手动释放内存
  transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);
//  stats.records++;
  (void)location;
  (void)buf;
  return 0;
}

/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for peloton by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_peloton::update_row(const uchar *, uchar *new_row) {
  DBUG_TRACE;

  (void)new_row;
  PELOTON_ASSERT(peloton_table);

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;

  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
  auto &curr_state = tx_stack.top();
  auto current_txn = curr_state.first;


  // Delete each tuple
  if (current_position == 0) {
    current_position = 1;
  }
  ItemPointer current_location = visible_tuple_locations.at(current_position-1);
  uint current_tuple_id = current_location.offset;
  auto current_tile_group = StorageManager::GetInstance()->GetTileGroup(current_location.block).get();
  TileGroupHeader *tile_group_header = current_tile_group->GetHeader();

  ItemPointer old_location(current_tile_group->GetTileGroupId(), current_tuple_id);

  ContainerTuple<TileGroup> old_tuple(current_tile_group, current_tuple_id);


  bool is_owner = transaction_manager.IsOwner(current_txn, tile_group_header,
                                              current_tuple_id);

  bool is_written = transaction_manager.IsWritten(
      current_txn, tile_group_header, current_tuple_id);

  // Prepare to examine primary key
  bool ret = false;
  bool priamry_update = IsPrimaryUpdate(&old_tuple);
//  const UpdatePlan &update_node = GetPlanNode<UpdatePlan>();

  // if the current transaction is the creator of this version.
  // which means the current transaction has already updated the version.
  if (is_owner == true && is_written == true) {
//    if (update_node.GetUpdatePrimaryKey()) {
    if (priamry_update) {//todo 如何判断是否是主键更新
      // Update primary key
//      ret = PerformUpdatePrimaryKey(is_owner, current_tile_group.get(), tile_group_header,
      ret = PerformUpdatePrimaryKey(is_owner, old_tuple, tile_group_header,
                                    current_tuple_id, old_location, true);

      if (ret == false) {
        return false;
      }
    }
      // Normal update (no primary key)
    else {


      set_update_value(&old_tuple, &old_tuple, true);

      transaction_manager.PerformUpdate(current_txn, old_location);
      // we do not need to add any item pointer to statement-level write set
      // here, because we do not generate any new version
    }
  }
    // if we have already obtained the ownership
  else {
    // Skip the IsOwnable and AcquireOwnership if we have already got the
    // ownership
    bool is_ownable = is_owner ||
                      transaction_manager.IsOwnable(
                          current_txn, tile_group_header, current_tuple_id);

    if (is_ownable == true) {
      // if the tuple is not owned by any transaction and is visible to
      // current transaction.

      bool acquire_ownership_success =
          is_owner ||
          transaction_manager.AcquireOwnership(current_txn, tile_group_header,
                                               current_tuple_id);

      if (acquire_ownership_success == false) {
//        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return false;
      }

//      if (update_node.GetUpdatePrimaryKey()) {
      if (priamry_update) {//todo 如何判断是否是主键更新
        // Update primary key
        ret = PerformUpdatePrimaryKey(is_owner, old_tuple, tile_group_header,
//        ret = PerformUpdatePrimaryKey(is_owner, current_tile_group.get(), tile_group_header,
                                      current_tuple_id, old_location, false);

//        if (ret == true) {
//          executor_context_->num_processed += 1;  // updated one
//        }
//          // When fail, ownership release is done inside PerformUpdatePrimaryKey
//        else {
//          return false;
//        }
        if (ret == false) {
          return false;
        }
      }

        // Normal update (no primary key)
      else {
        // if it is the latest version and not locked by other threads, then
        // insert a new version.

        // acquire a version slot from the table.
        uint curr_thd_id = current_thd->thread_id();
        ItemPointer new_location = peloton_table->AcquireVersion(curr_thd_id);

        auto storage_manager = StorageManager::GetInstance();
        auto new_tile_group = storage_manager->GetTileGroup(new_location.block);

        ContainerTuple<TileGroup> new_tuple(new_tile_group.get(),
                                            new_location.offset);

//        ContainerTuple<TileGroup> old_tuple(current_tile_group.get(),
//        ContainerTuple<TileGroup> old_tuple(current_tile_group,
//                                            current_tuple_id);

        // perform projection from old version to new version.
        // this triggers in-place update, and we do not need to allocate
        // another version.
        //更新的赋值
//        project_info_->Evaluate(&new_tuple, &old_tuple, nullptr,
//                                executor_context_);
        //更新的赋值重新实现一下.
        set_update_value_new(&new_tuple, new_row, old_location);

        // get indirection.
        ItemPointer *indirection =
            tile_group_header->GetIndirection(old_location.offset);
        // finally install new version into the table
        ret = peloton_table->InstallVersion(&new_tuple,
                                            0,
            // &(project_info_->GetTargetList()),
            //todo 之后要找一个结构代替targetlist, 并且对接二级索引.

                                            current_txn, indirection);

        // PerformUpdate() will not be executed if the insertion failed.
        // There is a write lock acquired, but since it is not in the write
        // set,
        // because we haven't yet put them into the write set.
        // the acquired lock can't be released when the txn is aborted.
        // the YieldOwnership() function helps us release the acquired write
        // lock.
        if (ret == false) {
//          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          if (is_owner == false) {
            // If the ownership is acquire inside this update executor, we
            // release it here
            transaction_manager.YieldOwnership(current_txn, tile_group_header,
                                               current_tuple_id);
          }
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        transaction_manager.PerformUpdate(current_txn, old_location,
                                          new_location);

      }
    } else {
      // transaction should be aborted as we cannot update the latest version.
//      LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(current_txn,
                                               ResultType::FAILURE);
      return false;
    }
  }
  return 0;
//  return HA_ERR_WRONG_COMMAND;
}

bool ha_peloton::PerformUpdatePrimaryKey(
    bool is_owner, ContainerTuple<TileGroup> old_tuple,
    TileGroupHeader *tile_group_header, uint physical_tuple_id,
    ItemPointer &old_location, bool setItself) {

  auto &transaction_manager = TransactionManagerFactory::GetInstance();
  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
  auto &curr_state = tx_stack.top();
  auto current_txn = curr_state.first;

  ///////////////////////////////////////
  // Delete tuple/version chain
  ///////////////////////////////////////
  ItemPointer new_location = peloton_table->InsertEmptyVersion();

  // PerformUpdate() will not be executed if the insertion failed.
  // There is a write lock acquired, but since it is not in the write
  // set,
  // because we haven't yet put them into the write set.
  // the acquired lock can't be released when the txn is aborted.
  // the YieldOwnership() function helps us release the acquired write
  // lock.
  if (new_location.IsNull() == true) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    if (is_owner == false) {
      // If the ownership is acquire inside this update executor, we
      // release it here
      transaction_manager.YieldOwnership(current_txn, tile_group_header,
                                         physical_tuple_id);
    }
    transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
    return false;
  }
  transaction_manager.PerformDelete(current_txn, old_location, new_location);
//  statement_write_set_.insert(new_location);

  ////////////////////////////////////////////
  // Insert tuple rather than install version
  ////////////////////////////////////////////
  auto target_table_schema = peloton_table->GetSchema();

  Tuple new_tuple(target_table_schema, true);

//  ContainerTuple<TileGroup> old_tuple(tile_group, physical_tuple_id);

  set_update_value(&new_tuple, &old_tuple, setItself);
//  set_update_value_new(&new_tuple, , setItself);

//  project_info_->Evaluate(&new_tuple, &old_tuple, nullptr, executor_context_);

  // insert tuple into the table.
  ItemPointer *index_entry_ptr = nullptr;
  ItemPointer location = peloton_table->InsertTuple(&new_tuple, current_txn, &index_entry_ptr);

  // it is possible that some concurrent transactions have inserted the
  // same tuple. In this case, abort the transaction.
  if (location.block == INVALID_OID) {
    transaction_manager.SetTransactionResult(current_txn,
                                             ResultType::FAILURE);
//    transaction_manager.SetTransactionResult(current_txn,
//                                             ResultType::FAILURE_AFTER_DELETE);


    current_txn->SetDelLocation(old_location);
    return false;//走到这里, 需要回滚PerformDelete的数据.然后把删除的location记下来
    //把location放到txn里面

  }

  // Check the source table of any foreign key constraint
//  if (peloton_table->GetSchema()->HasForeignKeySources()) {
//    Tuple prev_tuple(target_table_schema, true);
//    // Get a copy of the old tuple
//    for (uint column_itr = 0; column_itr < target_table_schema->GetColumnCount(); column_itr++) {
//      Value val = (old_tuple.GetValue(column_itr));
//      prev_tuple.SetValue(column_itr, val, executor_context_->GetPool());
//    }

//    if (peloton_table->CheckForeignKeySrcAndCascade(&prev_tuple,
//                                                    &new_tuple,
//                                                    current_txn,
//                                                    executor_context_,
//                                                    true) == false)
//    {
//      transaction_manager.SetTransactionResult(current_txn,
//                                               ResultType::FAILURE);
//      return false;
//    }
//  }

  transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);
//  statement_write_set_.insert(location);
  return true;
}

void ha_peloton::set_update_value_new(ContainerTuple<TileGroup> *dest, uchar* buf, ItemPointer old_location){

  std::string db_name(table->s->db.str);
  std::string table_name("./"+db_name+"/");
  std::string table_name1(table->s->table_name.str);
  table_name = table_name+table_name1;
  //gsy ===
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithName(db_name);
  DataTable *pt = db->GetTableWithName(table_name);
  //DataTable *pt = peloton_tables.at(table_name);//todo 如果为空则返回表不存在

  if(pt->GetInit_buf() == 9999){
    pt->SetInit_buf((uint)*buf);
  }

  auto schema = pt->GetSchema();
  std::unique_ptr<Tuple> storage_tuple ;
  storage_tuple.reset(new Tuple(schema, false));

  uint16 i = pt->GetMove_pos();//先移动初始偏移量.

  storage_tuple->SetDataAddress((char*)(buf+i));//1119
  Tuple *tuple = storage_tuple.get();
  uint curr_thd_id = current_thd->thread_id();

  dest->SetValueAllNew(tuple, curr_thd_id, old_location);
  storage_tuple = nullptr;
  tuple = nullptr;
}

void ha_peloton::set_update_value(AbstractTuple *dest,
                                  const AbstractTuple *old, bool setItself){
  char attribute_buffer[1024];
  (void)old;
  (void)setItself;
  int col_id = 0;
  String attribute(attribute_buffer, sizeof(attribute_buffer), &my_charset_bin);
  for (Field **field = table->field; *field; field++,col_id++) {
    Value value;
//    String rowBuffer;
//    (*field)->val_str(&attribute, &attribute);
//    if(attribute.ptr() != old->GetValue(col_id).GetData()){
      switch ((*field)->type()) {
        case MYSQL_TYPE_TINY:
          value = ValueFactory::GetTinyIntValue((int8_t)(*field)->val_int());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_SHORT:
          value = ValueFactory::GetSmallIntValue((int16_t)(*field)->val_int());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_INT24:
          value = ValueFactory::GetIntegerValue((int)(*field)->val_int());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_LONG:
          value = ValueFactory::GetBigIntValue((long)(*field)->val_int());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_DOUBLE:
          value = ValueFactory::GetDecimalValue((*field)->val_real());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_FLOAT:
          value = ValueFactory::GetDecimalValue((*field)->val_real());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_TIMESTAMP:
          //todo 时间戳类型的实现
//          value = ValueFactory::GetTimestampValue((*field)->val_date_temporal());
//          value = ValueFactory::GetTimestampValue(0);
          value = ValueFactory::GetVarcharValue("2021-09-20 14:47:18");
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_DATE:
          value = ValueFactory::GetDateValue((*field)->val_date_temporal());
          dest->SetValue(col_id, value);
          break;
        case MYSQL_TYPE_VARCHAR:{
          (*field)->val_str(&attribute, &attribute);
          value = ValueFactory::GetVarcharValue(attribute.ptr(),attribute.length()+1, false);
          dest->SetValue(col_id, value);
          break;
        }
        case MYSQL_TYPE_INVALID:
          break;
        default:
          break;
      }
//    }else {
//      if(!setItself){
//        dest->SetValue(col_id,old->GetValue(col_id));
//      }
//    }
  }
}

bool ha_peloton::IsPrimaryUpdate(const AbstractTuple *old){

  std::vector<uint> pk_cols = peloton_table->GetPk_cols();


  (void)old;

  for(uint i=0 ;i<pk_cols.size();i++){
    uint pk_col = pk_cols.at(i);
    (void)pk_col;
    if((*table->field[pk_col]).val_int() != old->GetValue(pk_col).GetIntData()){
      return true;
    }
  }
//  for (Field **field = table->field; *field; field++,col_id++) {
//    Value value;
//    String rowBuffer;
//    (*field)->val_str(&attribute, &attribute);
//    if(attribute.ptr() != old->GetValue(col_id).GetData()){
//
//    }
//  }


  return false;
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_peloton::delete_row(const uchar *) {//wjh
  DBUG_TRACE;

  PELOTON_ASSERT(peloton_table);

//  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());

//  auto &pos_lists = source_tile.get()->GetPositionLists();

  auto &transaction_manager =
      TransactionManagerFactory::GetInstance();

  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;

  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
  auto &curr_state = tx_stack.top();
  auto current_txn = curr_state.first;

  auto target_table_schema = peloton_table->GetSchema();
//  auto column_count = target_table_schema->GetColumnCount();


  // Delete each tuple

  ItemPointer current_location = visible_tuple_locations.at(current_position-1);
  uint current_tuple_id = current_location.offset;
  auto current_tile_group =
      StorageManager::GetInstance()->GetTileGroup(current_location.block).get();

  TileGroupHeader *tile_group_header = current_tile_group->GetHeader();

  ItemPointer old_location(current_tile_group->GetTileGroupId(), current_tuple_id);


//    // if running at snapshot isolation,
//    // then we need to retrieve the latest version of this tuple.
//    if (current_txn->GetIsolationLevel() == IsolationLevelType::SNAPSHOT) {
//      old_location = *(tile_group_header->GetIndirection(physical_tuple_id));
//
//      auto storage_manager = StorageManager::GetInstance();
//      tile_group = storage_manager->GetTileGroup(old_location.block).get();
//      tile_group_header = tile_group->GetHeader();
//
//      physical_tuple_id = old_location.offset;
//    }

//    ContainerTuple<TileGroup> old_tuple(current_tile_group.get(), current_tuple_id);
//    Tuple prev_tuple(peloton_table->GetSchema(), true);

  // Get a copy of the old tuple
//    for (uint column_itr = 0; column_itr < target_table_schema->GetColumnCount(); column_itr++) {
//      Value val = (old_tuple.GetValue(column_itr));
//      prev_tuple.SetValue(column_itr, val, nullptr);
//    }

  // Check the foreign key source table
//    if (peloton_table->CheckForeignKeySrcAndCascade(&prev_tuple,
//                                                    nullptr,
//                                                    current_txn,
//                                                    executor_context_,
//                                                    false) == false)
//    {
//      transaction_manager.SetTransactionResult(current_txn,
//                                               ResultType::FAILURE);
//      return HA_ERR_KEY_NOT_FOUND;
//    }

  bool is_owner = transaction_manager.IsOwner(current_txn, tile_group_header,
                                              current_tuple_id);

  bool is_written = transaction_manager.IsWritten(
      current_txn, tile_group_header, current_tuple_id);

  // if the current transaction is the creator of this version.
  // which means the current transaction has already updated the version.

  std::unique_ptr<Tuple> real_tuple(
      new Tuple(target_table_schema, false));

  if (is_owner == true && is_written == true) {
    // if the transaction is the owner of the tuple, then directly update in
    // place.
//      LOG_TRACE("The current transaction is the owner of the tuple");
    transaction_manager.PerformDelete(current_txn, old_location);
  } else {
    bool is_ownable = is_owner ||
                      transaction_manager.IsOwnable(
                          current_txn, tile_group_header, current_tuple_id);

    if (is_ownable == true) {
      // if the tuple is not owned by any transaction and is visible to
      // current transaction.
//        LOG_TRACE("Thread is not the owner of the tuple, but still visible");

      bool acquire_ownership_success =
          is_owner ||
          transaction_manager.AcquireOwnership(current_txn, tile_group_header,
                                               current_tuple_id);

      if (acquire_ownership_success == false) {
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return HA_ERR_GENERIC;// todo 返回一个合适的错误代码
      }
      // if it is the latest version and not locked by other threads, then
      // insert an empty version.
      ItemPointer new_location = peloton_table->InsertEmptyVersion();

      // PerformDelete() will not be executed if the insertion failed.
      // There is a write lock acquired, but since it is not in the write set,
      // because we haven't yet put them into the write set.
      // the acquired lock can't be released when the txn is aborted.
      // the YieldOwnership() function helps us release the acquired write
      // lock.
      if (new_location.IsNull() == true) {
//          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        if (is_owner == false) {
          // If the ownership is acquire inside this update executor, we
          // release it here
          transaction_manager.YieldOwnership(current_txn, tile_group_header,
                                             current_tuple_id);
        }
        transaction_manager.SetTransactionResult(current_txn,
                                                 ResultType::FAILURE);
        return HA_ERR_GENERIC;// todo 返回一个合适的错误代码
      }
      transaction_manager.PerformDelete(current_txn, old_location,
                                        new_location);

//        executor_context_->num_processed += 1;  // deleted one
    } else {
      // transaction should be aborted as we cannot update the latest version.
//        LOG_TRACE("Fail to update tuple. Set txn failure.");
      transaction_manager.SetTransactionResult(current_txn,
                                               ResultType::FAILURE);
      return HA_ERR_GENERIC;// todo 返回一个合适的错误代码
    }
  }
//  return true;
  //返回0则成功.
//  return HA_ERR_WRONG_COMMAND;
//  transaction_manager.SetTransactionResult(current_txn,
//                                           ResultType::FAILURE_AFTER_DELETE);
//  current_txn->SetDelLocation(old_location);//测试用的
//  stats.records--;
  return 0;
}

int ha_peloton::index_init(uint keynr,  /*!< in: key (index) number */
                           bool sorted __attribute__((unused))){
  DBUG_TRACE;
  //jy
//  stats.records = 0;
  is_idx_pos = false;// 初始化该值
  is_seq_scan = false;//
  enum_sql_command sql_command = (enum_sql_command)thd_sql_command(current_thd);
  if(!(for_update&&(sql_command == SQLCOM_UPDATE))){
    current_tuple_id = 0;
  }
  current_position = 0;
//  index_result_pos = 0;
  isFirstUseIndex = true;
//  isUseIndex = true;
//  current_tuples.clear();
  //这个keynr是创建索引的顺序. 即该表的第几个索引.
  //我们需要的值是索引所在的列. 移入index_read和in_range中.
//  int key_parts = table->key_info[keynr].actual_key_parts;
//  for(int i=0;i<key_parts;i++){
//    int col_num = table->key_info[keynr].key_part[i].fieldnr;
//    key_column_ids_.push_back(col_num);
//  }

  if(!is_idx_range){
    int col_num = table->key_info[keynr].key_part[0].fieldnr;
    key_column_ids_.clear();
    key_column_ids_.push_back(col_num-1);
  }

  active_index = keynr;

  limit_ = false;

  //result_itr_ = START_OID;


//  values_;//区间值. 按语句顺序.
//  expr_types_;//区间是大于号还是小于号. 按语句顺序.
  // 区间
//  left_open_ = node.GetLeftOpen();
//  right_open_ = node.GetRightOpen();


//  key_column_ids_.push_back(0);
  //expr_types_ = node.GetExprTypes();
  //values_ = node.GetValues();
  //runtime_keys_ = node.GetRunTimeKeys();
  //predicate_ = node.GetPredicate();
  //left_open_ = node.GetLeftOpen();
  //right_open_ = node.GetRightOpen();

  // This is for limit operation accelerate

  // limit_number_ = node.GetLimitNumber();
  // limit_offset_ = node.GetLimitOffset();
  // descend_ = node.GetDescend();

  std::string table_name(table->s->table_name.str);
  std::string db_name(table->s->db.str);
  table_name = "./"+db_name+"/"+table_name;
   //gsy ===
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithName(db_name);
  index_table_ = db->GetTableWithName(table_name);
  //index_table_ = peloton_tables.at(table_name);
  //======

  if(index_table_!=nullptr){
    column_ids_.resize(index_table_->GetSchema()->GetColumnCount());
    iota(column_ids_.begin(), column_ids_.end(), 0);
  }

  std::string mysql_index_name(table->key_info[keynr].name);
  std::string index_name = table_name + "_" + mysql_index_name;

  index_ = index_table_->GetIndexWithName(index_name);
  visible_tuple_locations.clear();

  peloton_table = index_table_;
//  index_predicate_.GetConjunctionListToSetup().clear();
//  index_predicate_.AddConjunctionScanPredicate(index_.get(), values_,
//                                               key_column_ids_, expr_types_);
  return 0;
}

/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/


int ha_peloton::get_map_len(ulong map){
  auto col_len = table->key_info->user_defined_key_parts;
  while (col_len < table->key_info->user_defined_key_parts &&
         map) {
    map >> 1;
    col_len++;
  }

  return col_len;
}

int ha_peloton::index_read_map(uchar *buf, const uchar *key_ptr, key_part_map map,
                               enum ha_rkey_function) {
  int rc = 0;
//  if(!visible_tuple_locations.empty()){
//    /*
//     * SELECT C_W_ID, C_D_ID, C_ID  FROM CUSTOMER where C_W_ID = 1 and  C_ID = 1;
//        该语句会出现反复调用index_read_map的现象. 为了避免这个问题, 当第一次调用index_read_map后,
//        visible_tuple_locations就有数据了. 自后只要一直index_next 最后返回HA_ERR_END_OF_FILE即可
//    */
//    return index_next(buf);
//  }

  if(key_ptr != nullptr){//如果key_ptr为nullptr, 则为full index_scan
    int col_len = get_map_len(map);

    if(is_seq_scan){
      //    isFirstUseIndex = false;
      return 0;
    }
    //  isFirstUseIndex = false;
    is_idx_pos = true;
    key_column_ids_.clear();
    expr_types_.clear();
    values_.clear();
    auto schema = index_table_->GetSchema();
    if(!is_idx_range){
      for(int i=0;i<col_len;i++){
        expr_types_.push_back(ExpressionType::COMPARE_EQUAL);
        int col_num = table->key_info[active_index].key_part[i].fieldnr;
        key_column_ids_.push_back(col_num-1);
        Value in_val;
        if(index_->GetKeySchema()->GetType(i) == TypeId::INTEGER
           ||index_->GetKeySchema()->GetType(i) == TypeId::BIGINT){
          int a = *(int *)key_ptr;
          in_val = ValueFactory::GetIntegerValue(a);
          size_t len = schema->GetColumn(col_num-1).GetLength();
          //        key_ptr = key_ptr+4;
          key_ptr = key_ptr+len;
        }else if(index_->GetKeySchema()->GetType(i) == TypeId::VARCHAR){
          key_ptr = key_ptr+2;
          std::string str1=(char*)key_ptr;
          size_t len = schema->GetColumn(col_num-1).GetLength();
          //        key_ptr = key_ptr+str1.size();
          key_ptr = key_ptr+len*3;
          in_val = ValueFactory::GetVarcharValue(str1);
        }
        values_.push_back(in_val);
      }
    }

  }else {
    key_column_ids_.clear();
    expr_types_.clear();
    values_.clear();
    key_column_ids_.push_back(0);
    expr_types_.push_back(ExpressionType::COMPARE_EQUAL);
    Value in_val = ValueFactory::GetIntegerValue(0);
    values_.push_back(in_val);
  }


  index_predicate_.GetConjunctionListToSetup().clear();
  index_predicate_.AddConjunctionScanPredicate(index_, values_,
                                                key_column_ids_, expr_types_, (key_ptr == nullptr));


  //弃用secondary_index_scan. 本质上是往visible_tuple_locations放数据.
  if(primary_index_scan() == -1){
    //      return -1;
  }

  if(visible_tuple_locations.empty()){
    return HA_ERR_KEY_NOT_FOUND;
  }

  if(!is_idx_range){//主键索引点查询
    return index_next(buf);
  }
  (void)buf;
  (void)key_ptr;
  (void)map;

  //  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}


int ha_peloton::index_read_last_map(uchar *buf, const uchar *key_ptr,
                                   key_part_map map) {

  (void)buf;
  (void)key_ptr;
  (void)map;

//  return 0;
  return index_read_map(buf, key_ptr, map, HA_READ_BEFORE_KEY);

}

int ha_peloton::primary_index_scan() {
  std::vector<ItemPointer *> tuple_location_ptrs; //存放scan的结果

  //调用索引的scan方法，结果放到tuple_location_ptrs里
  uint curr_thd_id = current_thd->thread_id();
  index_->Scan(values_,key_column_ids_,expr_types_,
               ScanDirectionType::FORWARD, tuple_location_ptrs,
               &index_predicate_.GetConjunctionList()[0], curr_thd_id );
  if(tuple_location_ptrs.size()==0){
    return 0;
  }

  TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
  std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
  auto &curr_state = tx_stack.top();
  auto current_txn = curr_state.first;


  auto &transaction_manager = TransactionManagerFactory::GetInstance();

  auto storage_manager = StorageManager::GetInstance();

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
#endif

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;
    auto tile_group = storage_manager->GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    size_t chain_length = 0;
    int retry_time = 0;

#ifdef LOG_TRACE_ENABLED
    num_tuples_examined++;
#endif
    // the following code traverses the version chain until a certain visible
    // version is found.
    // we should always find a visible version from a version chain.
    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VisibilityType::DELETED) {

        break;
      }
        // if the tuple is visible.
      else if (visibility == VisibilityType::OK) {

        bool eval = true;

        // if passed evaluation, then perform write.
        if (eval) {
          auto res = transaction_manager.PerformRead(current_txn,
                                                     tuple_location,
                                                     tile_group_header,
                                                     acquire_owner);

          if (res == 0){
            visible_tuple_locations.push_back(tuple_location);
            //          }else if (res == 1) {//false，应该会进入abort
          }
         else if (res == 2){//retry 等待机制
            retry_time++;
            std::this_thread::sleep_for(std::chrono::milliseconds(RETRYTIME));
            if(retry_time==5){
              retry_time = 0;
              transaction_manager.SetTransactionResult(current_txn,
                                                       ResultType::FAILURE);
              return -1;
            }

            auto new_location = tile_group_header->GetPrevItemPointer(tuple_location.offset);
            /*
            if(new_location.IsNull()){
              //当前的 tuple_locatiuon 已经是最新版本，不用更新，继续循环即可
            }
            */
            if(!new_location.IsNull()){
              ItemPointer *index_entry_ptr = tile_group_header->GetIndirection(tuple_location.offset);
              PELOTON_ASSERT(index_entry_ptr != nullptr);

              tuple_location = *index_entry_ptr;
              tile_group_header = storage_manager->GetTileGroup(tuple_location.block)->GetHeader();
            }
            continue;
          }
       else {//false，进入abort
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            return -1;
          }
        }

        break;
      }
        // if the tuple is not visible.
      else {
        PELOTON_ASSERT(visibility == VisibilityType::INVISIBLE);

        LOG_TRACE("Invisible read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        bool is_acquired = (tile_group_header->GetTransactionId(
            tuple_location.offset) == INITIAL_TXN_ID);
        bool is_alive =
            (tile_group_header->GetEndCommitId(tuple_location.offset) <=
             current_txn->GetReadId());
        if (is_acquired && is_alive) {
          // See an invisible version that does not belong to any one in the
          // version chain.
          // this means that some other transactions have modified the version
          // chain.
          // Wire back because the current version is expired. have to search
          // from scratch.
          tuple_location =
              *(tile_group_header->GetIndirection(tuple_location.offset));
          auto storage_manager = StorageManager::GetInstance();
          tile_group = storage_manager->GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);

        // there must exist a visible version.
        if (tuple_location.IsNull()) {
          if (chain_length == 1) {
            break;
          }

          // in most cases, there should exist a visible version.
          // if we have traversed through the chain and still can not fulfill
          // one of the above conditions,
          // then return result_failure.
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        // search for next version.
        auto storage_manager = StorageManager::GetInstance();
        tile_group = storage_manager->GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
        continue;
      }
    }
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
  }

  return 0;
}

/**
  @brief
  Used to read forward through the index.
*/

int ha_peloton::index_next(uchar *buf) {
  DBUG_TRACE;

  if(!is_idx_pos&&!is_idx_range){//既不是索引范围也不是索引点查询. 那么就是全表扫描.
    if(visible_tuple_locations.empty()){
      rnd_init(true);
    }
//    return rnd_next(buf);
  }

  if(current_position == visible_tuple_locations.size()){
    current_position = 0;
    return HA_ERR_END_OF_FILE;
  }

  if(peloton_table->GetMove_pos()){
    *buf = (uchar)peloton_table->GetInit_buf();
    buf += peloton_table->GetMove_pos();
  }
  if(current_position < visible_tuple_locations.size()){

    ItemPointer current_location = visible_tuple_locations.at(current_position);
    GetAllValueAsBuffer(buf, current_location);
    current_position++;
//    stats.records++;
    return 0;
  }

  current_position = 0;
  return HA_ERR_END_OF_FILE;
}

void ha_peloton::GetAllValueAsBuffer(uchar *buf, ItemPointer current_location) {

  auto tile_group = StorageManager::GetInstance()->GetTileGroup(current_location.block).get();
  Tile *tile = tile_group->GetTile(0);
  uint tuple_id = current_location.offset;

  int filed_position = 0;
  uint rec_length = 0;
  const char *tuple_location = tile->GetTupleLocation(tuple_id);

//    printf("Read tile_tuple_location: %p \n", tuple_location);

  const Schema *schema = tile->GetSchema();

  for (Field **field = table->field; *field; field++,filed_position++,buf += rec_length){
    const char *field_location = tuple_location + schema->GetOffset(filed_position);
    rec_length = (*field)->pack_length_in_rec();

    switch ((*field)->type()) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_SHORT:
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_DATE:
      case MYSQL_TYPE_TIMESTAMP:
      case MYSQL_TYPE_LONG:{
        int64_t val = *reinterpret_cast<const int64_t *>(field_location);
        (void)val;
        memcpy(buf, (uchar*)field_location, (size_t)rec_length);
        break;
      }
      case MYSQL_TYPE_FLOAT:{
        double val = *reinterpret_cast<const double *>(field_location);
        (void)val;
        memcpy(buf, (uchar*)field_location, (size_t)rec_length);
        break;
      }
      case MYSQL_TYPE_NEWDECIMAL:
      case MYSQL_TYPE_DOUBLE:{
        double val = *reinterpret_cast<const double *>(field_location);
        (void)val;
        memcpy(buf, (uchar*)field_location, (size_t)rec_length);
        break;
      }
//        case MYSQL_TYPE_TIMESTAMP:
        //todo 时间戳类型的实现
//          break;
//        case MYSQL_TYPE_DATE:
//          break;
      case MYSQL_TYPE_STRING:
      {
        memcpy(buf, (uchar*)field_location, (size_t)(rec_length/3));
        break;
      }
      case MYSQL_TYPE_VARCHAR:{
        const char *ptr = *reinterpret_cast<const char *const *>(field_location);
        uint32_t actual_len = *reinterpret_cast<const uint32_t *>(ptr);
        uint pack_length = HA_VARCHAR_PACKLENGTH(rec_length - 1);
        if (pack_length == 1) {
          *buf = (uchar)actual_len;
        } else {
          int2store(buf, actual_len);
        }
        if(actual_len<(*field)->pack_length_in_rec()){
//            LOG_INFO("varchar data: %s", ptr + sizeof(uint32_t));
          //actual_len-1对应tuple.cpp:122中的len+1
          memcpy(buf + pack_length, (uchar*)(ptr + sizeof(uint32_t)), actual_len);
        }
        break;
      }
      case MYSQL_TYPE_INVALID:
        break;
      default:
        break;
    }
  }
}


int ha_peloton::index_next_same(uchar *buf, const uchar *key, uint keylen){

  (void)key;
  (void)keylen;

  if(!is_idx_pos&&!is_idx_range){//既不是索引范围也不是索引点查询. 那么就是全表扫描.
    if(visible_tuple_locations.empty()){
      rnd_init(true);
    }
//    return rnd_next(buf);
  }

  if(current_position == visible_tuple_locations.size()){
    current_position = 0;
    return HA_ERR_END_OF_FILE;
  }

  if(peloton_table->GetMove_pos()){
    *buf = (uchar)peloton_table->GetInit_buf();
    buf += peloton_table->GetMove_pos();
  }
  if(current_position < visible_tuple_locations.size()){

    ItemPointer current_location = visible_tuple_locations.at(current_position);
    GetAllValueAsBuffer(buf, current_location);
    current_position++;
//    stats.records++;
    return 0;
  }

  current_position = 0;
  return HA_ERR_END_OF_FILE;

}

/**
  @brief
  Used to read backwards through the index.
*/

int ha_peloton::index_prev(uchar *buf) {
  DBUG_TRACE;
  (void)buf;
//  return index_next(buf);
  return index_next_same(buf,0,0);
//  return HA_ERR_END_OF_FILE;
}

/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_peloton::index_first(uchar *buf) {

//  if(!is_idx_pos&&!is_idx_range){//既不是索引范围也不是索引点查询. 那么就是全表扫描.
//    is_seq_scan = true;
//    if(visible_tuple_locations.empty()){
//      rnd_init(true);
//    }
//  }

//  if(is_idx_range){
  return index_read_map(buf, nullptr,0,HA_READ_KEY_EXACT);
//  }
//  return index_next(buf);
}

/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_peloton::index_last(uchar *) {
  int rc = 0;//todo 多并发插入的时候会走到这里, 目前还不清楚原因, 先让它走通吧
  DBUG_TRACE;
//  rc = HA_ERR_WRONG_COMMAND;
  return rc;
}

int ha_peloton::index_end() {
  current_position = 0;
  active_index = MAX_KEY;
//  index_result_pos = 0;
  left_open_ = false;
  right_open_ = false;

  if(!for_update){
    values_.clear();
    std::vector<Value>().swap(values_);
  }

  expr_types_.clear();
//  result_.clear();
  visible_tuple_locations.clear();
//  current_tuples.clear();
//  index_current_tuples.clear();
  key_column_ids_.clear();
  column_ids_.clear();
//  std::vector<uint>().swap(current_tuple_id_list);
//  std::vector<std::vector<std::string>>().swap(index_current_tuples);

//  for(uint i=0;i<result_.size();i++){
//    if(result_[i] != nullptr)   delete result_[i];
//  }

//  std::vector<LogicalTile *>().swap(result_);
//  std::vector<std::vector<std::string>>().swap(current_tuples);
  std::vector<uint>().swap(key_column_ids_);
  std::vector<uint>().swap(column_ids_);
  std::vector<ExpressionType>().swap(expr_types_);
//  delete index_.get();
  done_ = false;
  key_ready_ = false;
  is_idx_range = false;
  is_idx_pos = false;

  // pushed_idx_cond_keyno= MAX_KEY;
//  mi_set_index_cond_func(file, nullptr, nullptr);
//  in_range_check_pushed_down = false;
//  ds_mrr.dsmrr_close();
  return 0;
}

/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the peloton in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
//extern long number_of_invalids_;
int ha_peloton::rnd_init(bool) {//wjh
  DBUG_TRACE;
//  current_tuples.clear();
  current_position = 0;
  visible_tuple_locations.clear();
//  stats.records = 0;
  std::string db_name(table->s->db.str);
  std::string table_name("./"+db_name+"/");
  std::string table_name1(table->s->table_name.str);
  table_name = table_name+table_name1;
   //gsy=====
  // peloton_table = peloton_tables.at(table_name);
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithName(db_name);
  peloton_table = db->GetTableWithName(table_name);
  //====
//  current_tile_group_offset_ = START_OID;
  table_tile_group_count_ = peloton_table->GetTileGroupCount();

  find_current_tile();

  return 0;
}

int ha_peloton::rnd_end() {

//  current_tuples.clear();
//  std::vector<std::vector<std::string>>().swap(current_tuples);
//  std::vector<uint>().swap(current_tuple_id_list);
  visible_tuple_locations.clear();
  current_position = 0;
//  stats.records = 0;
//  current_tuple_id = 0;
//  current_tile_group_offset_ = START_OID;

  DBUG_TRACE;
  return 0;
}

/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc,
  sql_table.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and
  sql_update.cc
*/
int ha_peloton::rnd_next(uchar *buf) {//wjh r
  DBUG_TRACE;
//  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  if(current_position == visible_tuple_locations.size()){
    current_position = 0;
    return HA_ERR_END_OF_FILE;
  }

  if(peloton_table->GetMove_pos()){
    *buf = (uchar)peloton_table->GetInit_buf();
    buf += peloton_table->GetMove_pos();
  }
  if(current_position < visible_tuple_locations.size()){

    ItemPointer current_location = visible_tuple_locations.at(current_position);
    GetAllValueAsBuffer(buf, current_location);

//      ZKY_TRACE("WTF\n");
//    LOG_INFO("current_location: %d %d", current_location.block, current_location.offset);
    current_position++;
//    stats.records++;
    return 0;
  }

//  my_bitmap_map *org_bitmap = tmp_use_all_columns(table, table->write_set);
//  tmp_restore_column_map(table->write_set, org_bitmap);
//  return 0;
  current_position = 0;
  return HA_ERR_END_OF_FILE;
}

int ha_peloton::find_current_tile() {
//  DBUG_TRACE;

  current_position = 0;
  uint current_tile_group_offset = 0;
//  std::vector<uint> column_ids_;
//  if (column_ids_.empty()) {
//    column_ids_.resize(peloton_table->GetSchema()->GetColumnCount());
//    std::iota(column_ids_.begin(), column_ids_.end(), 0);
//  }
//
//
//  std::vector<int> result_format(peloton_table->GetSchema()->GetColumnCount(), 0);

//  std::vector<uint> position_list;
  while (current_tile_group_offset < table_tile_group_count_) {
    TransactionManager &transaction_manager = TransactionManagerFactory::GetInstance();
    TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
    std::stack<TcopTxnState> tx_stack = trafficCop->GetTcopTxnState();
    auto &curr_state = tx_stack.top();
    auto current_txn = curr_state.first;

    auto tile_group =
        peloton_table->GetTileGroup(current_tile_group_offset++);
    current_tile_group = tile_group.get();
    auto tile_group_header = tile_group->GetHeader();

    uint active_tuple_count = tile_group->GetNextTupleSlot();

    // Construct position list by looping through tile group
    // and applying the predicate.
//    std::vector<uint> position_list;
    bool isContuine = false;
    for (uint tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {

//        LOG_INFO("\n    active_tuple_count: %d", (int)active_tuple_count);

      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);



      auto visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, tuple_id);

      // check transaction visibility
      if (visibility == VisibilityType::OK) {

//          LOG_INFO("Read Location: %d, %d", (int)location.block, (int)location.offset);
  //        position_list.push_back(tuple_id);//这是之前的.

        //acquire_owner: 是否是select for update
        // todo 还需要知道, mysql相应的参数.
  //      bool acquire_owner = false;
        // if the tuple is visible, then perform predicate evaluation.
//        position_list.push_back(tuple_id);

        auto res = transaction_manager.PerformRead(current_txn,
                                                   location,
                                                   tile_group_header,
                                                   acquire_owner);

        if (res != 0) {
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          //如果读的时候和其他事务有冲突的情况下,
          // peloton直接回滚整个事务. mysql则能看到, 但是更新相关语句时阻塞.
          //todo 我这里先返回读错误.
          // 即没有执行成功. 整个事务是不会回滚. 之后再改改吧.

          return -1;
        }
        isContuine = true;
        visible_tuple_locations.push_back(location);
      }
    }

    // Don't return empty tiles
//    if ((position_list.size() == 0) && (current_tile_group_offset_ < table_tile_group_count_)) {
//    if ((position_list.size() == 0)) {
    if (isContuine) {
//      return find_current_tile(column_ids_, result_format);
        continue;
    }
//    else if((position_list.size() == 0) && (current_tile_group_offset_ == table_tile_group_count_)){
//      current_tuples.clear();
//      return 0;
//    }
//    //给删除做准备. 删除一个元祖, 需要tile_group_id和相对位置. 这里记录相对位置.
//    current_tuple_id_list.assign(position_list.begin(), position_list.end());
//
//    // Construct logical tile.
//  //  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
//    LogicalTile* logical_tile =LogicalTileFactory::GetTile();
//    logical_tile->AddColumns(tile_group, column_ids_);
//    logical_tile->AddPositionList(std::move(position_list));
//
//  //    std::vector<std::vector<std::string>> tuples;
//  //    current_tuples.clear();
//    current_tuples = logical_tile->GetAllValuesAsStrings(result_format, false);
//
//  //  delete logical_tile.get();
//  //  logical_tile.release();
//
//    if(current_tuples.empty()){
//      return -1;
//    }

  //    (void)tuples;

  //    LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
  //    SetOutput(logical_tile.release());
  //    return 0;
  }
  return 0;
}



/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_peloton::position(const uchar *) { DBUG_TRACE; }

/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and
  sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_peloton::rnd_pos(uchar *, uchar *) {
  int rc;
  DBUG_TRACE;
//  rc = HA_ERR_WRONG_COMMAND;
  rc = 0;//innodb和myisam都是返回0

  return rc;
}

/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really
  needed. SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_show.cc, sql_table.cc, sql_union.cc and sql_update.cc
*/
int ha_peloton::info(uint) {
  DBUG_TRACE;
//  if (stats.records < 2)
//    stats.records = 2;

  std::string table_name1(table->s->table_name.str);
//  std::string table_name("./" + table->s->db.str + "/");
  std::string db_name(table->s->db.str);
  std::string table_name("./"+db_name+"/");
  table_name = table_name+table_name1;
  //gsy=====
  // peloton_table = peloton_tables.at(table_name);
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithName(db_name);
  peloton_table = db->GetTableWithName(table_name);
  //====

//  stats.records = peloton_table->GetRealTupleCount();
  stats.records = peloton_table->GetRealTupleCount();
//  stats.deleted = ;
  stats.data_file_length = peloton_table->GetSchema()->GetLength();
  int col_num = peloton_table->GetSchema()->GetColumnCount();
  stats.index_file_length = stats.data_file_length/col_num;
//  stats.delete_length = ;
//  stats.check_time = ;
//  stats.mean_rec_length =;
  return 0;
}

/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_peloton::extra(enum ha_extra_function) {
  DBUG_TRACE;
  return 0;
}

/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases
  where the optimizer realizes that all rows will be removed as a result of an
  SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_query_block_query_expression::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_query_block_query_expression::exec() in sql_union.cc.
*/
int ha_peloton::delete_all_rows() {
  DBUG_TRACE;
  return HA_ERR_WRONG_COMMAND;
}

/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to
  understand this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/

//int e_thdid = 0;

int ha_peloton::external_lock(THD *thd, int lock_type) {//wjh e
//  sql_print_warning("external_lock");
  //@wjh
//  bool is_avtive_multi_stmt = thd->in_active_multi_stmt_transaction();
  DBUG_TRACE;

//测试用

  uint e_thdid = thd->thread_id();
  (void)e_thdid;
  enum_sql_command sql_command = (enum_sql_command)thd_sql_command(thd);

  if (lock_type != F_UNLCK) {//加锁的时候, 即语句开始的时候执行.

//    acquire_owner = (sql_command == SQLCOM_SELECT)&&(lock_type == F_WRLCK);

//    acquire_owner = false;
    acquire_owner = (sql_command == SQLCOM_UPDATE||sql_command == SQLCOM_DELETE
                     ||sql_command == SQLCOM_UPDATE_MULTI||sql_command == SQLCOM_DELETE_MULTI);


    if(sql_command == SQLCOM_DELETE||
        sql_command == SQLCOM_DELETE_MULTI||sql_command == SQLCOM_UPDATE_MULTI){
      for_update = false;
    }
    if(sql_command == SQLCOM_SELECT){
      for_update = (lock_type == F_WRLCK);
    }

    ResultType result = ResultType::SUCCESS;
    TrafficCop* trafficCop = (TrafficCop*)thd->get_ha_data(peloton_hton->slot)->ha_ptr;
    if(!trafficCop){
      trafficCop = new TrafficCop();
      current_thd->get_ha_data(peloton_hton->slot)->ha_ptr = trafficCop;
    }
    ulonglong *trxid = nullptr;//false和true代表的意义还需要进一步了解.
    trans_register_ha(thd, false, ht, trxid);//注册事务. 使得MySQL可以执行commit函数.

    if((thd->in_multi_stmt_transaction_mode()||thd->in_active_multi_stmt_transaction())&&trafficCop->get_is_first_stmt()){//多语句且是第一句, 就再注册一次.
      trans_register_ha(thd, true, ht, trxid);
      trafficCop->set_is_first_stmt(false);//这个值要在最后手动commit的时候设置为true才行.
    }

    result = trafficCop->BeginQueryHelper(e_thdid%TASK_NO);
//    sql_print_warning("BeginQueryHelper");

//    result = trafficCop->BeginQueryHelper(std::rand()%TASK_NO);//todo 随机数 //8.17

/*     * 其实不需要判断是否是单语句还是多语句.
     * 因为, BeginQueryHelper里有一个堆栈, begin的时候, 入栈,
     * commit和abort的时候, 出栈.
     * 如果是单语句的话, 执行晚了MySQL自动就到commit了, 直接出栈
     * 如果是多语句的话, 堆栈里有元素, 则不用begin, 堆栈里没有元素, 则begin.
     * 然后, 手动提交才会到commit函数, 那彼时再出栈即可.
     **/
    (void)trafficCop;
    if(result == ResultType::FAILURE){
      return HA_ERR_WRONG_COMMAND;
    }
  }//解锁的时候, 即语句结束的时候, 也会调用一次external_lock
  else if(sql_command != SQLCOM_SELECT &&lock_type == F_UNLCK){
    for_update = false;
  }

  (void)thd;
  (void)lock_type;
  return 0;
}


static int peloton_commit(handlerton *hton, THD *thd, bool commit_trx) {
  TrafficCop* trafficCop = (TrafficCop*)thd->get_ha_data(hton->slot)->ha_ptr;

  if(trafficCop->GetTcopTxnState().top().first->GetResult()
      == ResultType::FAILURE){
    return HA_ERR_LOCK_DEADLOCK;
  }

  if(commit_trx){
    // 多语句事务结束时，修改这个标记，下一条事务的第一个 sql 语句执行时，便会知道其是第一条语句
    trafficCop->set_is_first_stmt(true);
    trafficCop->CommitQueryHelper();
  }

  if(!thd->in_active_multi_stmt_transaction()){
    //单语句直接执行提交.
    trafficCop->CommitQueryHelper();
  }

  return 0;
}

static int peloton_rollback(handlerton *hton, THD *thd, bool rollback_trx) {
  /*
   * 一般情况下, 回滚单条语句
   *
   * 死锁或者手动回滚时 回滚整个事务.
   *  rollback_trx为true的时候, 则是回滚整条语句
   * rollback_trx false的话, 回滚一条语句.
   * 单条语句需要我主动调用, 多条一句则以 rollback; 为触发.
   *
   */
  TrafficCop *trafficCop = (TrafficCop *)thd->get_ha_data(hton->slot)->ha_ptr;

  if (rollback_trx) {
    trafficCop->set_is_first_stmt(true);
    trafficCop->AbortQueryHelper(true);  // 仅当回滚整个事务的时候, 为true
    return 0;
  }

  if (trafficCop->GetTcopTxnState().top().first->GetResult() ==
      ResultType::FAILURE) {
    trafficCop->AbortQueryHelper(true);
  }

  return 0;
}


char *make_str_helper(MEM_ROOT *root, const char *str, size_t len) {
  char *pos;
  if ((pos = static_cast<char *>(root->Alloc(len + 1)))) {
    if (len > 0) memcpy(pos, str, len);
    pos[len] = 0;
  }
  return pos;
}


uint ha_peloton::lock_count(void) const { return 0; }


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for peloton, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_peloton::store_lock(THD *, THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type) {
//  if(std::rand()%10 == 0){
//    if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type = lock_type;
//    *to++ = &lock;
//  }
  (void)lock_type;
  return to;
}

/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_peloton::delete_table(const char *, const dd::Table *) {
  DBUG_TRACE;
  /* This is not implemented but we want someone to be able that it works. */
  return 0;
}

/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions from
  handlerton::file_extensions.

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_peloton::rename_table(const char *, const char *, const dd::Table *,
                             dd::Table *) {
  DBUG_TRACE;
//  return HA_ERR_WRONG_COMMAND;//这里返回了错误的值.
  return 0;
}

/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_peloton::records_in_range(uint keynr, key_range *min_key, key_range *max_key) {

  DBUG_TRACE;
  is_idx_range = true;
  is_idx_pos = false;


  int col_num = table->key_info[keynr].key_part[0].fieldnr;
  //如果min_key和max_key相等, 则为点查询.
  //目前还不清楚, 为什么点查询也会到该函数里来. 在联合索引的情况下.
  if(min_key != NULL&&max_key != NULL&&*(int *)min_key->key == *(int *)max_key->key){
    is_idx_pos = true;
    is_idx_range = false;
//    key_column_ids_.clear();
//    expr_types_.clear();
//    values_.clear();
//    Value in_val = ValueFactory::GetIntegerValue(*(int *)min_key->key);
//    values_.push_back(in_val);
//    expr_types_.push_back(ExpressionType::COMPARE_EQUAL);
//    key_column_ids_.push_back(col_num-1);
    if(keynr == 0){
//      return 10;//return 1776;//
      return 10;//return 1776;//
    }else {//触发二级索引, 优先走之.
      return 1;
    }

  }
  if(min_key != NULL){
    Value min = ValueFactory::GetIntegerValue(*(int *)min_key->key);
    //区间值. 按语句顺序. 默认小于/大于等于, 如果是开区间则交给mysql来筛选
    values_.push_back(min);
    expr_types_.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    left_open_ = false;
    //即便是联合主键, 也只有一个属性被排序. 其他的部分有序.
    //因此, 其他key即便是点查询, 也不是有序的, 换言之, 这些key并没有什么用.
    //其他的key part用不上, 这里只取0
    key_column_ids_.push_back(col_num-1);
  }
//*(int *)min_key->key!=*(int *)max_key->key
  if(max_key != NULL){
    Value max = ValueFactory::GetIntegerValue(*(int *)max_key->key);
    values_.push_back(max);
    expr_types_.push_back(ExpressionType::COMPARE_LESSTHANOREQUALTO);
    right_open_ = false;
    key_column_ids_.push_back(col_num-1);
  }


//  expr_types_;//区间是大于号还是小于号. 按语句顺序.

  (void)keynr;
  if(keynr == 0){
    return 10;
  }else {
    return 1;//触发二级索引, 优先走之
  }
//  return 10;  //return 1766; low number to force index usage
}

static MYSQL_THDVAR_STR(last_create_thdvar, PLUGIN_VAR_MEMALLOC, nullptr,
                        nullptr, nullptr, nullptr);

static MYSQL_THDVAR_UINT(create_count_thdvar, 0, nullptr, nullptr, nullptr, 0,
                         0, 1000, 0);

/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/
int ha_peloton::create(const char *name, TABLE *, HA_CREATE_INFO *,
                       dd::Table *) {//wjh
  DBUG_TRACE;
  /*
    This is not implemented but we want someone to be able to see that it
    works.
  */
  std::string table_name(name);
  std::string db_name(table->s->db.str);

  if(table->s->keys == 0 || (table->s->primary_key == 0 && table->s->keys == 1)){
    //要么没有索引, 要么刚创建主键.
    THD *thd = ha_thd();
    char *buf = (char *)my_malloc(PSI_NOT_INSTRUMENTED, SHOW_VAR_FUNC_BUFF_SIZE,
                                  MYF(MY_FAE));
    snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE, "Last creation '%s'", name);
    THDVAR_SET(thd, last_create_thdvar, buf);
    my_free(buf);

    uint count = THDVAR(thd, create_count_thdvar) + 1;
    THDVAR_SET(thd, create_count_thdvar, &count);
    uchar *record;
    uint fieldpos, temp_length, recpos, minpos, length = 0;
    record = table->record[0];
    recpos = 0;
    minpos = table->s->reclength;
    Field **field, *found = nullptr;
    for (field = table->field; *field; field++) {
      if ((fieldpos = (*field)->offset(record)) >= recpos &&
          fieldpos <= minpos) {
        /* skip null fields */
        if (!(temp_length = (*field)->pack_length_in_rec()))
          continue; /* Skip null-fields */
        if (!found || fieldpos < minpos ||
            (fieldpos == minpos && temp_length < length)) {
          minpos = fieldpos;
          found = *field;
          length = temp_length;
        }
      }
    }

//------peloton code--------
    int rec_len = 0;//1119
    std::vector<Column> columns;
    int col_id = 0;

    for (Field **field = table->field; *field; field++,col_id++) {

      TypeId value_type;
      size_t field_size;
      bool is_inlined = true;
      switch ((*field)->type()) {
//      case MYSQL_TYPE_BOOL:
//        Value value = ValueFactory::GetBooleanValue((*field).);
//        break;
        case MYSQL_TYPE_TINY:
          value_type = TypeId::TINYINT;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_SHORT:
          value_type = TypeId::SMALLINT;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_INT24:
          value_type = TypeId::INTEGER;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_LONG:
          rec_len = (*field)->pack_length_in_rec();
          if(rec_len == 4){
            value_type = TypeId::INTEGER;
            field_size = Type::GetTypeSize(value_type);
          }else if (rec_len == 8){
            value_type = TypeId::BIGINT;
            field_size = Type::GetTypeSize(value_type);
          }
          break;
        case MYSQL_TYPE_NEWDECIMAL:
        case MYSQL_TYPE_DOUBLE:
          value_type = TypeId::DECIMAL;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_FLOAT:
          value_type = TypeId::DECIMAL;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_TIMESTAMP:
          //todo 时间戳类型的实现
//          value_type = TypeId::VARCHAR;
          value_type = TypeId::TIMESTAMP;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_DATE:
          value_type = TypeId::DATE;
          field_size = Type::GetTypeSize(value_type);
          break;
        case MYSQL_TYPE_STRING:{
          value_type = TypeId::CHAR;
          is_inlined = true;
          field_size = (*field)->pack_length_in_rec()/3;
          break;
        }
        case MYSQL_TYPE_VARCHAR:{
          value_type = TypeId::VARCHAR;
          is_inlined = false;
          //1119
//          field_size = Type::GetTypeSize(value_type);
          uint pack_length = HA_VARCHAR_PACKLENGTH((*field)->pack_length_in_rec() - 1);
          rec_len = ((*field)->pack_length_in_rec()-pack_length)/3;
          field_size = rec_len;
          //1119
          break;
        }
        case MYSQL_TYPE_INVALID:
          break;
        default:
          break;
      }

      Column column(value_type, field_size,
                    (*field)->field_name, is_inlined);
      columns.push_back(column);
    }
    bool own_schema = true;
    bool adapt_table = false;
    Schema *schema = new Schema(columns);
//gsy ====/
    auto storageManager = StorageManager::GetInstance();


    Database *db = storageManager->GetDatabaseWithName(db_name);
    if(db == nullptr){//如果该database没有就创建一个.

      db = new Database(ABC_DATABASE_INDEX);
      auto storage_manager = StorageManager::GetInstance();
      db->setDBName(db_name);
      storage_manager->AddDatabaseToStorageManager(db);
    }

    DataTable *pt =
        TableFactory::GetDataTable(0,db->GetTableCount(), schema,
                                   name,per_tile_group_count,
                                   own_schema,adapt_table, false,
                                   LayoutType::ROW);

    if (recpos != minpos) {//如果不相等则有null的值. 那么在buf读的时候就要相应的移位.
      pt->SetMove_pos((uint16)(minpos - recpos));
    }
    // std::pair<std::string, DataTable*>table_pair(table_name, pt);
    // peloton_tables.insert(table_pair);

    // LOGGING
    // FIXME: 这里的 EnterEpoch 不做处理，会影响 GC 么
    auto &log_manager = LogManager::GetInstance();
    log_manager.TableLogInsert( pt);

    //add table to storage manager
    db->AddTable(pt);
    //====

    if (table->s->keys == 1 && table->s->primary_key == 0){//创建主键索引
//      std::string db_name("./abc/");

      create_index(true, table_name, db_name);
    }
  }else if(table->s->keys != 0) {//创建二级索引.
    TrafficCop* trafficCop = (TrafficCop*)current_thd->get_ha_data(peloton_hton->slot)->ha_ptr;
//    std::string db_name("./abc/");
    table_name = trafficCop->get_curr_table_name();
    create_index(false, table_name, db_name);
  }

//  if (table->s->keys == 1 && table->s->primary_key == 0){//创建主键索引
//    create_index(true);
//  }else {
//    create_index(false);
//  }

  return 0;
}

//jy ===================
void ha_peloton::create_index(bool is_primary, std::string table_name, std::string db_name){

//  std::string db_name("./abc/");
//  table_name = db_name+table_name;
  //gsy ====
  auto storageManager = StorageManager::GetInstance();
//  std::string db_name1 = "abc";
  Database *db = storageManager->GetDatabaseWithName(db_name);
  DataTable *pt = db->GetTableWithName(table_name);
  //DataTable *pt = peloton_tables.at(table_name);
  //=====
  auto schema = pt->GetSchema();
  bool unique_keys ;
  int key_parts;
  std::string index_name;
  std::vector<uint> key_attrs;
  IndexConstraintType index_constraint;
  uint database_oid=0;
  uint table_oid = 0;
  uint index_oid = 0;
  uint key_length = 0;

  IndexType index_type = INDEXTYPE;

  IndexMetadata *index_metadata;
  //判断是否为主键索引的创建.
  if(is_primary){
    std::string mysql_index_name("PRIMARY");
    std::string scheme_name;
    //std::string table_name;
    index_name = table_name + "_" + mysql_index_name;

    index_constraint= IndexConstraintType::PRIMARY_KEY;

//获得要建index的column

    key_parts = table->key_info[0].actual_key_parts;
    for(int i=0;i<key_parts;i++){
      int col_num = table->key_info[0].key_part[i].fieldnr;
      key_attrs.push_back(col_num-1);
      if (TypeId::VARCHAR == schema->GetType(col_num-1)) {
        key_length+= schema->GetColumn(col_num-1).GetLength()+4;
      }else {
        key_length+= schema->GetColumn(col_num-1).GetLength()+1;
      }
    }

    pt->SetPk_cols(key_attrs); // 添加主键列, 让update的时候知道知否是主键更新.
    pt->SetPrimaryIndexName(index_name); // 在这里就先把名字存好，从日志解析的时候不用再在前面加表名

    auto key_schema = Schema::CopySchema(schema,key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

//set index metadata
    unique_keys = true;
    index_metadata =new IndexMetadata(
        index_name,index_oid,table_oid,database_oid,index_type,
        index_constraint,schema,key_schema,key_attrs,unique_keys);

//add index to table

  }else {//非主键索引.
    TABLE_SHARE *ts = table->s;
    int cur_key_num = 0;
    for(uint i=ts->primary_key == 0?1:0;i<ts->keys;i++){//如果有主键索引, 则从1开始.
      std::string mysql_index_name(ts->key_info[i].name);
      index_name = table_name + "_" + mysql_index_name;
      if(find(all_index.begin(), all_index.end(), index_name)==all_index.end()){
        //没找到key则为当前插入的key
        cur_key_num = i;
        key_parts = table->key_info[cur_key_num].actual_key_parts;
        all_index.push_back(index_name);
        break;
      }
    }
    int i;
    for(i=0;i<key_parts;i++){
      int col_num = table->key_info[cur_key_num].key_part[i].fieldnr;
      key_attrs.push_back(col_num-1);
      if (TypeId::VARCHAR == schema->GetType(col_num-1)) {
        key_length+= schema->GetColumn(col_num-1).GetLength()+4;
      }else {
        key_length+= schema->GetColumn(col_num-1).GetLength()+1;
      }
    }
    unique_keys = ts->key_info[i].flags == 97;
    //todo 独一索引.或者普通索引.(没找到unique index相关的标识)

    if(true){
      index_constraint= IndexConstraintType::PRIMARY_KEY;
//      index_constraint= IndexConstraintType::UNIQUE;

    }else {
      index_constraint= IndexConstraintType::DEFAULT;
    }
    auto key_schema = Schema::CopySchema(schema,key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    // 添加其他索引的信息
    pt->AddUniqueIndexInfo(index_name, key_attrs);

    index_metadata =new IndexMetadata(
      //jy
        index_name,index_oid,table_oid,database_oid,index_type,
        index_constraint,schema,key_schema,key_attrs,unique_keys);
  }

  Index* key_index = IndexFactory::GetIndex(index_metadata);
  key_index->SetKeyLength(key_length);
  pt->AddIndex(key_index);

  // logging，记录索引数据
  auto &log_manager = LogManager::GetInstance();
  log_manager.TableLogInsert(pt);
}
//jy ===================


struct st_mysql_storage_engine peloton_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

static ulong srv_enum_var = 0;
static ulong srv_ulong_var = 0;
static double srv_double_var = 0;
static int srv_signed_int_var = 0;
static long srv_signed_long_var = 0;
static longlong srv_signed_longlong_var = 0;

const char *enum_var_names_peloton[] = {"peloton1", "peloton2", NullS};

TYPELIB enum_var_typelib_peloton = {array_elements(enum_var_names_peloton) - 1,
                            "enum_var_typelib_peloton", enum_var_names_peloton, nullptr};

static MYSQL_SYSVAR_ENUM(enum_var,                        // name
                         srv_enum_var,                    // varname
                         PLUGIN_VAR_RQCMDARG,             // opt
                         "Sample ENUM system variable.",  // comment
                         nullptr,                         // check
                         nullptr,                         // update
                         0,                               // def
                         &enum_var_typelib_peloton);              // typelib

static MYSQL_SYSVAR_ULONG(ulong_var, srv_ulong_var, PLUGIN_VAR_RQCMDARG,
                          "0..1000", nullptr, nullptr, 8, 0, 1000, 0);

static MYSQL_SYSVAR_DOUBLE(double_var, srv_double_var, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5,
                           0);  // reserved always 0

static MYSQL_THDVAR_DOUBLE(double_thdvar, PLUGIN_VAR_RQCMDARG,
                           "0.500000..1000.500000", nullptr, nullptr, 8.5, 0.5,
                           1000.5, 0);

static MYSQL_SYSVAR_INT(signed_int_var, srv_signed_int_var, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_THDVAR_INT(signed_int_thdvar, PLUGIN_VAR_RQCMDARG,
                        "INT_MIN..INT_MAX", nullptr, nullptr, -10, INT_MIN,
                        INT_MAX, 0);

static MYSQL_SYSVAR_LONG(signed_long_var, srv_signed_long_var,
                         PLUGIN_VAR_RQCMDARG, "LONG_MIN..LONG_MAX", nullptr,
                         nullptr, -10, LONG_MIN, LONG_MAX, 0);

static MYSQL_THDVAR_LONG(signed_long_thdvar, PLUGIN_VAR_RQCMDARG,
                         "LONG_MIN..LONG_MAX", nullptr, nullptr, -10, LONG_MIN,
                         LONG_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(signed_longlong_var, srv_signed_longlong_var,
                             PLUGIN_VAR_RQCMDARG, "LLONG_MIN..LLONG_MAX",
                             nullptr, nullptr, -10, LLONG_MIN, LLONG_MAX, 0);

static MYSQL_THDVAR_LONGLONG(signed_longlong_thdvar, PLUGIN_VAR_RQCMDARG,
                             "LLONG_MIN..LLONG_MAX", nullptr, nullptr, -10,
                             LLONG_MIN, LLONG_MAX, 0);

static SYS_VAR *peloton_system_variables[] = {
    MYSQL_SYSVAR(enum_var),
    MYSQL_SYSVAR(ulong_var),
    MYSQL_SYSVAR(double_var),
    MYSQL_SYSVAR(double_thdvar),
    MYSQL_SYSVAR(last_create_thdvar),
    MYSQL_SYSVAR(create_count_thdvar),
    MYSQL_SYSVAR(signed_int_var),
    MYSQL_SYSVAR(signed_int_thdvar),
    MYSQL_SYSVAR(signed_long_var),
    MYSQL_SYSVAR(signed_long_thdvar),
    MYSQL_SYSVAR(signed_longlong_var),
    MYSQL_SYSVAR(signed_longlong_thdvar),
    nullptr};

// this is an peloton of SHOW_FUNC
static int show_func_peloton(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_CHAR;
  var->value = buf;  // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
           "enum_var is %lu, ulong_var is %lu, "
           "double_var is %f, signed_int_var is %d, "
           "signed_long_var is %ld, signed_longlong_var is %lld",
           srv_enum_var, srv_ulong_var, srv_double_var, srv_signed_int_var,
           srv_signed_long_var, srv_signed_longlong_var);
  return 0;
}

struct peloton_vars_t {
  ulong var1;
  double var2;
  char var3[64];
  bool var4;
  bool var5;
  ulong var6;
};

peloton_vars_t peloton_vars = {100, 20.01, "three hundred", true, false, 8250};

static SHOW_VAR show_status_peloton[] = {
    {"var1", (char *)&peloton_vars.var1, SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"var2", (char *)&peloton_vars.var2, SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF,
        SHOW_SCOPE_UNDEF}  // null terminator required
};

static SHOW_VAR show_array_peloton[] = {
    {"array", (char *)show_status_peloton, SHOW_ARRAY, SHOW_SCOPE_GLOBAL},
    {"var3", (char *)&peloton_vars.var3, SHOW_CHAR, SHOW_SCOPE_GLOBAL},
    {"var4", (char *)&peloton_vars.var4, SHOW_BOOL, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

static SHOW_VAR func_status[] = {
    {"peloton_func_peloton", (char *)show_func_peloton, SHOW_FUNC,
                                   SHOW_SCOPE_GLOBAL},
    {"peloton_status_var5", (char *)&peloton_vars.var5, SHOW_BOOL,
                                   SHOW_SCOPE_GLOBAL},
    {"peloton_status_var6", (char *)&peloton_vars.var6, SHOW_LONG,
                                   SHOW_SCOPE_GLOBAL},
    {"peloton_status", (char *)show_array_peloton, SHOW_ARRAY,
                                   SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};

mysql_declare_plugin(peloton){
                                 MYSQL_STORAGE_ENGINE_PLUGIN,
                                 &peloton_storage_engine,
                                 "PELOTON",
                                 PLUGIN_AUTHOR_ORACLE,
                                 "Peloton storage engine",
                                 PLUGIN_LICENSE_GPL,
                                 peloton_init_func, /* Plugin Init */
                                 nullptr,           /* Plugin check uninstall */
                                 nullptr,           /* Plugin Deinit */
                                 0x0001 /* 0.1 */,
                                 func_status,              /* status variables */
                                 peloton_system_variables, /* system variables */
                                 nullptr,                  /* config options */
                                 0,                        /* flags */
                             } mysql_declare_plugin_end;
