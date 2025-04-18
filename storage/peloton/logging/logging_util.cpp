//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// logging_util.cpp
//
// Identification: src/logging/logging_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "logging_util.h"

#include <butil/logging.h>
#include <dirent.h>
#include <sys/stat.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <utility>

#include "my_inttypes.h"
#include "storage/peloton/catalog/catalog.h"
// #include "storage/peloton/store/database.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/logging/frontend_logger.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/tile_group_header.h"
#include "storage/peloton/store/tuple.h"

size_t LoggingUtil::ReadNextFrame(butil::IOBuf &entry_data,
                                  SimpleBuffer &buffer) {
  // Assuming GetNextFrameSize gets the next frame's size from the buffer
  auto frame_size = GetNextFrameSize(entry_data);
  CHECK_NE(frame_size, 0);
  if (frame_size == 0) {
    return 0;
  }

  buffer.check_size(frame_size);
  auto ret = entry_data.cutn(buffer.data(), frame_size);
  CHECK_EQ(ret, frame_size);
  if (ret != frame_size) {
    return 0;
  }

  return frame_size;
}

bool LoggingUtil::ReadTupleRecordHeader(
    TupleRecord &tuple_record, butil::IOBuf &entry_data,
    SimpleBuffer &buffer) {  // Check if buffer is empty

  auto header_size = ReadNextFrame(entry_data, buffer);
  if (header_size == 0) {
    return false;
  }

  CopySerializeInput tuple_header(buffer.data(), header_size);
  tuple_record.DeserializeHeader(tuple_header);

  return true;
}

bool LoggingUtil::ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                              butil::IOBuf &entry_data,
                                              SimpleBuffer &buffer) {
  auto header_size = ReadNextFrame(entry_data, buffer);
  if (header_size == 0) {
    return false;
  }

  CopySerializeInput txn_header(buffer.data(), header_size);
  txn_record.Deserialize(txn_header);

  return true;
}

size_t LoggingUtil::GetNextFrameSize(butil::IOBuf &entry_data) {
  size_t frame_size;
  int32_t int_buffer;

  // Check if the frame size is broken
  if (entry_data.size() < sizeof(int32_t)) {
    LOG(INFO) << "Frame size is broken: " << entry_data.size() << " < "
              << sizeof(int32_t);
    return 0;
  }

  // Read frame size from IOBuf
  auto ret = entry_data.copy_to(&int_buffer, sizeof(int32_t));
  CHECK(ret == sizeof(int32_t));
  if (ret != sizeof(int32_t)) {
    return 0;
  }

  // Convert buffer bytes to integer and add buffer's size
  frame_size = static_cast<size_t>(int_buffer) + sizeof(int32_t);

  // Check if the frame is broken
  CHECK_LE(frame_size, entry_data.size());
  if (entry_data.size() < frame_size) {
    return 0;
  }

  return frame_size;
}

// unique_ptr<Tuple> LoggingUtil::ReadTupleRecordBody(const Schema *schema,
//                                                    butil::IOBuf &entry_data,
//                                                    SimpleBuffer &buffer) {
//     CHECK(schema != nullptr);
//     // Check if the frame is broken
//     auto body_size = ReadNextFrame(entry_data, buffer);
//     if (body_size == 0) {
//         return nullptr;
//     }

//     CopySerializeInput tuple_body(buffer.data(), body_size);

//     // We create a tuple based on the message
//     std::unique_ptr<Tuple> tuple(new Tuple(schema, true));
//     LoggingUtil::DeserializeTupleFrom(tuple.get(), tuple_body);
//     return std::move(tuple);
// }

unique_ptr<Tuple> LoggingUtil::ReadTupleRecordBody(const Schema *schema,
                                                   butil::IOBuf &entry_data,
                                                   SimpleBuffer &buffer) {
  CHECK(schema != nullptr);
  // Check if the frame is broken
  auto body_size = ReadNextFrame(entry_data, buffer);
  if (body_size == 0) {
    return nullptr;
  }
  auto column_count = schema->GetColumnCount();
  auto data = buffer.data();
  std::unique_ptr<Tuple> tuple(new Tuple(schema, true));
  // Skip the size of the tuple
  data += sizeof(int32_t);

  for (int col = 0; col < column_count; col++) {
    const Column &column = schema->GetColumn(col);
    auto type = column.GetType();
    if (TypeId::VARCHAR == type) {
      auto actual_len = *reinterpret_cast<const uint32_t *>(data);
      data += sizeof(uint32_t);
      tuple->SetValueNew(col, data, actual_len, nullptr);
      data += actual_len;
    } else {
      tuple->SetValueNew(col, data, -1, nullptr);
      data += column.GetLength();
    }
  }

  return std::move(tuple);
}

LogRecordType LoggingUtil::GetNextLogRecordType(butil::IOBuf &entry_data) {
  char buffer;

  // Check if the log record type is broken
  if (entry_data.size() < sizeof(char)) {
    return LOGRECORD_TYPE_INVALID;
  }

  // Otherwise, read the log record type
  auto ret = entry_data.cutn(&buffer, sizeof(char));
  CHECK_EQ(ret, sizeof(char));

  CopySerializeInput input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

void LoggingUtil::SkipRecordBody(butil::IOBuf &entry_data,
                                 SimpleBuffer &buffer) {
  auto body_size = ReadNextFrame(entry_data, buffer);
  if (body_size == 0) {
    LOG(FATAL) << "Broken entry data";
    return;
  }
  entry_data.cutn(buffer.data(), body_size);
}

void *LoggingUtil::RecoveryOneTransaction(void *arg) {
  Records *tuple_records = static_cast<Records *>(arg);
  for (auto it = tuple_records->begin(); it != tuple_records->end(); it++) {
    auto curr = *it;
    switch (curr->GetType()) {
      case LOGRECORD_TYPE_TUPLE_INSERT:
        InsertTuple(curr.get());
        LOG_TRACE("recovery insert record");
        break;
      case LOGRECORD_TYPE_TUPLE_UPDATE:
        UpdateTuple(curr.get());
        LOG_TRACE("recovery update record");
        break;
      case LOGRECORD_TYPE_TUPLE_DELETE:
        DeleteTuple(curr.get());
        break;
      default:
        continue;
    }
  }
  // delete tuple_records;
  return nullptr;
}

/**
 * @brief     Insert a record in databse thread safe.
 * @param[in] commit_id  事务提交时的 transaction id
 * @param[in] db_id     数据库 id
 * @param[in] table_id  表 id
 * @param[in] tuple     要插入的 tuple，这里拥有完整的数据
 * @param[in] should_increase_tuple_count  是否增加 tuple_count
 */
void InsertTupleHelper(cid_t commit_id, uint db_id, uint table_id, Tuple *tuple,
                       uint32_t thread_id,
                       bool should_increase_tuple_count = true) {
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithOid(db_id);
  CHECK(db != nullptr);
  auto table = db->GetTableWithOid(table_id);
  CHECK(table != nullptr);

  auto index = table->GetIndex(0);
  if (index == nullptr) {
    // LOG(WARNING) << "No index found for table " << table->GetName();
    // FIXME: For raft cluster, each table must have a primary key index.
    (void)table->GetEmptyTupleSlotForRecovery(tuple, thread_id);
    return;
  }

  auto indexSchema = index->GetMetadata()->GetKeySchema();
  // 得到主键列的序号
  auto indexColumns = indexSchema->GetIndexedColumns();

  std::vector<ItemPointer *> tuple_location_ptrs;  // 存放scan的结果
  // 构建tuple中的主键数组
  std::vector<Value> colValues;
  for (int i = 0; i < indexColumns.size(); ++i) {
    uint colNo = indexColumns[i];
    colValues.push_back(tuple->GetValue(colNo));
  }
  // 通过主键查询
  index->PointGet(colValues, tuple_location_ptrs, index->GetKeyLength(),
                  thread_id);

  if (tuple_location_ptrs.size() != 0) {  // 查到

    // 拿到tuple_header
    ItemPointer *tuple_location = tuple_location_ptrs[0];
    auto tile_group_old = storageManager->GetTileGroup(tuple_location->block);
    auto tile_group_header_old = tile_group_old.get()->GetHeader();

    auto &latch = tile_group_header_old->GetSpinLatch(tuple_location->offset);
    // 拿到索引中指向的版本的记录锁, 判断当前插入的版本是否过期了.
    latch.Lock();
    cid_t old_commit_id =
        tile_group_header_old->GetTransactionId(tuple_location->offset);
    if (old_commit_id > commit_id) {  // 当前插入的版本过期
      latch.Unlock();
      return;
    } else {
      // 把header里的txid改为当前版本的txid
      tile_group_header_old->SetAtomicTransactionId(tuple_location->offset,
                                                    commit_id);

      // 得到index value中itempointer指针指向的值.
      ItemPointer *index_entry_ptr =
          tile_group_header_old->GetIndirection(tuple_location->offset);

      uint curr_thd_id = 0;
      auto insert_loc = new ItemPointer(
          table->GetEmptyTupleSlotForRecovery(tuple, curr_thd_id));
      auto tile_group = storageManager->GetTileGroup(insert_loc->block);
      auto tile_group_header = tile_group.get()->GetHeader();

      tile_group->GetHeader()->SetIndirection(insert_loc->offset,
                                              index_entry_ptr);

      // 修改index
      AtomicUpdateItemPointer(index_entry_ptr,
                              *insert_loc);  // 不可能失败的. 记录已经被锁住了
      tile_group_header->SetTransactionId(insert_loc->offset, INITIAL_TXN_ID);
      tile_group_header->SetBeginCommitId(insert_loc->offset, commit_id);
      tile_group_header->SetEndCommitId(insert_loc->offset, MAX_CID);
      tile_group_header->SetNextItemPointer(insert_loc->offset,
                                            INVALID_ITEMPOINTER);

      // 释放锁
      latch.Unlock();
    }

  } else {  // 没有查到则直接插入
    // 插入索引
    auto alwaysTrueLambda = [](const void *) -> bool { return true; };
    std::function<bool(const void *)> fn = alwaysTrueLambda;

    std::unique_ptr<Tuple> key(new Tuple(indexSchema, true));
    key->SetFromTupleAsIndex((const Tuple *)tuple, indexColumns);

    uint curr_thd_id = 0;
    // 插入新版本并拿到itempoint

    // Should allocate a new Itempointer, otherwise when the insert_loc is
    // deleted, the index will be invalid.
    auto insert_loc = new ItemPointer(
        table->GetEmptyTupleSlotForRecovery(tuple, curr_thd_id));
    auto tile_group = storageManager->GetTileGroup(insert_loc->block);
    auto tile_group_header = tile_group.get()->GetHeader();

    bool res = index->CondInsertEntry(key.get(), insert_loc, fn, 0);
    // 如果失败了则retry
    //  FIXME: Should delete the tuple if failed, or use the insert_loc to
    //  retry.
    if (!res) {
      delete insert_loc;
      InsertTupleHelper(commit_id, db_id, table_id, tuple, thread_id,
                        should_increase_tuple_count = true);
    }

    tile_group_header->SetTransactionId(insert_loc->offset, INITIAL_TXN_ID);
    tile_group_header->SetBeginCommitId(insert_loc->offset, commit_id);
    tile_group_header->SetEndCommitId(insert_loc->offset, MAX_CID);
    tile_group_header->SetNextItemPointer(insert_loc->offset,
                                          INVALID_ITEMPOINTER);
  }

  if (should_increase_tuple_count) {
    table->IncreaseTupleCount(1);
    table->IncreaseRealTupleCount(1);
  }
  delete tuple;
}

/**
 * @brief     Insert a record in databse thread safe.
 * @param[in] primary_key_tuple     注意：这里的 tuple 只有 primary key
 * 的数据，仅用于查询 true，暂时不用管？
 */
void DeleteTupleHelper(cid_t commit_id, uint db_id, uint table_id,
                       int64_t thread_id, Tuple *primary_key_tuple) {
  auto storageManager = StorageManager::GetInstance();
  Database *db = storageManager->GetDatabaseWithOid(db_id);
  CHECK(db != nullptr);
  auto table = db->GetTableWithOid(table_id);
  CHECK(table != nullptr);
  //@wjh
  // 拿到主键, 然后查询
  // 如果是复合主键, 怎么办.
  // 拿到主键索引index.
  uint curr_thd_id = 0;
  auto index = table->GetIndex(0);
  auto indexSchema = index->GetMetadata()->GetKeySchema();
  // 得到主键列的序号
  auto indexColumns = indexSchema->GetIndexedColumns();

  std::vector<ItemPointer *> tuple_location_ptrs;  // 存放scan的结果
  // 构建tuple中的主键数组
  std::vector<Value> colValues;
  for (int i = 0; i < indexColumns.size(); ++i) {
    uint colNo = indexColumns[i];
    colValues.push_back(primary_key_tuple->GetValue(colNo));
  }
  // 通过主键查询
  index->PointGet(colValues, tuple_location_ptrs, index->GetKeyLength(),
                  curr_thd_id);

  if (tuple_location_ptrs.size() != 0) {  // 查到

    /*
     * 根据主键查询, 判断delete_loc是否存在
     *    如果存在, 则直接比较begin_ts和commit_id,
     *          如果本次删除是之前的删除则不做改动,
     *          如果是之后的删除则修改当前的begin_ts和end_ts
     *    如果不存在, 则直接插入一条删除tuple, 内容为空, begin_ts=end_ts
     *
     */
    // 拿到tuple_header
    ItemPointer *tuple_location = tuple_location_ptrs[0];
    auto tile_group_old = storageManager->GetTileGroup(tuple_location->block);
    auto tile_group_header_old = tile_group_old.get()->GetHeader();
    auto &latch = tile_group_header_old->GetSpinLatch(tuple_location->offset);
    // 拿到索引中指向的版本的记录锁, 判断当前插入的版本是否过期了.
    latch.Lock();
    cid_t old_commit_id =
        tile_group_header_old->GetTransactionId(tuple_location->offset);
    if (old_commit_id > commit_id) {  // 如果本次删除是之前的删除则不做改动
      latch.Unlock();
      return;
    } else {
      // 把header里的txid改为当前删除的txid
      tile_group_header_old->SetAtomicTransactionId(tuple_location->offset,
                                                    commit_id);
      // 如果是之后的删除则修改当前的begin_ts和end_ts
      tile_group_header_old->SetBeginCommitId(tuple_location->offset,
                                              commit_id);
      tile_group_header_old->SetEndCommitId(tuple_location->offset, commit_id);

      // 释放锁
      latch.Unlock();
    }

  } else {  // 如果不存在, 则直接插入一条删除tuple, 内容为空, begin_ts=end_ts

    // 插入空的值
    ItemPointer delete_loc =
        table->GetEmptyTupleSlotForRecovery(nullptr, curr_thd_id);
    auto tile_group = storageManager->GetTileGroup(delete_loc.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    auto &latch = tile_group_header->GetSpinLatch(delete_loc.offset);
    // 拿到索引中指向的版本的记录锁, 判断当前插入的版本是否过期了.
    latch.Lock();
    // 把header里的txid改为当前删除的txid
    tile_group_header->SetAtomicTransactionId(delete_loc.offset, commit_id);
    // 如果是之后的删除则修改当前的begin_ts和end_ts
    tile_group_header->SetBeginCommitId(delete_loc.offset, commit_id);
    tile_group_header->SetEndCommitId(delete_loc.offset, commit_id);
    latch.Unlock();

    // 插入索引
    auto alwaysTrueLambda = [](const void *) -> bool { return true; };

    std::function<bool(const void *)> fn = alwaysTrueLambda;

    std::unique_ptr<Tuple> key(new Tuple(indexSchema, true));
    key->SetFromTupleAsIndex((const Tuple *)primary_key_tuple, indexColumns);

    bool res = index->CondInsertEntry(
        key.get(), const_cast<ItemPointer *>(&delete_loc), fn, 0);
    // 如果失败了则retry
    if (!res) {
      DeleteTupleHelper(commit_id, db_id, table_id, thread_id,
                        primary_key_tuple);
    }
  }

  table->DecreaseTupleCount(1);
}

void LoggingUtil::InsertTuple(TupleRecord *record) {
  InsertTupleHelper(record->GetTransactionId(), record->GetDatabaseOid(),
                    record->GetTableId(), record->GetTuple(), 0, true);
}

void LoggingUtil::DeleteTuple(TupleRecord *record) {
  DeleteTupleHelper(record->GetTransactionId(), record->GetDatabaseOid(),
                    record->GetTableId(), 0, record->GetTuple());
}

void LoggingUtil::UpdateTuple(TupleRecord *record) {
  InsertTupleHelper(record->GetTransactionId(), record->GetDatabaseOid(),
                    record->GetTableId(), record->GetTuple(), 0, false);
}
//===--------------------------------------------------------------------===//
// LoggingUtil
//===--------------------------------------------------------------------===//
bool LoggingUtil::IsVisible(const TileGroupHeader *const tile_group_header,
                            const uint &tuple_id, cid_t start_cid) {
  txn_id_t tuple_txn_id = tile_group_header->GetTransactionId(tuple_id);
  cid_t tuple_begin_cid = tile_group_header->GetBeginCommitId(tuple_id);
  cid_t tuple_end_cid = tile_group_header->GetEndCommitId(tuple_id);
  if (tuple_txn_id == INVALID_TXN_ID) {
    // the tuple is not available.
    return false;
  }

  bool activated = (start_cid >= tuple_begin_cid);
  bool invalidated = (start_cid >= tuple_end_cid);
  if (tuple_txn_id != INITIAL_TXN_ID) {
    // if the tuple is owned by other transactions.
    if (tuple_begin_cid == MAX_CID) {
      // never read an uncommitted version.
      return false;
    } else {
      // the older version may be visible.
      if (activated && !invalidated) {
        return true;
      } else {
        return false;
      }
    }
  } else {
    // if the tuple is not owned by any transaction.
    if (activated && !invalidated) {
      return true;
    } else {
      return false;
    }
  }
}

BackendType LoggingUtil::GetBackendType(const LoggingType &logging_type) {
  // Default backend type
  BackendType backend_type = BackendType::MM;

  switch (logging_type) {
    case LoggingType::NVM_WBL:
      backend_type = BackendType::NVM;
      break;

    case LoggingType::SSD_WBL:
      backend_type = BackendType::SSD;
      break;

    case LoggingType::HDD_WBL:
      backend_type = BackendType::HDD;
      break;

    case LoggingType::NVM_WAL:
    case LoggingType::SSD_WAL:
    case LoggingType::HDD_WAL:
      backend_type = BackendType::MM;
      break;

    default:
      break;
  }

  return backend_type;
}

bool LoggingUtil::IsBasedOnWriteAheadLogging(const LoggingType &logging_type) {
  bool status = false;

  switch (logging_type) {
    case LoggingType::NVM_WAL:
    case LoggingType::SSD_WAL:
    case LoggingType::HDD_WAL:
      status = true;
      break;

    default:
      status = false;
      break;
  }

  return status;
}

bool LoggingUtil::IsBasedOnWriteBehindLogging(const LoggingType &logging_type) {
  bool status = true;

  switch (logging_type) {
    case LoggingType::NVM_WBL:
    case LoggingType::SSD_WBL:
    case LoggingType::HDD_WBL:
      status = true;
      break;

    default:
      status = false;
      break;
  }

  return status;
}

void LoggingUtil::FFlushFsync(FileHandle &file_handle) {
  // First, flush
  PELOTON_ASSERT(file_handle.fd != -1);
  if (file_handle.fd == -1) return;
  int ret = fflush(file_handle.file);
  if (ret != 0) {
    //  LOG_ERROR("Error occured in fflush(%s)", strerror(errno));
  }
  // Finally, sync
  ret = fsync(file_handle.fd);
  if (ret != 0) {
    //  LOG_ERROR("Error occured in fsync(%s)", strerror(errno));
  }
}

void LoggingUtil::FlushFdatasync(FileHandle &file_handle) {
  // First, flush
  PELOTON_ASSERT(file_handle.fd != -1);
  if (file_handle.fd == -1) return;
  int ret = fflush(file_handle.file);
  if (ret != 0) {
    //  LOG_ERROR("Error occured in fflush(%s)", strerror(errno));
  }
  // Finally, sync
  ret = fdatasync(file_handle.fd);
  if (ret != 0) {
    //  LOG_ERROR("Error occured in fdatasync(%s), try fsync",
    //  strerror(errno));
    ret = fsync(file_handle.fd);
  }
}

bool LoggingUtil::InitFileHandle(const char *name, FileHandle &file_handle,
                                 const char *mode) {
  auto file = fopen(name, mode);
  if (file == NULL) {
    //  LOG_ERROR("Checkpoint File is NULL");
    return false;
  } else {
    file_handle.file = file;
  }

  // also, get the descriptor
  auto fd = fileno(file);
  if (fd == INVALID_FILE_DESCRIPTOR) {
    //  LOG_ERROR("checkpoint_file_fd_ is -1");
    return false;
  } else {
    file_handle.fd = fd;
  }
  file_handle.size = 0;
  return true;
}

size_t LoggingUtil::GetLogFileSize(FileHandle &file_handle) {
  struct stat log_stats;
  fstat(file_handle.fd, &log_stats);
  return log_stats.st_size;
}

bool LoggingUtil::IsFileTruncated(FileHandle &file_handle,
                                  size_t size_to_read) {
  // Cache current position
  size_t current_position = ftell(file_handle.file);

  // Check if the actual file size is less than the expected file size
  // Current position + frame length
  if (current_position + size_to_read <= file_handle.size) {
    return false;
  } else {
    fseek(file_handle.file, 0, SEEK_END);
    return true;
  }
}

size_t LoggingUtil::GetNextFrameSize(FileHandle &file_handle) {
  size_t frame_size;
  char buffer[sizeof(int32_t)];

  // Check if the frame size is broken
  if (LoggingUtil::IsFileTruncated(file_handle, sizeof(buffer))) {
    return 0;
  }

  // Otherwise, read the frame size
  size_t ret = fread(buffer, 1, sizeof(int32_t), file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  // Read next 4 bytes as an integer
  CopySerializeInput frameCheck(buffer, sizeof(buffer));
  frame_size = (frameCheck.ReadInt()) + sizeof(buffer);

  // Move back by 4 bytes
  // So that tuple deserializer works later as expected
  int res = fseek(file_handle.file, -sizeof(buffer), SEEK_CUR);
  if (res == -1) {
    //  LOG_ERROR("Error occured in fseek ");
  }

  // Check if the frame is broken
  if (IsFileTruncated(file_handle, frame_size)) {
    return 0;
  }

  return frame_size;
}

LogRecordType LoggingUtil::GetNextLogRecordType(FileHandle &file_handle) {
  char buffer;

  // Check if the log record type is broken
  if (IsFileTruncated(file_handle, 1)) {
    //  LOG_TRACE("Log file is truncated");
    return LOGRECORD_TYPE_INVALID;
  }

  // Otherwise, read the log record type
  int ret = fread((void *)&buffer, 1, sizeof(char), file_handle.file);
  assert(ret > 0);

  CopySerializeInput input(&buffer, sizeof(char));
  LogRecordType log_record_type = (LogRecordType)(input.ReadEnumInSingleByte());

  return log_record_type;
}

int LoggingUtil::ExtractNumberFromFileName(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");
  if (start_index != std::string::npos) {
    int end_index = str.find_first_not_of("0123456789", start_index);
    return atoi(str.substr(start_index, end_index - start_index).c_str());
  }
  //  LOG_ERROR("The last found log file doesn't have a version number.");
  return 0;
}
bool LoggingUtil::ReadTableRecordHeader(TableRecord &table_record,
                                        FileHandle &file_handle) {
  // Check if frame is broken
  auto header_size = GetNextFrameSize(file_handle);
  if (header_size == 0) {
    //  LOG_ERROR("Header size is zero ");
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, header_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread");
  }

  CopySerializeInput table_header(header, header_size);
  table_record.DeserializeHeader(table_header);

  return true;
}

Schema *LoggingUtil::ReadTableRecordBody(FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    //  LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, body_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput schema_input(body, body_size);

  Schema *schema = LoggingUtil::DeserializeSchemaFrom(schema_input);
  // // We create a tuple based on the message
  // Tuple *tuple = new Tuple(schema, true);
  // //tuple->DeserializeFrom(tuple_body, pool);
  // LoggingUtil::DeserializeTupleFrom(tuple, tuple_body);
  return schema;
}

// for checkpoint
Schema *LoggingUtil::ReadTableRecordBody(FileHandle &file_handle,
                                         size_t &body_size) {
  // Check if the frame is broken
  body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    //  LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, body_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput schema_input(body, body_size);

  Schema *schema = LoggingUtil::DeserializeSchemaFrom(schema_input);
  // // We create a tuple based on the message
  // Tuple *tuple = new Tuple(schema, true);
  // //tuple->DeserializeFrom(tuple_body, pool);
  // LoggingUtil::DeserializeTupleFrom(tuple, tuple_body);
  return schema;
}

Schema *LoggingUtil::DeserializeSchemaFrom(SerializeInput &schema_input) {
  // deserialize columns
  // column string format
  // head:
  //   column count
  // body:
  //   type(TypeId), size(size_t), name(std::string), is_inlined(bool)
  auto fuck = schema_input.ReadInt();
  int column_count = schema_input.ReadInt();
  std::vector<Column> columns;
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const int type = schema_input.ReadInt();
    int32_t column_length = schema_input.ReadInt();
    std::string column_name = schema_input.ReadTextString();
    const bool is_inlined = schema_input.ReadBool();
    Column column((TypeId)type, column_length, column_name, is_inlined);
    columns.push_back(column);
  }
  // then construct shema
  Schema *schema = new Schema(columns);

  return schema;
}

void LoggingUtil::SerializeSchemaTo(Schema *schema, SerializeOutput &output) {
  size_t start = output.ReserveBytes(4);
  // serialize columns of schema
  int column_count = schema->GetColumnCount();
  output.WriteInt(column_count);
  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    Column c = schema->GetColumn(column_itr);
    const int type = static_cast<int>(c.GetType());
    int32_t column_length = c.GetLength();
    std::string column_name = c.GetName();
    const bool is_inlined = c.IsInlined();
    output.WriteInt(type);
    output.WriteInt(column_length);
    output.WriteTextString(column_name);
    output.WriteBool(is_inlined);
  }
  // body length
  output.WriteIntAt(
      start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

bool LoggingUtil::ReadTransactionRecordHeader(TransactionRecord &txn_record,
                                              FileHandle &file_handle) {
  // Check if frame is broken
  auto header_size = GetNextFrameSize(file_handle);
  if (header_size == 0) {
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, header_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput txn_header(header, header_size);
  txn_record.Deserialize(txn_header);

  return true;
}

bool LoggingUtil::ReadTupleRecordHeader(TupleRecord &tuple_record,
                                        FileHandle &file_handle) {
  // Check if frame is broken
  auto header_size = GetNextFrameSize(file_handle);
  if (header_size == 0) {
    //  LOG_ERROR("Header size is zero ");
    return false;
  }

  // Read header
  char header[header_size];
  size_t ret = fread(header, 1, header_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread");
  }

  CopySerializeInput tuple_header(header, header_size);
  tuple_record.DeserializeHeader(tuple_header);

  return true;
}

Tuple *LoggingUtil::ReadTupleRecordBody(const Schema *schema,
                                        AbstractPool *pool,
                                        FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    //  LOG_ERROR("Body size is zero ");
    return nullptr;
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, body_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  CopySerializeInput tuple_body(body, body_size);

  // We create a tuple based on the message
  Tuple *tuple = new Tuple(schema, true);
  // tuple->DeserializeFrom(tuple_body, pool);
  LoggingUtil::DeserializeTupleFrom(tuple, tuple_body);
  return tuple;
}

void LoggingUtil::DeserializeTupleFrom(Tuple *tuple, SerializeInput &input) {
  auto tuple_schema_ = tuple->GetSchema();
  PELOTON_ASSERT(tuple_schema_);
  PELOTON_ASSERT(tuple->GetData());

  auto temp = input.ReadInt();

  //  LOG_INFO("tuple size: %d", temp);

  const int column_count = tuple_schema_->GetColumnCount();

  for (int column_itr = 0; column_itr < column_count; column_itr++) {
    const auto type = tuple_schema_->GetType(column_itr);

    /**
     * DeserializeFrom is only called when we serialize/deserialize tables.
     * The serialization format for Strings/Objects in a serialized table
     * happens to have the same in memory representation as the
     * Strings/Objects in a Tuple. The goal here is to wrap the serialized
     * representation of the value in an Value and then serialize that into
     * the tuple from the Value. This makes it possible to push more value
     * specific functionality out of Tuple. The memory allocation will be
     * performed when serializing to tuple storage.
     */
    const bool is_inlined = tuple_schema_->IsInlined(column_itr);
    int32_t column_length;
    char *data_ptr = tuple->GetDataPtr(column_itr);

    if (is_inlined) {
      column_length = tuple_schema_->GetLength(column_itr);
    } else {
      column_length = tuple_schema_->GetVariableLength(column_itr);
    }

    const bool is_in_bytes = false;

    Value value = Value::DeserializeFrom(input, type, nullptr);
    tuple->SetValue(column_itr, value, nullptr);

    //      if (type == TypeId::INTEGER) {
    //          LOG_INFO("value: %d", value.GetIntData());
    //      }
    //    int32_t test = value.GetIntData();
  }
}

void LoggingUtil::SkipTableRecordBody(FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    //  LOG_ERROR("Body size is zero ");
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, body_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  // TODO Is it necessary?
  CopySerializeInput table_body(body, body_size);
}

void LoggingUtil::SkipTupleRecordBody(FileHandle &file_handle) {
  // Check if the frame is broken
  size_t body_size = GetNextFrameSize(file_handle);
  if (body_size == 0) {
    //  LOG_ERROR("Body size is zero ");
  }

  // Read Body
  char body[body_size];
  int ret = fread(body, 1, body_size, file_handle.file);
  if (ret <= 0) {
    //  LOG_ERROR("Error occured in fread ");
  }

  // TODO Is it necessary?
  CopySerializeInput tuple_body(body, body_size);
}

// Wrappers
DataTable *LoggingUtil::GetTable(TupleRecord &tuple_record) {
  // // Get db, table, schema to insert tuple
  // //auto catalog = Catalog::GetInstance();
  auto storageManager = StorageManager::GetInstance();

  Database *db = storageManager->GetDatabaseWithOid(
      ABC_DATABASE_INDEX);  // tuple_record.GetDatabaseOid()
  if (!db) {
    return nullptr;
  }
  PELOTON_ASSERT(db);

  //  LOG_TRACE("Table ID for this tuple: %d",
  //  (int)tuple_record.GetTableId());
  auto table = db->GetTableWithOid(tuple_record.GetTableId());
  if (!table) {
    return nullptr;
  }
  PELOTON_ASSERT(table);

  return table;
}

int LoggingUtil::GetFileCidFromFilName(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");

  assert(start_index != std::string::npos);
  int end_index = str.find_first_not_of("0123456789", start_index);
  return atoi(str.substr(start_index, end_index - start_index).c_str());
}

int LoggingUtil::GetFileSizeFromFileName(const char *file_name) {
  struct stat st;
  int ret_val;

  ret_val = stat(file_name, &st);
  if (ret_val == 0) return st.st_size;

  return -1;
}

bool LoggingUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
    //  LOG_TRACE("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
    //  LOG_TRACE("Directory %s already exists", dir_name);
  } else {
    //  LOG_TRACE("Creating directory failed: %s", strerror(errno));
    return false;
  }
  return true;
}

/**
 * @return false if fail to remove directory
 */
bool LoggingUtil::RemoveDirectory(const char *dir_name, bool only_remove_file) {
  struct dirent *file;
  DIR *dir;

  dir = opendir(dir_name);
  if (dir == nullptr) {
    return true;
  }

  // XXX readdir is not thread safe???
  while ((file = readdir(dir)) != nullptr) {
    if (strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0) {
      continue;
    }
    char complete_path[256];
    strcpy(complete_path, dir_name);
    strcat(complete_path, "/");
    strcat(complete_path, file->d_name);
    auto ret_val = remove(complete_path);
    if (ret_val != 0) {
      //  LOG_ERROR("Failed to delete file: %s, error: %s", complete_path,
      //          strerror(errno));
    }
  }
  closedir(dir);
  if (!only_remove_file) {
    auto ret_val = remove(dir_name);
    if (ret_val != 0) {
      // LOG_ERROR("Failed to delete dir: %s, error: %s", file->d_name,
      //           strerror(errno));
    }
  }
  return true;
}
