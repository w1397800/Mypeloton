//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_checkpoint.cpp
//
// Identification: src/logging/checkpoint/simple_checkpoint.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <dirent.h>
#include <sys/mman.h>
#include <cstdio>
#include <numeric>

#include "storage/peloton/logging/backend_logger.h"
#include "storage/peloton/logging/checkpoint/simple_checkpoint.h"
#include "storage/peloton/logging/records/tuple_record.h"
#include "storage/peloton/logging/records/transaction_record.h"
#include "storage/peloton/logging/log_record.h"
#include "storage/peloton/logging/logging_util.h"

#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/store/logical_tile.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/common/container_tuple.h"
#include "storage/peloton/catalog/catalog.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/internal_types.h"
#include "storage/peloton/logging/log_manager.h"
#include "storage/peloton/logging/checkpoint_manager.h"
#include "storage/peloton/store/table_factory.h"

#include "sql/log.h"

//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

SimpleCheckpoint::SimpleCheckpoint()
    : Checkpoint(), logger_(nullptr) {
  InitDirectory();
}

void SimpleCheckpoint::DoCheckpoint() {
  // choose right edition
  checkpoint_version = 0;
  FILE_PREFIX = next_edition_ == A_EDITION ? FILE_PREFIX_A : FILE_PREFIX_B;

  // 关键
  auto &log_manager = LogManager::GetInstance();
  start_commit_id_ = log_manager.GetGlobalLowerBoundFlushedCid();

//  sql_print_warning("Checkpoint start_commit_id_, last_checkpoint_cid: %lu, %lu", start_commit_id_, last_checkpoint_cid);

  if (start_commit_id_ <= last_checkpoint_cid) {
    LOG_TRACE("nothing to checkpoint");
    return;
  }

  // Create first file for checkpoint
  FileHandle cur_file_handle;
  CreateFile(cur_file_handle);

  if (logger_ == nullptr) {
    logger_.reset(new BackendLogger());
  }

  // Add txn begin record
  start_epoch_id_ = start_commit_id_ >> 32;
  start_txn_id_ = start_commit_id_ - (start_epoch_id_ << 32);

  LOG_TRACE("Do Checkpoint (start_commit_id_, start_epoch_id_, start_txn_id_): %lu, %d, %d",
            start_commit_id_, start_epoch_id_, start_txn_id_);

  std::shared_ptr<LogRecord> begin_record(new TransactionRecord(LOGRECORD_TYPE_CP_BEGIN, start_commit_id_,start_epoch_id_,start_txn_id_));
  CopySerializeOutput begin_output_buffer;
  begin_record->Serialize(begin_output_buffer);
  AddRecordForFlush(begin_record.get(), cur_file_handle);

  // TODO: only support one database
  //auto catalog = Catalog::GetInstance();
  uint database_count = ABC_DATABASE_INDEX;//catalog->GetDatabaseCount();
  auto storageManager = StorageManager::GetInstance();

  // loop all databases
  for (uint database_idx = 0; database_idx <= database_count; database_idx++) {
    auto database = storageManager->GetDatabaseWithOid(database_idx);//catalog->GetDatabaseWithOffset(database_idx);
    auto database_oid = database->GetOid();
    auto table_count = database->GetTableCount();

    // loop all tables
    for (uint table_idx = 0; table_idx < table_count; table_idx++) {
      // Get the target table
      DataTable *target_table = database->GetTable(table_idx);
      assert(target_table);
      ScanOneTable(target_table, database_oid, cur_file_handle);
    }
  }

  Cleanup(cur_file_handle);

  SwapEdition();

  last_checkpoint_cid = start_commit_id_;
  LOG_TRACE("checkpoint done", start_commit_id_);
}

std::string SimpleCheckpoint::GetFullName(std::string file_name) {
  return this->checkpoint_dir + "/" + file_name;
}

void SimpleCheckpoint::SwapEdition() {
  //  flag file swap
  auto new_flag_filename = next_edition_ == A_EDITION ? FLAG_FILE_NAME_A : FLAG_FILE_NAME_B;

  std::string new_full_name = GetFullName(new_flag_filename);
  FILE *flag_file = fopen(new_full_name.c_str(), "wb"); // create a new flag file
  assert(flag_file);
  fclose(flag_file);

  if(!cur_flag_filename_.empty()) {  // delete old flag file
    auto res = remove(cur_flag_filename_.c_str());
    assert(res == 0);
  }
  cur_flag_filename_ = new_full_name;

  //  checkpoint file swap
  for (auto file : last_checkpoint_files_) {
    auto res = remove(file.file_name.c_str());
    assert (res == 0);
  }
  last_checkpoint_files_.clear();
  last_checkpoint_files_ = cur_checkpoint_files_;
  cur_checkpoint_files_.clear();

  //  edition change
  next_edition_ = next_edition_ == A_EDITION ? B_EDITION : A_EDITION;
}

void SimpleCheckpoint::ScanOneTable(
    DataTable *target_table, uint database_oid, FileHandle & cur_file_handle) {
  CopySerializeOutput output_buffer;

//  std::vector<ItemPointer> visible_tuple_locations;
  std::unique_ptr<LogRecord> record;
  std::unique_ptr<Tuple> tuple;
  auto schema = target_table->GetSchema();
  auto table_tile_group_count = target_table->GetTileGroupCount();

  // loop all tile_group
  for (unsigned int current_tile_group_offset = 0;
       current_tile_group_offset < table_tile_group_count;
       current_tile_group_offset++) {

    auto tile_group = target_table->GetTileGroup(current_tile_group_offset);
    auto active_tuple_count = tile_group->GetNextTupleSlot();
    auto tile_group_header = tile_group->GetHeader();

    // loop all slot of tile_group
    for (uint tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
      auto visibility = LoggingUtil::IsVisible(tile_group_header, tuple_id, start_commit_id_);
      if (!visibility) {
        continue ;
      }

      tuple.reset(new Tuple(schema, true));
      for (uint col = 0; col < schema->GetColumnCount(); col++) {
        Value val = (tile_group->GetValue(tuple_id, col));
        // TODO: 这个 pool 会不会是内存泄漏的地方
        tuple->SetValue(col, val, this->pool.get());
      }

      ItemPointer location(tile_group->GetTileGroupId(), tuple_id);
      record.reset(new TupleRecord(
          LOGRECORD_TYPE_TUPLE_INSERT, this->start_commit_id_,
          target_table->GetOid(), tuple.get(), database_oid));

      record->Serialize(output_buffer);
      AddRecordForFlush(record.get(), cur_file_handle);

      LOG_TRACE("add a new record from checkpoint: (%d, %d)", location.block, location.offset);
    }

    // 定期释放内存
    this->pool->Free(nullptr);
  }

}



void SimpleCheckpoint::AddRecordForFlush(LogRecord *record, FileHandle & file_handle) {
  auto res = flush_buffer_.WriteRecord(record);

  // buffer full, flush
  if (!res) {
    if (FileSwitchCondIsTrue(file_handle)) {
      fclose(file_handle.file);
      CreateFile(file_handle);
    }
    res = fwrite(flush_buffer_.GetData(), sizeof(char), flush_buffer_.GetSize(), file_handle.file);
    assert(res);

    // clear buffer;
    flush_buffer_.ResetData();
    // write the failed record
    res = flush_buffer_.WriteRecord(record);
    assert(res);
  }
}

// flush the flush_buffer_
void SimpleCheckpoint::Cleanup(FileHandle & file_handle) {
  auto res = fwrite(flush_buffer_.GetData(), sizeof(char), flush_buffer_.GetSize(), file_handle.file);
  assert(res);

  res = fclose(file_handle.file);
  assert(res == 0);

  // clear buffer;
  flush_buffer_.ResetData();
}

bool SimpleCheckpoint::FileSwitchCondIsTrue(FileHandle & file_handle) {
  struct stat stat_buf{};
  if (file_handle.fd == -1) return false;

  fstat(file_handle.fd, &stat_buf);
  file_handle.size = stat_buf.st_size;

  return file_handle.size >
         LogManager::GetInstance().GetLogFileSizeLimit() * 1024;
}

// thread safe
bool SimpleCheckpoint::GetNextFileForRecovery(FileHandle & file_handle, bool close_last_file) {
  if (close_last_file) {
    fclose(file_handle.file);
  }

  auto file_idx = cur_file_idx_.fetch_add(1);
  if (file_idx >= last_checkpoint_files_.size()) {
    return false;
  }

  auto cur_checkpoint_file_name =
      last_checkpoint_files_[file_idx].file_name;

  bool res =
      LoggingUtil::InitFileHandle(cur_checkpoint_file_name.c_str(), file_handle, "rb");
  assert(res);

  file_handle.size = LoggingUtil::GetLogFileSize(file_handle);
  return true;
}

void SimpleCheckpoint::DoRecovery() {

  LOG(FATAL) << "SimpleCheckpoint::DoRecovery() need rewrite!";
  return ;

  // InitCheckpointFilesList();
  // // open first file
  // FileHandle cur_file_handle;
  // auto res = GetNextFileForRecovery(cur_file_handle);

  // // No checkpoint to recover from
  // if (!res) {
  //   return;
  // }

  // // the first record must be LOGRECORD_TYPE_CP_BEGIN
  // auto record_type = LoggingUtil::GetNextLogRecordType(cur_file_handle);
  // assert(record_type == LOGRECORD_TYPE_CP_BEGIN);
  // TransactionRecord txn_rec(record_type);
  // res = LoggingUtil::ReadTransactionRecordHeader(txn_rec, cur_file_handle);
  // assert(res);
  // start_commit_id_ = txn_rec.GetTransactionId();
  // start_epoch_id_ = txn_rec.GetEpochId();
  // start_txn_id_=txn_rec.GetTxnId();

  // // create multi-thread for recovery, only works when recovery_thread_num_ > 1
  // std::vector<std::thread> recovery_threads;
  // for (int i = 1; i < recovery_thread_num_; i++) {
  //   recovery_threads.emplace_back(&SimpleCheckpoint::RecoveryFunc, this);
  // }

  // // this main thread also do something
  // while (true) {
  //   auto record_type = LoggingUtil::GetNextLogRecordType(cur_file_handle);
  //   if (record_type == LOGRECORD_TYPE_WAL_TUPLE_INSERT) {
  //     TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  //     // Check for torn log write
  //     auto res = LoggingUtil::ReadTupleRecordHeader(tuple_record, cur_file_handle);
  //     assert(res);

  //     auto table = LoggingUtil::GetTable(tuple_record);
  //     if (!table) {
  //       LOG_TRACE("Skip a record");
  //       LoggingUtil::SkipTupleRecordBody(cur_file_handle);
  //       break;
  //     }

  //     // Read off the tuple record body from the log
  //     std::unique_ptr<Tuple> tuple(LoggingUtil::ReadTupleRecordBody(
  //         table->GetSchema(), pool.get(), cur_file_handle));

  //     auto target_location = tuple_record.GetInsertLocation();
  //     auto tile_group_id = target_location.block;
  //     RecoverTuple(tuple.get(), table, target_location, start_commit_id_);

  //     if (max_tile_group_id_ < target_location.block) {
  //       max_tile_group_id_ = tile_group_id;
  //     }
  //   }
  //   else {
  //     auto res = GetNextFileForRecovery(cur_file_handle, true);
  //     // file ends
  //     if (!res) {
  //       break;
  //     }
  //   }
  // }// end while

  // // wait for son threads finish work
  // for (auto &trd : recovery_threads) {
  //   trd.join();
  // }

  // // After finishing recovery, set the next oid with maximum oid
  // // observed during the recovery
  // auto storage_manager = StorageManager::GetInstance();
  // if (max_tile_group_id_ > storage_manager->GetNextTileGroupId()) {
  //   storage_manager->SetNextTileGroupId(max_tile_group_id_);
  // }

  // // TransactionManagerFactory::GetInstance().SetNextCid(commit_id);
  // auto &log_manager = LogManager::GetInstance();
  // log_manager.UpdateCatalogAndTxnManagers(max_tile_group_id_, start_epoch_id_, start_txn_id_);
  // CheckpointManager::GetInstance().SetRecoveredCid(start_commit_id_);

  // last_checkpoint_cid = start_commit_id_;
}

void SimpleCheckpoint::RecoveryFunc() {
  LOG(FATAL) << "SimpleCheckpoint::RecoveryFunc() need rewrite!";
  // return; 

  // FileHandle cur_file_handle;
  // auto res = GetNextFileForRecovery(cur_file_handle);
  // if (!res) {
  //   return;
  // }

  // uint cur_max_tilegroup_id = 0;
  // LogRecordType record_type;
  // while (true) {
  //   record_type = LoggingUtil::GetNextLogRecordType(cur_file_handle);

  //   if (record_type == LOGRECORD_TYPE_WAL_TUPLE_INSERT) {
  //     TupleRecord tuple_record(LOGRECORD_TYPE_WAL_TUPLE_INSERT);

  //     // Check for torn log write
  //     auto res = LoggingUtil::ReadTupleRecordHeader(tuple_record, cur_file_handle);
  //     assert(res);

  //     auto table = LoggingUtil::GetTable(tuple_record);
  //     if (!table) {
  //       LOG_TRACE("Skip a record");
  //       LoggingUtil::SkipTupleRecordBody(cur_file_handle);
  //       break;
  //     }

  //     // Read off the tuple record body from the log
  //     std::unique_ptr<Tuple> tuple(LoggingUtil::ReadTupleRecordBody(
  //         table->GetSchema(), pool.get(), cur_file_handle));
  //     assert(tuple);

  //     auto target_location = tuple_record.GetInsertLocation();
  //     RecoverTuple(tuple.get(), table, target_location, this->start_commit_id_);

  //     cur_max_tilegroup_id = std::max(cur_max_tilegroup_id, target_location.block);
  //   }
  //   else {
  //     auto res = GetNextFileForRecovery(cur_file_handle, true);
  //     // file ends
  //     if (!res) {
  //       break;
  //     }
  //   }
  // }// end while

  // UpdateMaxTileGroupId(cur_max_tilegroup_id);
}

bool CompareByVersion(struct CheckpointFile left, struct CheckpointFile right) {
  return left.version_number < right.version_number;
}

bool CompareByLogNumber(class CheckpointFile left, class CheckpointFile right) {
  return left.version_number < right.version_number;
}

void SimpleCheckpoint::InitCheckpointFilesList() {
  this->cur_file_idx_ = 0;

  struct dirent *file;
  DIR *dirp;
  FILE *fp;
  FileHandle temp_file_handle;
  int version_number;
  // use for validate
  int max_version_A = 0;
  int max_version_B = 0;

  std::vector<CheckpointFile> checkpoint_files_A;
  std::vector<CheckpointFile> checkpoint_files_B;

  std::string flag_file_A;
  std::string flag_file_B;

  std::string full_name;
  //  LOG_TRACE("Trying to read log directory");
  dirp = opendir(this->checkpoint_dir.c_str());
  assert(dirp);

  while ((file = readdir(dirp)) != NULL) {
    // A.ok
    if (strncmp(file->d_name, this->FLAG_FILE_NAME_A.c_str(), this->FLAG_FILE_NAME_A.length()) == 0) {
      flag_file_A = GetFullName(file->d_name);
    }

    // B.ok
    else if (strncmp(file->d_name, this->FLAG_FILE_NAME_B.c_str(), this->FLAG_FILE_NAME_B.length()) == 0) {
      flag_file_B = GetFullName(file->d_name);
    }

    // A
    else if (strncmp(file->d_name, this->FILE_PREFIX_A.c_str(), this->FILE_PREFIX_A.length()) == 0) {
      version_number = LoggingUtil::ExtractNumberFromFileName(file->d_name);
      checkpoint_files_A.emplace_back(version_number, GetFullName(file->d_name));
      max_version_A = std::max(max_version_A, version_number);
    }

    // B
    else if (strncmp(file->d_name, this->FILE_PREFIX_B.c_str(), this->FILE_PREFIX_B.length()) == 0)  {
      version_number = LoggingUtil::ExtractNumberFromFileName(file->d_name);
      checkpoint_files_B.emplace_back(version_number, GetFullName(file->d_name));
      max_version_B = std::max(max_version_B, version_number);
    }
  }
  closedir(dirp);

  // remove old checkpoint last_checkpoint_files_
  if (!flag_file_A.empty()) {
    cur_flag_filename_ = flag_file_A;
    next_edition_ = B_EDITION;
    last_checkpoint_files_ = checkpoint_files_A;
    assert(checkpoint_files_A.size() == max_version_A+1);

    // remove old checkpoint last_checkpoint_files_
    for (const auto& cur_file : checkpoint_files_B) {
      auto res = remove(cur_file.file_name.c_str());
      assert(res == 0);
      LOG_INFO("remove one checkpoint file B");
    }

    // A, B 都有 ok_file, 取A版本，这种情况几乎不可能发生（在新版本完成后，会第一时间删除旧版本的 ok_file )
    if (!flag_file_B.empty()) {
      auto res = remove(flag_file_B.c_str());
      assert(res == 0);
    }

  } else if (!flag_file_B.empty()){
    cur_flag_filename_ = flag_file_B;
    next_edition_ = A_EDITION;
    last_checkpoint_files_ = checkpoint_files_B;
    assert(checkpoint_files_B.size() == max_version_B+1);

    for (const auto& cur_file : checkpoint_files_A) {
      auto res = remove(cur_file.file_name.c_str());
      assert(res == 0);
      LOG_INFO("remove one checkpoint file A");
    }

    // 没有 ok 文件，说明是第一次启动，或者是在第一次检查点时发生了崩溃（ok 文件只有在检查点完成后才更新）
    // 如果有检查点文件，这些检查点文件都是不完整的，需要删除
  } else {
    for (const auto& cur_file : checkpoint_files_A) {
      auto res = remove(cur_file.file_name.c_str());
      assert(res == 0);
      LOG_INFO("remove one checkpoint file A");
    }
    for (const auto& cur_file : checkpoint_files_B) {
      auto res = remove(cur_file.file_name.c_str());
      assert(res == 0);
      LOG_INFO("remove one checkpoint file B");
    }
  }

  std::sort(last_checkpoint_files_.begin(), last_checkpoint_files_.end(), CompareByLogNumber);
  LOG_TRACE("got %zu checkpoint last_checkpoint_files_", last_checkpoint_files_.size());
}


// Private Functions
void SimpleCheckpoint::CreateFile(FileHandle & file_handle) {
  auto version = checkpoint_version.fetch_add(1);
  std::string file_name = ConcatFileName(checkpoint_dir, version);
  bool res = LoggingUtil::InitFileHandle(file_name.c_str(), file_handle, "ab");
  assert(res);

  cur_checkpoint_files_.emplace_back(version, file_name);
}

// 竞争很小，所有可以直接用 mutex 上锁
void SimpleCheckpoint::UpdateMaxTileGroupId(uint new_id) {
  tile_group_id_lock_.lock();
  max_tile_group_id_ = std::max(max_tile_group_id_, new_id);
  tile_group_id_lock_.unlock();
}

