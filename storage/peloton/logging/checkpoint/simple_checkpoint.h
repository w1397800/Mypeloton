//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_checkpoint.h
//
// Identification: src/include/logging/checkpoint/simple_checkpoint.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <memory>
#include <thread>

#include "storage/peloton/logging/checkpoint.h"
#include "storage/peloton/logging/log_buffer.h"
// #include "storage/peloton/logging/log_file.h"
#include "storage/peloton/store/tile_group_header.h"
#include "storage/peloton/logging/log_storage/storage_types.h"
#include <mutex>

class LogRecord;
class BackendLogger;

#define  DEFAULT_RECOVERY_THREAD_NUMBER 1
//===--------------------------------------------------------------------===//
// Simple Checkpoint
//===--------------------------------------------------------------------===//

class SimpleCheckpoint : public Checkpoint {
 public:
  SimpleCheckpoint(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint &operator=(const SimpleCheckpoint &) = delete;
  SimpleCheckpoint(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint &operator=(SimpleCheckpoint &&) = delete;
  SimpleCheckpoint();

  // Inherited functions
  void DoCheckpoint() override;

  void DoRecovery() override;

  // single function for multi-thread recovery
  void RecoveryFunc();

  std::string GetFullName(std::string file_name);

  void SwapEdition();

  void ScanOneTable(
      DataTable *target_table, uint database_oid, FileHandle & cur_file_handle);

 private:

  bool GetNextFileForRecovery(FileHandle & handle, bool close_last_file = false);

  void CreateFile(FileHandle & handle);

  void Cleanup(FileHandle & cur_file_handle);

  void AddRecordForFlush(LogRecord *record, FileHandle & file_handle);

  static bool FileSwitchCondIsTrue(FileHandle & cur_file_handle);

  void InitCheckpointFilesList();

  // 竞争很小，所有可以直接用 mutex 上锁
  void UpdateMaxTileGroupId(uint new_id);

  std::unique_ptr<BackendLogger> logger_;

  // Keep tracking max oid for setting next_oid in manager
  // For active processing after recovery
  uint max_tile_group_id_ = 0;
  std::mutex tile_group_id_lock_;

  // commit id of current checkpoint
  cid_t start_commit_id_ = 0;
  cid_t start_epoch_id_ = 0;
  uint32_t start_txn_id_ = 0;

  // for flush TODO: find a better way.
  LogBuffer flush_buffer_;

  // the version of next checkpoint
  std::atomic_int checkpoint_version;

  // for multi_thread recovery
  std::atomic_int cur_file_idx_;

  // record last checkpoint cid when DoCheckpoint
  // use to decide if we should do a new checkpoint
  cid_t last_checkpoint_cid = INVALID_CID;

  int recovery_thread_num_ = DEFAULT_RECOVERY_THREAD_NUMBER;

//  std::vector<ItemPointer> visible_tuple_locations_;

  //===--------------------------------------------------------------------===//
  // edition management
  //===--------------------------------------------------------------------===//
  const std::string FILE_PREFIX_A = "peloton_checkpoint_A_";
  const std::string FILE_PREFIX_B = "peloton_checkpoint_B_";

  const std::string FLAG_FILE_NAME_A = "A.ok";
  const std::string FLAG_FILE_NAME_B = "B.ok";

  std::vector<CheckpointFile> last_checkpoint_files_;
  std::vector<CheckpointFile> cur_checkpoint_files_;

  std::string cur_flag_filename_;

  // 默认初始化为版本 A
  // 每一次产生新的检查点时，后缀变为另一版本
  // 有 .ok 文件的版本被认定为正确版本，如果同时存在 A.ok B.ok，以A.ok为准（todo：改成以时间新旧为准）
  CheckpointVersion next_edition_ = A_EDITION;

};

