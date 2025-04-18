//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint.cpp
//
// Identification: src/logging/checkpoint.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>
#include "sql/log.h"
#include "storage/peloton/logging/checkpoint.h"
#include "storage/peloton/logging/logging_util.h"
#include "storage/peloton/logging/checkpoint/simple_checkpoint.h"
#include "storage/peloton/logging/log_manager.h"
#include "storage/peloton/logging/checkpoint_manager.h"
#include "storage/peloton/store/tile.h"
#include "storage/peloton/store/database.h"
#include "storage/peloton/store/tuple.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/common/internal_types.h"
//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
/**
 * @brief MainLoop
 */
void Checkpoint::MainLoop(void) {
  auto &checkpoint_manager = CheckpointManager::GetInstance();
  /////////////////////////////////////////////////////////////////////
  // STANDBY MODE
  /////////////////////////////////////////////////////////////////////
  // LOG_TRACE("Checkpoint Standby Mode");

  // Standby before we need to do RECOVERY
  checkpoint_manager.WaitForModeTransition(CheckpointStatus::STANDBY, false);
  // Do recovery if we can, otherwise terminate
  switch (checkpoint_manager.GetCheckpointStatus()) {
    case CheckpointStatus::RECOVERY: {
      /////////////////////////////////////////////////////////////////////
      // RECOVERY MODE
      /////////////////////////////////////////////////////////////////////

      // First, do recovery if needed
//      chrono::steady_clock::time_point start = chrono::steady_clock::now();
      LOG_TRACE("start checkpoint recovery");
      DoRecovery();
      LOG_TRACE("end checkpoint recovery");
//      chrono::steady_clock::time_point end = chrono::steady_clock::now();
//      chrono::duration<double> dur = end - start;
//      sql_print_error("Checkpoint Recovery Done, cost %lf", dur.count());

      // LOG_TRACE("Checkpoint DoRecovery Done");
      checkpoint_status = CheckpointStatus::DONE_RECOVERY;
      break;
    }

    case CheckpointStatus::CHECKPOINTING: {
      // LOG_TRACE("Checkpoint Checkpointing Mode");
    } break;

    default:
      break;
  }

  checkpoint_manager.SetCheckpointStatus(CheckpointStatus::DONE_RECOVERY);
  checkpoint_manager.WaitForModeTransition(CheckpointStatus::CHECKPOINTING,
                                           true);

  /////////////////////////////////////////////////////////////////////
  // CHECKPOINTING MODE
  /////////////////////////////////////////////////////////////////////
  // Periodically, wake up and do checkpointing
  while (checkpoint_manager.GetCheckpointStatus() == CheckpointStatus::CHECKPOINTING) {
    checkpoint_status = CheckpointStatus::CHECKPOINTING;
    auto sleep_period = std::chrono::seconds(checkpoint_interval_);
    std::this_thread::sleep_for(sleep_period);


    sql_print_warning("Start Checkpoint");
    chrono::steady_clock::time_point start = chrono::steady_clock::now();

    DoCheckpoint();

    chrono::steady_clock::time_point end = chrono::steady_clock::now();
    chrono::duration<double> dur = end - start;
    sql_print_warning("End Checkpoint, time: %lf", dur);
  }
}

std::string Checkpoint::ConcatFileName(std::string checkpoint_directory,
                                       int version) {
  return checkpoint_directory + "/" + FILE_PREFIX + std::to_string(version) +
         FILE_SUFFIX;
}

void Checkpoint::InitDirectory() {
  checkpoint_dir = LogManager::GetInstance().GetFirstLogDirectoryName()+ "/" + checkpoint_dir;
  auto success = LoggingUtil::CreateDirectory(checkpoint_dir.c_str(), 0700);
  PELOTON_ASSERT(success);
}

std::unique_ptr<Checkpoint> Checkpoint::GetCheckpoint( CheckpointType checkpoint_type) {
  if (checkpoint_type == CheckpointType::NORMAL) {
    std::unique_ptr<Checkpoint> checkpoint(
        new SimpleCheckpoint());
    return checkpoint;
  }
  return std::unique_ptr<Checkpoint>(nullptr);
}

void Checkpoint::RecoverTuple(Tuple *tuple, DataTable *table,
                              ItemPointer target_location, cid_t commit_id) {
  auto tile_group_id = target_location.block;
  auto tuple_slot = target_location.offset;

  auto storage_manager = StorageManager::GetInstance();
  auto tile_group = storage_manager->GetTileGroup(tile_group_id);//this is the root cause

  // Create new tile group if table doesn't already have that tile group
  if (tile_group == nullptr) {
    table->AddTileGroupWithOidForRecovery(tile_group_id);
    tile_group = storage_manager->GetTileGroup(tile_group_id);
  }

  // Do the insert!
  tile_group->InsertTupleFromCheckpoint(tuple_slot, tuple, commit_id);

  PELOTON_ASSERT(inserted_tuple_slot != INVALID_OID);

  // not thread safe
  table->SetTupleCount(table->GetTupleCount() + 1);
  table->IncreaseRealTupleCount(1);
  table->IncreaseTupleCount(1);

  LOG_TRACE("Inserted a tuple from checkpoint: (%u, %u)", target_location.block, target_location.offset);
}

