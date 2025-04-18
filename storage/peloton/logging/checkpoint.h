//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// checkpoint.h
//
// Identification: src/include/logging/checkpoint.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sys/stat.h>
#include <memory>
#include <string>

#include "storage/peloton/common/item_pointer.h"
#include "storage/peloton/type/tuple_value_pool.h"
#include "storage/peloton/common/internal_types.h"


class Tuple;
class DataTable;




//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
class Checkpoint {
  friend class CheckpointManager;

 public:
  Checkpoint() {
    pool.reset(new TupleValuePoll());
  }

  virtual ~Checkpoint(void) { pool.reset(); }

  // Main body of the checkpoint thread
  void MainLoop(void);

  // Do checkpoint periodically LogBuffer
  virtual void DoCheckpoint() = 0;

  // Do recovery from most recent version of checkpoint
  virtual void DoRecovery() = 0;

  void RecoverTuple(Tuple *tuple, DataTable *table,
                    ItemPointer target_location, cid_t commit_id);


  inline CheckpointStatus GetCheckpointStatus() { return checkpoint_status; }

 protected:
  std::string ConcatFileName(std::string checkpoint_dir, int version);

  void InitDirectory();

  // whether file access is disabled. mainly used for testing
  bool disable_file_access = false;

  // Default checkpoint interval (seconds)
  // TODO set interval to configurable variable
  int64_t checkpoint_interval_ = 1;

  // variable length memory pool
  // TODO better periodically clean up varlen pool?
  std::unique_ptr<AbstractPool> pool;

  // TODO set directory to configurable variables
  std::string checkpoint_dir = "peloton_checkpoint";

  // prefix for checkpoint file name
  std::string FILE_PREFIX = "peloton_checkpoint_";

  // suffix for checkpoint file name
  const std::string FILE_SUFFIX = ".log";

  // current status
  CheckpointStatus checkpoint_status = CheckpointStatus::INVALID;

 private:
  // Get a checkpoint
  static std::unique_ptr<Checkpoint> GetCheckpoint( CheckpointType checkpoint_type);
};

