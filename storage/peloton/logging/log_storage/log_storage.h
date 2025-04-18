#pragma once

#include <fcntl.h>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

// #include "storage/peloton/logging/log_manager.h"
#include "storage_types.h"
#include "logger.h"

// FIXME: 搞清楚 raft 中第一个日志的索引是 1 还是 0， 这里假定是 0
class LogEntryInfoManager {
 public:
  LogEntryInfoManager() = default;
  ~LogEntryInfoManager() = default;

  // 在恢复后使用，从最后一个日志文件中恢复出此info
  void SetFirstLogIndex(index_t index) {  
    if (!entry_infos_.empty()) {
      L_ERROR("entry_infos_ is not empty");
    }
    first_log_index_ = index;
  }

  void AddLogEntryInfo(LogEntryInfo&& info) {
    if ((!entry_infos_.empty() && info.index != first_log_index_ + entry_infos_.size())
          || (entry_infos_.empty() && info.index != first_log_index_)) {
      L_ERROR("LogEntryInfoManager::AddLogEntryInfo: index is not continuous");
    }

    entry_infos_.push_back(std::move(info));
  }

  const LogEntryInfo &GetLogEntryInfo(index_t index) {
    if (index < first_log_index_ || index >= first_log_index_ + entry_infos_.size()) {
      L_ERROR("LogEntryInfoManager::GetLogEntryInfo: index is out of range");
    }

    return entry_infos_[index - first_log_index_];
  }

  index_t GetLastLogIndex() {
    return first_log_index_ + entry_infos_.size() - 1;
  }

  index_t GetFirstLogIndex() {
    return first_log_index_;
  }

  void SetLogPosition(index_t index, fversion_t file_index, off_t offset) {
    if (index < first_log_index_ || index >= first_log_index_ + entry_infos_.size()) {
      L_ERROR("LogEntryInfoManager::SetLogPosition: index is out of range");
    }

    entry_infos_[index - first_log_index_].file_index = file_index;
    entry_infos_[index - first_log_index_].entry_offset = offset;
  }

  // Test Only !!!
  void clean() {
    entry_infos_.clear();
    first_log_index_ = 0;
  }

 private:
  // FIXME: 改成指针，避免 copy
  std::vector<LogEntryInfo> entry_infos_;
  
  // 如果没有发生崩溃，那么这个值就是 0，表示日志下标就是日志 index
  index_t first_log_index_ = 0;
};



class LogStrorage {
  friend class LogStorageConfiguration;

 public:

  LogStrorage();

  ~LogStrorage() = default;

  //===--------------------------------------------------------------------===//
  // 外部接口： 共识协议
  //===--------------------------------------------------------------------===//
  
  // 1. 创建日志文件夹
  // 2. 获取所有存在的日志文件
  void Init(LogStorageConfiguration config = DEFAULT_LOG_STORAGE_CONFIGURATION);

  // FIXME: 参数有待商榷，这里肯定尽量不要做内存复制，需要兼容 raft 的日志
  // 1. Storage 不负责内存的释放
  // 2. LogEntry 存入文件时，需要在每个 LogEntry 前面添加 term 信息 [size, term, data]
  // 3. transaction id 不由 Storage 感知，且只有在新建文件时用到（那怎么知道当前的最小 transaction id？）
  void AppendLogs(const std::vector<LogStorageEntry> &Logs);

  // 获取单条日志记录，不负责内存的释放
  // 目前只支持从内存中的信息获取日志的位置，并从磁盘读取
  LogStorageEntry GetLogEntry(uint64_t index);

  // 获取日志的 term
  // 目前只支持从内存中的信息获取日志的位置，并从磁盘读取
  int64_t GetLogTerm(uint64_t index);

  // 获取日志索引
  int64_t FirstLogIndex();
  int64_t LastLogIndex();

  // TODO: 用于恢复后，Leader 覆盖 Follower 同一 index 的日志
  void TruncateSubfixLogs(int64_t index);

  // TODO: 检查点指向完成后，删除不需要的日志文件（可以放在最后做）
  void TruncatePrefixLogs(int64_t index);

  //===--------------------------------------------------------------------===//
  // 外部接口： 日志层 
  //===--------------------------------------------------------------------===//

  // transaction_id: 检查点恢复后，只需要获取 transaction_id >= 检查点之后的文件
  // 1. 需要在获取每个日志文件句柄时，读取它们的 LogEntry索引表，将 LogEntryInfoManager 恢复到内存中
  //    由于是多线程的，所以对日志文件的读取也是多线程，不会很慢
  // 2. 如果没有索引表，需要将这个日志文件的索引表恢复出来（只有最后一个日志文件没有索引表）
  bool GetNextLogFile(FileHandle & file_handle, uint64_t transaction_id = 0);

 private:

  void InitLogFilesList();

  // 功能：创建新的日志文件
  void CreateNewLogFile(uint64_t safe_transaction_id, index_t log_index);

  void InitLogDirectory();

  // 0. 判断当前日志文件是否达到限制大小，如果否，则直接返回
  // 1. 关闭前一个打开的文件，需要将 LogFileInfoTable 中的信息写入文件
  // 2. 并在文件末尾记录其偏移量
  void CloseLogFileIfNeeded();

  // 从指定的文件中解析出
  bool ExtractLogFileInfo(FileHandle &file_handle, LogFileInfoTable &table);

  // 遍历文件，从中恢复出 LogEntryInfoManager 
  bool ReBuildLogFileInfo(FileHandle &file_handle, LogFileInfoTable &table);

  //===--------------------------------------------------------------------===//
  // 工具函数
  //===--------------------------------------------------------------------===//

  // 创建新的日志文件
  std::string GetFileNameFromVersion(int version, uint64_t lower_bound_flushed_cid);



  //===--------------------------------------------------------------------===//
  // 私有成员
  //===--------------------------------------------------------------------===//

  LogStorageConfiguration config_;

  // list of log files
  std::vector<std::unique_ptr<LogFile>> log_files_;

  // File pointer and descriptor
  FileHandle cur_file_handle;
  LogFileInfoTable cur_file_info_table;
  

  // log file version number, used for create new log file
  int log_file_counter_;
  // 恢复时，需要通过 GetNextLogFile() 遍历所有日志文件
  std::atomic_int recovery_file_index_{0};

  std::unique_ptr<LogEntryInfoManager> linfo_manager_ = nullptr;
};



