//
// Created by zky on 23-7-10.
//
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <assert.h>
#include <string.h>
#include <cstdio>
#include <memory>
#include <algorithm>
#include <vector>

#include "log_storage.h"
// #include "storage/peloton/common/logger.h"
#include "storage_types.h"
#include "storage_util.h"
#include "logger.h"
// #include "logging_util.h"

LogStrorage::LogStrorage() {
  linfo_manager_ = std::move(
    std::unique_ptr<LogEntryInfoManager>(new LogEntryInfoManager()));
}

void LogStrorage::Init(LogStorageConfiguration config) {
  this->config_ = config;
  InitLogDirectory();
  InitLogFilesList();

  L_INFO("---------- Log storage initialized ----------");
}

void LogStrorage::AppendLogs(const std::vector<LogStorageEntry> &logs) {
  if (logs.empty()) {
    L_WARN("LogStrorage::AppendLogs: logs is empty");
    return;
  }

  if (logs[0].index() != LastLogIndex() + 1 
      || logs.back().index() != LastLogIndex() + logs.size()) {
    L_ERROR("LogStrorage::AppendLogs: index is not continuous");
  }

  if (cur_file_handle.fd == -1) {
    CreateNewLogFile(logs[0].safe_transaction_id(), logs[0].index());
    cur_file_info_table.reset();
    cur_file_info_table.SetIndex(logs[0].index());
  }

  // 1. 将日志写入到文件中，顺便添加到 linfo_manager_ 中
  for (auto &log : logs) {
    if (log.index() != LastLogIndex() + 1) {
      L_ERROR("LogStrorage::AppendLogs: index is not continuous");
    }

    // 1.1 记录该 LogEntry 的信息
    auto offset = ftell(cur_file_handle.file);
    cur_file_info_table.AddTermOffset(log.term(), offset);
    linfo_manager_->AddLogEntryInfo(log.info()); // FIXME: 能不能避免 copy
    linfo_manager_->SetLogPosition(log.index(), log_files_.size()-1, offset);

    // 1.2 在 logEntry 前写入 size, term，这里的 size 包括 term 和 data
    size_t size = log.data_size() + sizeof(int64_t);
    auto term = log.term();
    fwrite(&size, sizeof(size), 1, cur_file_handle.file);
    fwrite(&term, sizeof(term), 1, cur_file_handle.file);

    // write data to file
    size_t written = fwrite(log.data(), sizeof(char), log.data_size(), cur_file_handle.file);
  }

  // 2. 刷盘
  StorageUtil::FlushFdatasync(cur_file_handle);
  
  CloseLogFileIfNeeded();
}

// TODO: test
LogStorageEntry LogStrorage::GetLogEntry(uint64_t index) {
  auto info = linfo_manager_->GetLogEntryInfo(index);
  if (info.file_index == -1) {
    L_ERROR("LogStrorage::GetLogEntry: file_index is -1");
  }

  LogStorageEntry log_entry;
  log_entry.set_index(index);
  log_entry.set_term(info.term);

  char *data = nullptr;
  size_t size = 0;
  auto file_name = log_files_[info.file_index]->GetLogFileName();
  StorageUtil::ReadEntryFromFile(
    file_name.c_str(), info.entry_offset, data, size);
  log_entry.set_data(data, size);

  return log_entry;
}

int64_t LogStrorage::GetLogTerm(uint64_t index) {
  auto info = linfo_manager_->GetLogEntryInfo(index);
  return info.term;
}


int64_t LogStrorage::FirstLogIndex() { 
  if (!linfo_manager_) {
    L_ERROR("linfo_manager_ is nullptr");
  }
  return linfo_manager_->GetFirstLogIndex(); 
}

int64_t LogStrorage::LastLogIndex() { 
  if (!linfo_manager_) {
    L_ERROR("linfo_manager_ is nullptr");
  }
  return linfo_manager_->GetLastLogIndex(); 
}

void LogStrorage::TruncateSubfixLogs(int64_t index) {
  auto info = linfo_manager_->GetLogEntryInfo(index);
  if (info.file_index == -1) {
    L_ERROR("LogStrorage::GetLogEntry: file_index is -1");
  }


}

bool LogStrorage::GetNextLogFile(FileHandle & file_handle, uint64_t transaction_id) {
  (void)transaction_id; // FIXME: 加入检查点后，需要用这个参数筛掉不需要恢复的文件

  int file_index = recovery_file_index_.fetch_add(1);
  if (file_index >= log_files_.size()) {
    file_handle.reset();
    return false;
  }

  if (file_handle.file) {
    fclose(file_handle.file);
    file_handle.reset();
  }

  auto file_name = log_files_[file_index]->GetLogFileName();
  if (!StorageUtil::InitFileHandle(file_name.c_str(), file_handle, "rb")) {
    L_ERROR("Failed to open log file %s", file_name.c_str());
  }

  // 从最后一个日志文件恢复出 LogEntryInfoManager FIXME: 以后需要从全部文件恢复
  if (file_index <= log_files_.size() - 2) {
    this->linfo_manager_->clean();
    LogFileInfoTable temp_info_table;
    std::vector<LogEntryInfo> temp_log_entry_infos;
    if (!ExtractLogFileInfo(file_handle, temp_info_table)) {
      L_ERROR("Failed to extract log info from file %s", file_name.c_str());
    }
    auto term_offsets = temp_info_table.term_offsets();
    this->linfo_manager_->SetFirstLogIndex(temp_info_table.index());
    auto cur_index = temp_info_table.index();
    for (auto pair : term_offsets) {
      LogEntryInfo info;
      info.index = cur_index;
      info.term = pair.first;
      info.entry_offset = pair.second;
      info.file_index = file_index;
      linfo_manager_->AddLogEntryInfo(std::move(info));
      cur_index++;
    }
    L_INFO("Recover LogEntryInfoManager from file %s", file_name.c_str());
  }

  return true;
}

bool CompareByLogNumber(const std::unique_ptr<LogFile>& left, const std::unique_ptr<LogFile>& right) {
  return left->GetVersion() < right->GetVersion();
}

// TODO: test
void LogStrorage::InitLogFilesList() {
  struct dirent *file;
  DIR *dirp;
  int max_version = 0;
  std::string base_name = this->config_.LOG_FILE_PREFIX;

  // 1. 打开目录
  dirp = opendir(this->config_.log_directory.c_str());
  if (!dirp) {
    L_ERROR("Failed to open log directory %s", this->config_.log_directory.c_str());
  }

  // 2. 遍历所有日志文件，初始化 LogFile
  while ((file = readdir(dirp)) != nullptr) {
    if (strncmp(file->d_name, base_name.c_str(), base_name.length()) == 0) {
      L_INFO("Find a log file: %s", file->d_name);

      std::string file_name_with_dir = std::string(this->config_.log_directory) + "/" +
                                        std::string(file->d_name);
      this->log_files_.push_back(
        StorageUtil::ExtractFileInfoFromFileName(file_name_with_dir.c_str()));

      if (log_files_.back()->GetVersion() > max_version) {
        max_version = log_files_.back()->GetVersion();
      }

    } else {
      L_WARN("%s is not a log file", file->d_name);
    }
  }
  closedir(dirp);

  // 3. 按照 version number 排序所有日志文件
  auto num_log_files = this->log_files_.size();
  std::sort(this->log_files_.begin(), this->log_files_.end(),
            CompareByLogNumber);
  if (num_log_files > 0) {
    this->log_file_counter_ = max_version + 1;
  } else {
    this->log_file_counter_ = 0;
  }

  L_INFO("Found %d files, Log file counter is %d", num_log_files,  this->log_file_counter_);
}

// FIXME: 需要加入 term，index 等东西在文件名上么？
std::string LogStrorage::GetFileNameFromVersion(int version, uint64_t lower_bound_flushed_cid) {
  return std::string(this->config_.log_directory) + "/" + this->config_.LOG_FILE_PREFIX +
         std::to_string(version) + "_" + std::to_string(lower_bound_flushed_cid) + this->config_.LOG_FILE_SUFFIX;
}

// TODO: test
void LogStrorage::CreateNewLogFile(uint64_t lower_bound_flushed_cid, index_t log_index) {
  // 1. 获取文件名，创建文件
  auto new_file_name = 
    this->GetFileNameFromVersion(this->log_file_counter_, lower_bound_flushed_cid);
  FILE *new_log_file = fopen(new_file_name.c_str(), "wb");
  if (!new_log_file) {
    L_ERROR("Failed to create new log file %s", new_file_name.c_str());
  }

  cur_file_handle.file = new_log_file;
  cur_file_handle.fd = fileno(cur_file_handle.file);
  cur_file_handle.size = 0;
  if (cur_file_handle.fd == -1) {
    L_ERROR("Failed to create new log file %s", new_file_name.c_str());
  }

  // 2. 创建 LogFile 对象，加入到 log_files_ 中
  std::unique_ptr<LogFile>new_log_file_object(new LogFile(
    cur_file_handle,
    new_file_name,
    this->log_file_counter_,
    0));

  log_files_.push_back(std::move(new_log_file_object));

  fallocate(
    cur_file_handle.fd,
    FALLOC_FL_KEEP_SIZE, 0,
    this->config_.log_file_size);

  // 3. 将该日志文件的初始 log_index 写入文件首
  fwrite(&log_index, sizeof(index_t), 1, cur_file_handle.file);

  this->log_file_counter_++;

  L_INFO("Create new log file %s", new_file_name.c_str());
}

void LogStrorage::InitLogDirectory() {
  auto success =
      StorageUtil::CreateDirectoryRecursively(this->config_.log_directory.c_str(), 0700);
  if (!success) {
    L_ERROR("Failed to create log directory %s",
            this->config_.log_directory.c_str());
  }
}

// TODO: test
void LogStrorage::CloseLogFileIfNeeded() {
  if (cur_file_handle.fd == -1) {
    L_ERROR("No Log file is opened!");
    return;
  }

  struct stat stat_buf;
  fstat(cur_file_handle.fd, &stat_buf);
  cur_file_handle.size = stat_buf.st_size;

  if (cur_file_handle.size <= this->config_.log_file_size) {
    return;
  }
  
  // 记录写入前文件末尾的位置
  off_t linfo_manager_offset = ftell(cur_file_handle.file);

  // 1. 将序列化后的 linfo_manager_ 写入到文件末尾
  auto res = cur_file_info_table.Serialize();
  auto serialized_data = res.first;
  auto data_size = res.second;
  if (!serialized_data) {
    L_ERROR("No data to write to file");
  }
  ssize_t write_res = fwrite(serialized_data, sizeof(char), data_size, cur_file_handle.file);
  if(write_res != data_size){
    L_ERROR("Failed to write linfo_manager_ to file");
  }

  // 2. 写入 linfo_manager_offset 到文件末尾
  write_res = fwrite(&linfo_manager_offset, sizeof(char), sizeof(off_t), cur_file_handle.file);
  if (write_res != sizeof(off_t)){
    L_ERROR("Failed to write linfo_manager_offset to file");
  }

  // 3. 写入标识，表示该文件有LogInfo
  write_res = fwrite(&LOG_INFO_TABLE_ID, sizeof(LOG_INFO_TABLE_ID), 1, cur_file_handle.file);

  StorageUtil::FlushFdatasync(cur_file_handle);

  // 3. 关闭文件
  fclose(cur_file_handle.file);
  cur_file_handle.reset();
  cur_file_handle.size += data_size + sizeof(off_t); // 需要吗？
}


bool LogStrorage::ExtractLogFileInfo(FileHandle &file_handle, LogFileInfoTable &table) {
  if (file_handle.file == nullptr || file_handle.fd == INVALID_FILE_DESCRIPTOR) {
    L_ERROR("LogStrorage::ExtractLogInfo: invalid file handle");
    return false;
  }
  if (file_handle.size < sizeof(LOG_INFO_TABLE_ID) + sizeof(off_t)) {
    L_ERROR("LogStrorage::ExtractLogInfo: file size is too small");
    return false;
  }

  // 1. 检查文件末尾的 LOG_INFO_TABLE_ID
  int endID;
  fseek(file_handle.file, -sizeof(LOG_INFO_TABLE_ID), SEEK_END);
  fread(&endID, sizeof(LOG_INFO_TABLE_ID), 1, file_handle.file);
  if (endID != LOG_INFO_TABLE_ID) {
    L_WARN("No log info table in file, Rebuilding... ");
    return ReBuildLogFileInfo(file_handle, table);
  }

  // 2. 读取序列化内容的偏移
  off_t offset;
  fseek(file_handle.file, -sizeof(LOG_INFO_TABLE_ID) - sizeof(off_t), SEEK_END);
  fread(&offset, sizeof(off_t), 1, file_handle.file);
  if (offset >= file_handle.size) {
    L_ERROR("LogStrorage::ExtractLogInfo: offset is out of range");
    return false;  // 偏移超出文件大小
  }

  // 3. 读取序列化内容
  fseek(file_handle.file, offset, SEEK_SET);
  size_t contentSize = file_handle.size - offset - sizeof(LOG_INFO_TABLE_ID) - sizeof(off_t);
  char* content = new char[contentSize];
  fread(content, contentSize, 1, file_handle.file);

  // 4. 反序列化日志信息表
  table.DeSerialize(content);

  delete[] content;
  return true;
}


bool LogStrorage::ReBuildLogFileInfo(FileHandle &file_handle, LogFileInfoTable &table) {
  assert(file_handle.file);

  int64_t log_index = INVALID_LOG_INDEX;
  term_t term = INVALID_TERM;
  size_t log_size = 0;
  if (file_handle.size <= sizeof(log_index)) {
    L_ERROR("LogStrorage::ReBuildLogFileInfo: file size is too small");
    return false;  
  }

  // 1. 读取文件开头的日志索引
  fread(&log_index, sizeof(log_index), 1, file_handle.file);
  size_t curr_pos = sizeof(log_index);

  // 2. 读取所有日志的信息
  while (curr_pos < file_handle.size) {
    fread(&log_size, sizeof(log_size), 1, file_handle.file);
    curr_pos += sizeof(log_size);

    if (log_size > file_handle.size - curr_pos) {
      L_WARN("Found invalid log size");
      ftruncate(file_handle.fd, curr_pos);
      break;
    } else if (log_size == 0) {
      L_WARN("Got a empty LogEntry!");
    }

    // 读取term
    fread(&term, sizeof(term), 1, file_handle.file);
    curr_pos += sizeof(term);

    // 根据term和index添加信息到LogFileInfoTable
    table.AddTermOffset(term, log_index);

    // 跳过日志数据
    curr_pos += log_size - sizeof(term) - sizeof(log_size);
    L_INFO("cur_pos: %lu", curr_pos);
    fseek(file_handle.file, curr_pos, SEEK_CUR);

    // 更新日志索引
    log_index++;
  }

  if (curr_pos != file_handle.size) {
    L_ERROR("LogStrorage::ReBuildLogFileInfo: file size is not equal to curr_pos");
    return false; 
  }

  // 序列化LogFileInfoTable并将其写入文件末尾
  auto res = table.Serialize();
  fwrite(res.first, sizeof(char), res.second, file_handle.file);
  StorageUtil::FlushFdatasync(file_handle);

  L_INFO("Rebuild log info table, size of InfoTable: %lu", table.term_offsets().size());
  return true;
}

