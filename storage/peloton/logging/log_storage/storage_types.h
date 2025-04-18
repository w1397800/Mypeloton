#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <bitset>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <utility>
#include <vector>
#include <cstring>

#include "logger.h"

#define INVALID_TERM -1
#define INVALID_LOG_INDEX -1
#define INVALID_FILE_VERSION -1
static const int LOG_INFO_TABLE_ID = 54321; // 放在文件末尾，用于标识该文件有信息表

using fversion_t = int;
using term_t = int64_t;
using index_t = int64_t;;

//===--------------------------------------------------------------------===//
// Log Storage Configuration
//===--------------------------------------------------------------------===//

class LogStorageConfiguration {
 public:
  LogStorageConfiguration() = default;
  ~LogStorageConfiguration() = default;

  // FIXME: should it be absulute path?
  std::string log_directory = "./plogs";

  std::string LOG_FILE_PREFIX = "peloton_log_";

  std::string LOG_FILE_SUFFIX = ".log";

  int log_file_size = 1024 * 1024 * 128;  // 128MB
};

static const LogStorageConfiguration DEFAULT_LOG_STORAGE_CONFIGURATION;


//===--------------------------------------------------------------------===//
// File Handle
//===--------------------------------------------------------------------===//
static const int INVALID_FILE_DESCRIPTOR = -1;

struct FileHandle {
  // FILE pointer
  FILE *file = nullptr;

  // File descriptor
  int fd;

  // Size of the file
  size_t size;

  FileHandle() : file(nullptr), fd(-1), size(0) {}

  FileHandle(FILE *file, int fd, size_t size)
      : file(file), fd(fd), size(size) {}

  void reset() {
    file = nullptr;
    fd = INVALID_FILE_DESCRIPTOR;
    size = 0;
  }
};
static const FileHandle INVALID_FILE_HANDLE;

//===--------------------------------------------------------------------===//
// Log File
//===--------------------------------------------------------------------===//

class LogFile {
 public:
  LogFile(FileHandle file_handle, std::string log_file_name, int version,
          uint64_t max_cid_file)
      : file_handle_(std::move(file_handle)),
        log_file_name_(log_file_name),
        version_(version),
        max_cid_file_(max_cid_file){};

  virtual ~LogFile(void){};

  void SetMaxCidFile(uint64_t cid) {
    max_cid_file_ = cid;
  }

  uint64_t GetMaxCidFile() { return max_cid_file_; }

  int GetVersion() { return version_; }

  int GetLogNumber() { return version_; }

  const std::string &GetLogFileName() { return log_file_name_; }

  void SetLogFileSize(int log_file_size) {
    file_handle_.size = log_file_size;
  }

  void SetLogFileFD(int fd) { file_handle_.fd = fd; }

  void SetFilePtr(FILE *fp) { file_handle_.file = fp; }

 private:
  FileHandle file_handle_;
  std::string log_file_name_;
  int version_ = INVALID_FILE_VERSION;
  uint64_t max_cid_file_;
};

//===--------------------------------------------------------------------===//
// Log File Index Table
//===--------------------------------------------------------------------===//

// index
// term，offset
// term，offset
// ...
class LogFileInfoTable {
 public:
  static const size_t kDEFAULT_DATA_SIZE = 1024;
  LogFileInfoTable() {
    data_ = new char[kDEFAULT_DATA_SIZE];
  }
  ~LogFileInfoTable() {
    delete[] data_;
  }

  std::pair<const char*, size_t> Serialize() {
    // required_size index [term, offset] [term, offset] [term, offset] ... 
    if (term_offsets_.empty()) {
      L_WARN("LogFileInfoTable is empty!");
      return std::make_pair(nullptr, 0);
    }

    size_t required_size = sizeof(required_size) + sizeof(index_) 
                          + term_offsets_.size() * sizeof(std::pair<term_t, off_t>);
    if (required_size > kDEFAULT_DATA_SIZE) {
      delete[] data_;
      data_ = new char[required_size];
    }
    
    auto pos = data_;
    memcpy(pos, &required_size, sizeof(pos));
    pos += sizeof(size_t);
    memcpy(pos, &index_, sizeof(index_));
    pos += sizeof(index_);
    for (const auto& term_offset : term_offsets_) {
      memcpy(pos, &term_offset, sizeof(term_offset));
      pos += sizeof(term_offset);
    }

    return std::make_pair(data_, required_size);
  }

  void DeSerialize(const char *input_data) {
    auto temp = input_data;
    size_t data_size = 0;
    memcpy(&data_size, temp, sizeof(data_size));
    temp += sizeof(data_size);

    // DeSerialize index_
    memcpy(&index_, temp, sizeof(index_));
    temp += sizeof(index_);

    // DeSerialize term_offsets_
    term_offsets_.clear();
    while (temp < input_data + data_size) {
      std::pair<term_t, off_t> term_offset;
      memcpy(&term_offset, temp, sizeof(term_offset));
      temp += sizeof(term_offset);
      term_offsets_.push_back(term_offset);
    }
  }

  void SetIndex(index_t index) { index_ = index; }

  index_t index() const { return index_; }

  void AddTermOffset(term_t term, off_t offset) {
    term_offsets_.push_back(std::make_pair(term, offset));
  }

  const std::vector<std::pair<term_t, off_t>>& term_offsets() const { return term_offsets_; }

  void reset() {
    index_ = INVALID_LOG_INDEX;
    term_offsets_.clear();
  }

 private:
  index_t index_ = INVALID_LOG_INDEX;
  std::vector<std::pair<term_t, off_t>> term_offsets_;
  char* data_;
};


//===--------------------------------------------------------------------===//
// Storage State
//===--------------------------------------------------------------------===//


//===--------------------------------------------------------------------===//
// Log Entry Info
//===--------------------------------------------------------------------===//

class LogEntryInfo {
  public:
    LogEntryInfo() = default;
    ~LogEntryInfo() = default;
  
    term_t term = INVALID_TERM;
    index_t index = INVALID_LOG_INDEX; //TODO: 可以考虑不要，直接用数组下标做为 index
    off_t entry_offset = 0; // 在日志文件中的偏移
    int file_index = -1; // 该文件在 LogStorage::log_files_ 中的下标，-1 表示该索引不可用
};

//===--------------------------------------------------------------------===//
// Log Storage Entry
//===--------------------------------------------------------------------===//

class LogStorageEntry {
 public:
  LogStorageEntry() = default;
  ~LogStorageEntry() = default;

  index_t index() const { return info_.index; }
  term_t term() const { return info_.term; }
  uint32_t entry_offset() const { return info_.entry_offset; }
  char *data() const { return data_; }
  size_t data_size() const { return data_size_; }
  uint64_t safe_transaction_id() const { return safe_transaction_id_; }
  LogEntryInfo info() const { return info_; }

  void set_index(index_t index) { info_.index = index; }
  void set_term(term_t term) { info_.term = term; }

  void set_data(char *data, size_t data_size) {
    data_ = data;
    data_size_ = data_size;
  }

  void unsafe_release_data() {
    if (data_size_ != 0) {
      delete[] data_;
    }
  }

 private:
  LogEntryInfo info_;
  uint64_t safe_transaction_id_ = 0;
  size_t data_size_ = 0;
  
  char *data_;
};