//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_buffer.cpp
//
// Identification: src/logging/log_buffer.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "log_buffer.h"
#include "log_manager.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/common/macros.h"

#include <cstring>


//===--------------------------------------------------------------------===//
// Log Buffer
//===--------------------------------------------------------------------===//
LogBuffer::LogBuffer(BackendLogger *backend_logger)
    : backend_logger_(backend_logger) {
  capacity_ = LogManager::GetInstance().GetLogBufferCapacity();
  elastic_data_.reset(new char[capacity_]);
}

LogBuffer::LogBuffer() {
  capacity_ = LogManager::GetInstance().GetLogBufferCapacity();
  elastic_data_.reset(new char[capacity_]);
  backend_logger_ = nullptr;
}

bool LogBuffer::WriteRecord(LogRecord *record) {
  bool success = WriteData(record->GetMessage(), record->GetMessageLength());
  return success;
}

void LogBuffer::ResetData() { size_ = 0; }

// Internal Methods
bool LogBuffer::WriteData(char *data, size_t len) {
  // Not enough space
  while (len + size_ > capacity_) {
    if (size_ == 0) {
      // double log buffer capacity for empty buffer
      capacity_ *= 2;
      elastic_data_.reset(new char[capacity_]);
    } else {
      return false;
    }
  }
  PELOTON_ASSERT(data);
  PELOTON_ASSERT(len);
  PELOTON_MEMCPY(elastic_data_.get() + size_, data, len);
  size_ += len;
  return true;
}

