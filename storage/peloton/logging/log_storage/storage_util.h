#pragma once

#include <fcntl.h>
#include <memory>
#include "storage_types.h"

class StorageUtil {
 public:
  static int ExtractNumberFromFileName(const char* name);
  static int GetFileCidFromFilName(const char* name);
  static int GetFileSizeFromFileName(const char*);
  static bool CreateDirectory(const char *dir_name, int mode);
  static bool CreateDirectoryRecursively(const std::string& dir_name, int mode);
  static void FlushFdatasync(FileHandle &file_handle);
  static std::unique_ptr<LogFile> ExtractFileInfoFromFileName(const char *name);
  static bool ReadEntryFromFile(const char* file_name, off_t position, char* &buf, size_t &size);
  static bool InitFileHandle(const char *name, FileHandle &file_handle,
                                 const char *mode);
};