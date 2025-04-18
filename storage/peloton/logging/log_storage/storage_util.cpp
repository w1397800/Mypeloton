
#include "storage_util.h"
#include <assert.h>
#include <sys/stat.h>
#include <cstddef>
#include <memory>
#include <fcntl.h>

#include "logger.h"
#include "storage_types.h"

int StorageUtil::ExtractNumberFromFileName(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");
  if (start_index != std::string::npos) {
    int end_index = str.find_first_not_of("0123456789", start_index);
    return atoi(str.substr(start_index, end_index - start_index).c_str());
  }
   L_ERROR("The last found log file doesn't have a version number.");
  return 0;
}

int StorageUtil::GetFileCidFromFilName(const char *name) {
  std::string str(name);
  size_t start_index = str.find_first_of("0123456789");

  assert(start_index != std::string::npos);
  int end_index = str.find_first_not_of("0123456789", start_index);
  return atoi(str.substr(start_index, end_index - start_index).c_str());
}

int StorageUtil::GetFileSizeFromFileName(const char *file_name) {
  struct stat st;
  int ret_val;

  ret_val = stat(file_name, &st);
  if (ret_val == 0) return st.st_size;

  return -1;
}

bool StorageUtil::CreateDirectory(const char *dir_name, int mode) {
  int return_val = mkdir(dir_name, mode);
  if (return_val == 0) {
     L_INFO("Created directory %s successfully", dir_name);
  } else if (errno == EEXIST) {
     L_INFO("Directory %s already exists", dir_name);
  } else {
     L_INFO("Creating directory failed");
    return false;
  }
  return true;
}


bool StorageUtil::CreateDirectoryRecursively(const std::string& dir_name, int mode) {
  struct stat st;
  int status = 0;

  // Check if directory already exists
  if (stat(dir_name.c_str(), &st) != -1) {
    // The path exists, check if it is a directory
    if (S_ISDIR(st.st_mode)) {
        return true;
    } else {
      L_ERROR("Path %s exists but is not a directory", dir_name.c_str());
    }
  }

  // Extract parent directory from the path
  std::size_t pos = dir_name.find_last_of('/');
  if (pos == std::string::npos) {
    return (mkdir(dir_name.c_str(), mode) == 0);
  }

  // Create the parent directory
  if (!CreateDirectoryRecursively(dir_name.substr(0, pos), mode)) {
    L_ERROR("Failed to create parent directory %s", dir_name.substr(0, pos).c_str());
    return false;
  }

  // Then create the current directory
  return (mkdir(dir_name.c_str(), mode) == 0);
}


void StorageUtil::FlushFdatasync(FileHandle &file_handle) {
  // 1. flush
  if (file_handle.fd == -1) {
    L_ERROR("File handle is not valid");
    return;
  }

  int ret = fflush(file_handle.file);
  if (ret != 0) {
    L_ERROR("Error occured in fflush(%s)", strerror(errno));
  }
  // 2. sync
  ret = fdatasync(file_handle.fd);
  if (ret != 0) {
    L_ERROR("Error occured in fdatasync(%s)", strerror(errno));
    ret = fsync(file_handle.fd);
  }
}


std::unique_ptr<LogFile> StorageUtil::ExtractFileInfoFromFileName(const char *name) {
  auto version_number = StorageUtil::ExtractNumberFromFileName(name);

  auto temp_max_cid_file = StorageUtil::GetFileCidFromFilName(name);

  FileHandle temp_file_handle;
  temp_file_handle.fd = -1;
  temp_file_handle.file = nullptr;
  temp_file_handle.size =
    StorageUtil::GetFileSizeFromFileName(name);

  return std::move(std::unique_ptr<LogFile> (new LogFile(
    temp_file_handle,
    name,
    version_number,
    temp_max_cid_file)));
}


bool StorageUtil::ReadEntryFromFile(const char *file_name, off_t position, char *&buf, size_t &size) {
  // 打开文件
  FILE *file = fopen(file_name, "rb");
  if (file == nullptr) {
    L_ERROR("Error opening file %s", file_name);
    return false;
  }

  // 定位到指定的位置
  if (fseek(file, position, SEEK_SET) != 0) {
    L_ERROR("Error seeking file %s", file_name);
    fclose(file);
    return false;
  }

  // 读取 size
  fread(&size, sizeof(size_t), 1, file);

  // 读取数据
  buf = new char[size];
  size_t result = fread(buf, sizeof(char), size, file);
  if (result != size) {
    L_ERROR("Error reading file");
    fclose(file);
    return false;
  }

  // 关闭文件
  fclose(file);
  return true;
}


bool TruncateFileAtPosition(const char *file_name, size_t position) {
    // Open file
    FILE *file = fopen(file_name, "r+b");
    if (file == nullptr) {
        perror("Error opening file");
        return false;
    }

    // Get the file descriptor
    int fd = fileno(file);

    // Truncate the file at the given position
    if (ftruncate(fd, position) != 0) {
        perror("Error truncating file");
        fclose(file);
        return false;
    }

    // Close the file
    fclose(file);
    return true;
}

bool StorageUtil::InitFileHandle(const char *name, FileHandle &file_handle,
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
  struct stat stat_buf;
  fstat(file_handle.fd, &stat_buf);
  file_handle.size = stat_buf.st_size;
  return true;
}