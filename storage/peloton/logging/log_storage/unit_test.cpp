#include "gtest/gtest.h"
#include "log_storage.h"
#include "logger.h"
#include "storage_types.h"
#include "storage_util.h"

//===--------------------------------------------------------------------===//
// storage_util
//===--------------------------------------------------------------------===//

TEST(StorageUtilTest, DISABLED_ExtractNumberFromFileNameTest) {
    // You need to replace "file_name" with a real file name for testing
    const char* file_name = "file_name";
    int result = StorageUtil::ExtractNumberFromFileName(file_name);
    // You need to replace 0 with the expected result
    EXPECT_EQ(result, 0);
}

TEST(StorageUtilTest, DISABLED_GetFileCidFromFileNameTest) {
    const char* file_name = "file_name";
    int result = StorageUtil::GetFileCidFromFilName(file_name);
    EXPECT_EQ(result, 0);
}

TEST(StorageUtilTest, DISABLED_GetFileSizeFromFileNameTest) {
    const char* file_name = "file_name";
    int result = StorageUtil::GetFileSizeFromFileName(file_name);
    EXPECT_EQ(result, 0);
}

TEST(StorageUtilTest, DISABLED_CreateDirectoryTest) {
    const char* dir_name = "./CreateDirectoryTest";
    int mode = 0777;
    bool result = StorageUtil::CreateDirectory(dir_name, mode);
    EXPECT_TRUE(result);
}

TEST(StorageUtilTest, DISABLED_CreateDirectoryRecursivelyTest) {
    std::string dir_name = "./Create/Directory/Recursively/Test";
    int mode = 0777;
    bool result = StorageUtil::CreateDirectoryRecursively(dir_name, mode);
    EXPECT_TRUE(result);
}

TEST(StorageUtilTest, ReadEntryFromFile) {
    const char *test_file_name = "test_file.txt";
    off_t position = 0;
    size_t write_size = 0;
    size_t read_size = 0;
    char *buf;

    // Create a test file
    FILE *file = fopen(test_file_name, "wb");
    ASSERT_TRUE(file != nullptr);
    const char *test_data = "Hello, World!";
    write_size = strlen(test_data);
    fwrite(&write_size, sizeof(size_t), 1, file);
    fwrite(test_data, sizeof(char), strlen(test_data), file);
    fclose(file);

    EXPECT_TRUE(StorageUtil::ReadEntryFromFile(test_file_name, position, buf, read_size));

    // Check the result
    EXPECT_EQ(read_size, strlen(test_data));
    EXPECT_EQ(0, memcmp(buf, test_data, read_size));

    // Clean up
    delete[] buf;
    remove(test_file_name);
}


//===--------------------------------------------------------------------===//
// log_storage
//===--------------------------------------------------------------------===//
TEST(LogSorageTest, Initialize) {
    LogStrorage *log_storage = new LogStrorage();
    log_storage->Init();

    delete log_storage;
}

TEST(LogSorageTest, AppendLogsAndReadLogs_1) {
    // 1. Init 初始化
    LogStrorage *log_storage = new LogStrorage();
    log_storage->Init();

    // 2. 使用 AppendLogs 循环追加 500MB 的日志，每一次追加 10 个 LogEntry。
    char *data = new char[1024 * 1024]; // 1MB data
    std::vector<LogStorageEntry> entries;
    for (int i = 0; i < 50; ++i) {  // 500MB logs, each log is about 1MB
        for (int j = 0; j < 10; ++j) {    // 10 LogEntry per loop
            LogStorageEntry entry;
            entry.set_term(i * 10 + j);
            entry.set_index(i * 10 + j);
            entry.set_data(data, 1024 * 1024);
            entries.push_back(entry);
        }
        log_storage->AppendLogs(entries);
        entries.clear();
    }
    delete[] data;

    // 3. 释放该 log_storage
    delete log_storage;

    // 4. 重新 new 一个 log_storage
    log_storage = new LogStrorage();
    log_storage->Init();

    // 5. 使用 GetNextLogFile() 获取所有日志文件，直到其返回值为 false
    std::string log_file_name;
    FileHandle handle;
    while (log_storage->GetNextLogFile(handle)) {
        L_INFO("Got one log file");
    }

    delete log_storage;
}



//===--------------------------------------------------------------------===//
// LogFileInfoTable
//===--------------------------------------------------------------------===//

TEST(LogFileInfoTableTest, SerializeAndDeserialize) {
  LogFileInfoTable table;
  
  // 填充一些初始数据
  int64_t index = 10;
  std::vector<std::pair<int64_t, int64_t>> term_offsets = {{1, 10}, {2, 20}, {3, 30}};
  table.SetIndex(index);
  for (const auto& term_offset : term_offsets) {
    table.AddTermOffset(term_offset.first, term_offset.second);
  }

  // 序列化
  auto [data, size] = table.Serialize();

  // 创建新的对象进行反序列化
  LogFileInfoTable new_table;
  new_table.DeSerialize(data);

  // 验证新对象的状态与原对象一致
  ASSERT_EQ(index, new_table.index());
  ASSERT_EQ(term_offsets, new_table.term_offsets());
}


int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}