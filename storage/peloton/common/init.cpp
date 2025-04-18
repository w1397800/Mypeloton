////===----------------------------------------------------------------------===//
////
////                         Peloton
////
//// init.cpp
////
//// Identification: src/common/init.cpp
////
//// Copyright (c) 2015-16, Carnegie Mellon University Database Group
////
////===----------------------------------------------------------------------===//
//
////#include "common/init.h"
//#include "init.h"
//
//#include <gflags/gflags.h>
//#include <google/protobuf/stubs/common.h>
//
////#include "catalog/catalog.h"
//#include "storage/peloton/catalog/catalog.h"
//// #include "common/statement_cache_manager.h"
//// #include "common/thread_pool.h"
//// #include "concurrency/transaction_manager_factory.h"
//// #include "gc/gc_manager_factory.h"
//// #include "index/index.h"
//// #include "settings/settings_manager.h"
//// #include "threadpool/mono_queue_pool.h"
//// #include "tuning/index_tuner.h"
//// #include "tuning/layout_tuner.h"
//
//
//
//#include "storage/peloton/gc/gc_manager_factory.h"
//#include "storage/peloton/index/index.h"
//#include "storage/peloton/settings/settings_manager.h"
//#include "storage/peloton/threadpool/mono_queue_pool.h"
//#include "storage/peloton/tuning/index_tuner.h"
//#include "storage/peloton/tuning/layout_tuner.h"
//
//
//namespace peloton {
//
//ThreadPool thread_pool;
//
//void PelotonInit::Initialize() {
//  CONNECTION_THREAD_COUNT = SettingsManager::GetInt(
//      SettingId::connection_thread_count);
//  LOGGING_THREAD_COUNT = 1;
//  GC_THREAD_COUNT = 1;
//  EPOCH_THREAD_COUNT = 1;
//
//  // set max thread number.
//  thread_pool.Initialize(0, CONNECTION_THREAD_COUNT + 3);
//
//  // start worker pool
//  MonoQueuePool::GetInstance().Startup();
//
//  // start indextuner thread pool
//  if (SettingsManager::GetBool(SettingId::brain)) {
//    MonoQueuePool::GetBrainInstance().Startup();
//  }
//
//  // start parallel execution pool
//  MonoQueuePool::GetExecutionInstance().Startup();
//
//  int parallelism = (CONNECTION_THREAD_COUNT + 3) / 4;
//  DataTable::SetActiveTileGroupCount(parallelism);
//  DataTable::SetActiveIndirectionArrayCount(parallelism);
//
//  // start epoch.
//  EpochManagerFactory::GetInstance().StartEpoch();
//
//  // start GC.
//  GCManagerFactory::Configure(SettingsManager::GetInt(SettingId::gc_num_threads));
//  GCManagerFactory::GetInstance().StartGC();
//
//  // start index tuner
//  if (SettingsManager::GetBool(SettingId::index_tuner)) {
//    // Set the default visibility flag for all indexes to false
//    IndexMetadata::SetDefaultVisibleFlag(false);
//    auto &index_tuner = IndexTuner::GetInstance();
//    index_tuner.Start();
//  }
//
//  // start layout tuner
//  if (SettingsManager::GetBool(SettingId::layout_tuner)) {
//    auto &layout_tuner = LayoutTuner::GetInstance();
//    layout_tuner.Start();
//  }
//
//  // Initialize catalog
//  auto pg_catalog = Catalog::GetInstance();
//  pg_catalog->Bootstrap();  // Additional catalogs
//  SettingsManager::GetInstance().InitializeCatalog();
//
//  // begin a transaction
//  auto &txn_manager = TransactionManagerFactory::GetInstance();
//  auto txn = txn_manager.BeginTransaction();
//
//  // initialize the catalog and add the default database, so we don't do this on
//  // the first query
//  pg_catalog->CreateDatabase(txn, DEFAULT_DB_NAME);
//
//  txn_manager.CommitTransaction(txn);
//
//  // Initialize the Statement Cache Manager
//  StatementCacheManager::Init();
//}
//
//void PelotonInit::Shutdown() {
//  // shut down index tuner
//  if (SettingsManager::GetBool(SettingId::index_tuner)) {
//    auto &index_tuner = IndexTuner::GetInstance();
//    index_tuner.Stop();
//  }
//
//  // shut down layout tuner
//  if (SettingsManager::GetBool(SettingId::layout_tuner)) {
//    auto &layout_tuner = LayoutTuner::GetInstance();
//    layout_tuner.Stop();
//  }
//
//  // shut down GC.
//  GCManagerFactory::GetInstance().StopGC();
//
//  // shut down epoch.
//  EpochManagerFactory::GetInstance().StopEpoch();
//
//  // shutdown execution thread pool
//  MonoQueuePool::GetExecutionInstance().Shutdown();
//
//  // stop worker pool
//  MonoQueuePool::GetInstance().Shutdown();
//
//  // stop indextuner thread pool
//  if (SettingsManager::GetBool(SettingId::brain)) {
//    MonoQueuePool::GetBrainInstance().Shutdown();
//  }
//
//  thread_pool.Shutdown();
//
//  // shutdown protocol buf library
//  google::protobuf::ShutdownProtobufLibrary();
//
//  // clear parameters
//  google::ShutDownCommandLineFlags();
//}
//
//void PelotonInit::SetUpThread() {}
//
//void PelotonInit::TearDownThread() {}
//
//}  // namespace peloton
