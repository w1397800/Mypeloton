//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database.cpp
//
// Identification: src/storage/database.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

//#include "storage/peloton/codegen/query_cache.h"
#include "storage/peloton/common/exception.h"
#include "storage/peloton/common/logger.h"
#include "storage/peloton/gc/gc_manager_factory.h"
#include "storage/peloton/index/index.h"
#include "storage/peloton/store/database.h"
#include "storage/peloton/store/table_factory.h"


Database::Database(const uint database_oid) : database_oid(database_oid) {}

Database::~Database() {
  // Clean up all the tables
  // LOG_TRACE("Deleting tables from database");
  for (auto table : tables) {
    delete table;
  }

  // LOG_TRACE("Finish deleting tables from database");
}

//===----------------------------------------------------------------------===//
// TABLE
//===----------------------------------------------------------------------===//

void Database::AddTable(DataTable *table, bool is_catalog) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);
    tables.push_back(table);

    if (is_catalog == false) {
      // Register table to GC manager.
      auto *gc_manager = &GCManagerFactory::GetInstance();
      assert(gc_manager != nullptr);
      gc_manager->RegisterTable(table->GetOid());
    }
  }
}

DataTable *Database::GetTableWithOid(const uint table_oid) const {
  for (auto *table : tables) {
    if (table->GetOid() == table_oid) {
      return table;
    }
  }

  // Table not found
  // throw CatalogException("Table with oid = " + std::to_string(table_oid) +
  //                        " is not found");
  return nullptr;
}

DataTable *Database::GetTableWithName(const std::string table_name) const {
  for (auto *table : tables) {
    if (table->GetName() == table_name) {
      return table;
    }
  }

  // Table not found
  return nullptr;
}

void Database::DropTableWithOid(const uint table_oid) {
  {
    std::lock_guard<std::mutex> lock(database_mutex);

    // Deregister table from GC manager.
    auto *gc_manager = &GCManagerFactory::GetInstance();
    PELOTON_ASSERT(gc_manager != nullptr);
    gc_manager->DeregisterTable(table_oid);

    // Deregister table from Query Cache manager
    //QueryCache::Instance().Remove(table_oid);

    uint table_offset = 0;
    for (auto table : tables) {
      if (table->GetOid() == table_oid) {
        delete table;
        break;
      }
      table_offset++;
    }
    PELOTON_ASSERT(table_offset < tables.size());

    // Drop the table
    tables.erase(tables.begin() + table_offset);
  }
}

DataTable *Database::GetTable(const uint table_offset) const {
  PELOTON_ASSERT(table_offset < tables.size());
  auto table = tables.at(table_offset);
  return table;
}

uint Database::GetTableCount() const { return tables.size(); }

//===----------------------------------------------------------------------===//
// UTILITIES
//===----------------------------------------------------------------------===//

// Get a string representation for debugging
const std::string Database::GetInfo() const {
  // std::ostringstream os;

  // os << GETINFO_THICK_LINE << std::endl;
  // os << "DATABASE(" << GetOid() << ") : \n";

  // uint table_count = GetTableCount();
  // os << "Table Count : " << table_count << std::endl;

  // uint table_itr = 0;
  // for (auto table : tables) {
  //   if (table != nullptr) {
  //     os << "(" << ++table_itr << "/" << table_count << ") "
  //        << "Table Oid : " << table->GetOid() << std::endl;

  //     uint index_count = table->GetIndexCount();

  //     if (index_count > 0) {
  //       os << "Index Count : " << index_count << std::endl;
  //       for (uint index_itr = 0; index_itr < index_count; index_itr++) {
  //         auto index = table->GetIndex(index_itr);
  //         if (index == nullptr) continue;

  //         switch (index->GetIndexType()) {
  //           case IndexConstraintType::PRIMARY_KEY:
  //             os << "primary key index \n";
  //             break;
  //           case IndexConstraintType::UNIQUE:
  //             os << "unique index \n";
  //             break;
  //           default:
  //             os << "default index \n";
  //             break;
  //         }

  //         os << *index << std::endl;
  //       }
  //     }

  //     if (table->GetSchema()->HasForeignKeys()) {
  //       os << "foreign tables \n";

  //       for (auto foreign_key : table->GetSchema()->GetForeignKeyConstraints()) {
  //         auto sink_table_oid = foreign_key->GetFKSinkTableOid();
  //         auto sink_table = GetTableWithOid(sink_table_oid);

  //         os << "table name : " << sink_table->GetName() << std::endl;
  //       }
  //     }
  //   }
  // }

  // os << GETINFO_THICK_LINE;

  // return os.str();
}

// deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
std::string Database::GetDBName() { return database_name; }

// deprecated, use catalog::DatabaseCatalog::GetInstance()->GetDatabaseName()
void Database::setDBName(const std::string &database_name) {
  Database::database_name = database_name;
}

