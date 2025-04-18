//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog.h
//
// Identification: src/include/catalog/catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <mutex>

#include "catalog_defaults.h"
#include "storage/peloton/function/functions.h"




class Constraint;
class Schema;
class DatabaseCatalogEntry;
class TableCatalogEntry;
class IndexCatalogEntry;
class SystemCatalogs;


class CodeContext;



class TransactionContext;



class Index;



class Database;
class DataTable;
class Layout;
class TableFactory;
class Tuple;



class AbstractPool;
class Value;
class ValueFactory;
class Value;



//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//

// information about functions (for FunctionExpression)
struct FunctionData {
  // name of the function
  std::string func_name_;
  // type of input arguments
  std::vector<TypeId> argument_types_;
  // function's return type
  TypeId return_type_;
  // indicates if PL/pgSQL udf
  bool is_udf_;
  // pointer to the function code_context (populated if UDF)
  std::shared_ptr<::CodeContext> func_context_;
  // pointer to the function (populated if built-in)
  BuiltInFuncType func_;
};

class Catalog {
 public:
  // Global Singleton
  static Catalog *GetInstance();

  // Bootstrap additional catalogs, only used in system initialization phase
  void Bootstrap();

  // Deconstruct the catalog database when destroying the catalog.
  ~Catalog();

  //===--------------------------------------------------------------------===//
  // CREATE FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Create a database
  ResultType CreateDatabase(TransactionContext *txn,
                            const std::string &database_name);

  // Create a schema(namespace)
  ResultType CreateSchema(TransactionContext *txn,
                          const std::string &database_name,
                          const std::string &schema_name);

  // Create a table in a database
  ResultType CreateTable(TransactionContext *txn,
                         const std::string &database_name,
                         const std::string &schema_name,
                         std::unique_ptr<Schema> schema,
                         const std::string &table_name,
                         bool is_catalog,
                         uint32_t tuples_per_tilegroup = DEFAULT_TUPLES_PER_TILEGROUP,
                         LayoutType layout_type = LayoutType::ROW);

  // Create index for a table
  ResultType CreateIndex(TransactionContext *txn,
                         const std::string &database_name,
                         const std::string &schema_name,
                         const std::string &table_name,
                         const std::string &index_name,
                         const std::vector<uint> &key_attrs,
                         bool unique_keys,
                         IndexType index_type);

  ResultType CreateIndex(TransactionContext *txn,
                         uint database_oid,
                         const std::string &schema_name,
                         uint table_oid,
                         bool is_catalog,
                         uint index_oid,
                         const std::string &index_name,
                         const std::vector<uint> &key_attrs,
                         bool unique_keys,
                         IndexType index_type,
                         IndexConstraintType index_constraint);


  /**
   * @brief   create a new layout for a table
   * @param   database_oid  database to which the table belongs to
   * @param   table_oid     table to which the layout has to be added
   * @param   column_map    column_map of the new layout to be created
   * @param   txn           TransactionContext
   * @return  shared_ptr    shared_ptr to the newly created layout in case of
   *                        success. nullptr in case of failure.
   */
  std::shared_ptr<const Layout> CreateLayout(TransactionContext *txn,
                                                      uint database_oid,
                                                      uint table_oid,
                                                      const column_map_type &column_map);

  /**
   * @brief   create a new layout for a table and make it the default if
   *          if the creating is successsful.
   * @param   database_oid  database to which the table belongs to
   * @param   table_oid     table to which the layout has to be added
   * @param   column_map    column_map of the new layout to be created
   * @param   txn           TransactionContext
   * @return  shared_ptr    shared_ptr to the newly created layout in case of
   *                        success. nullptr in case of failure.
   */
  std::shared_ptr<const Layout> CreateDefaultLayout(TransactionContext *txn,
                                                             uint database_oid,
                                                             uint table_oid,
                                                             const column_map_type &column_map);

  //===--------------------------------------------------------------------===//
  // SET FUNCTIONS FOR COLUMN CONSTRAINT
  //===--------------------------------------------------------------------===//

  // Set not null constraint for a column
  ResultType SetNotNullConstraint(TransactionContext *txn,
                                  uint database_oid,
                                  uint table_oid,
                                  uint column_id);

  // Set default constraint for a column
  ResultType SetDefaultConstraint(TransactionContext *txn,
                                  uint database_oid,
                                  uint table_oid,
                                  uint column_id,
                                  const Value &default_value);

  //===--------------------------------------------------------------------===//
  // ADD FUNCTIONS FOR TABLE CONSTRAINT
  //===--------------------------------------------------------------------===//

  // Add a new primary constraint for a table
  ResultType AddPrimaryKeyConstraint(TransactionContext *txn,
                                     uint database_oid,
                                     uint table_oid,
                                     const std::vector<uint> &column_ids,
                                     const std::string &constraint_name);

  // Add a new unique constraint for a table
  ResultType AddUniqueConstraint(TransactionContext *txn,
                                 uint database_oid,
                                 uint table_oid,
                                 const std::vector<uint> &column_ids,
                                 const std::string &constraint_name);

  // Add a new foreign key constraint for a table
  ResultType AddForeignKeyConstraint(TransactionContext *txn,
                                     uint database_oid,
                                     uint src_table_oid,
                                     const std::vector<uint> &src_col_ids,
                                     uint sink_table_oid,
                                     const std::vector<uint> &sink_col_ids,
                                     FKConstrActionType upd_action,
                                     FKConstrActionType del_action,
                                     const std::string &constraint_name);

  // Add a new check constraint for a table
  ResultType AddCheckConstraint(TransactionContext *txn,
                                uint database_oid,
                                uint table_oid,
                                const std::vector<uint> &column_ids,
                                const std::pair<ExpressionType, Value> &exp,
                                const std::string &constraint_name);

  //===--------------------------------------------------------------------===//
  // DROP FUNCTIONS
  //===--------------------------------------------------------------------===//
  // Drop a database with its name
  ResultType DropDatabaseWithName(TransactionContext *txn,
                                  const std::string &database_name);

  // Drop a database with its oid
  ResultType DropDatabaseWithOid(TransactionContext *txn,
                                 uint database_oid);

  // Drop a schema(namespace) using schema name
  ResultType DropSchema(TransactionContext *txn,
                        const std::string &database_name,
                        const std::string &schema_name);

  // Drop a table using table name
  ResultType DropTable(TransactionContext *txn,
                       const std::string &database_name,
                       const std::string &schema_name,
                       const std::string &table_name);

  // Drop a table, use this one in the future
  ResultType DropTable(TransactionContext *txn,
                       uint database_oid,
                       uint table_oid);

  // Drop an index, using its index_oid
  ResultType DropIndex(TransactionContext *txn,
                       uint database_oid,
                       uint index_oid);

  /** @brief   Drop layout
   * tile_groups
   * @param   database_oid    the database to which the table belongs
   * @param   table_oid       the table to which the layout belongs
   * @param   layout_oid      the layout to be dropped
   * @param   txn             TransactionContext
   * @return  ResultType(SUCCESS or FAILURE)
   */
  ResultType DropLayout(TransactionContext *txn,
                        uint database_oid,
                        uint table_oid,
                        uint layout_oid);

  // Drop not null constraint for a column
  ResultType DropNotNullConstraint(TransactionContext *txn,
                                   uint database_oid,
                                   uint table_oid,
                                   uint column_id);

  // Drop default constraint for a column
  ResultType DropDefaultConstraint(TransactionContext *txn,
                                   uint database_oid,
                                   uint table_oid,
                                   uint column_id);

  // Drop constraint for a table
  ResultType DropConstraint(TransactionContext *txn,
                            uint database_oid,
                            uint table_oid,
                            uint constraint_oid);

  //===--------------------------------------------------------------------===//
  // GET WITH NAME - CHECK FROM CATALOG TABLES, USING TRANSACTION
  //===--------------------------------------------------------------------===//

  /* Check database from pg_database with database_name using txn,
   * get it from storage layer using database_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  Database *GetDatabaseWithName(TransactionContext *txn,
                                         const std::string &db_name) const;

  /* Check table from pg_table with table_name & schema_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  DataTable *GetTableWithName(TransactionContext *txn,
                                       const std::string &database_name,
                                       const std::string &schema_name,
                                       const std::string &table_name);

  /* Check table from pg_database with database_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(TransactionContext *txn,
                                                                const std::string &database_name);

  std::shared_ptr<DatabaseCatalogEntry> GetDatabaseCatalogEntry(TransactionContext *txn,
                                                                uint database_oid);

  /* Check table from pg_table with table_name using txn,
   * get it from storage layer using table_oid,
   * throw exception and abort txn if not exists/invisible
   * */
  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(TransactionContext *txn,
                                                          const std::string &database_name,
                                                          const std::string &schema_name,
                                                          const std::string &table_name);

  std::shared_ptr<TableCatalogEntry> GetTableCatalogEntry(TransactionContext *txn,
                                                          uint database_oid,
                                                          uint table_oid);

  /*
   * Using database oid to get system catalog object
   */
  std::shared_ptr<SystemCatalogs> GetSystemCatalogs(uint database_oid);
  //===--------------------------------------------------------------------===//
  // DEPRECATED FUNCTIONS
  //===--------------------------------------------------------------------===//
  /*
   * We're working right now to remove metadata from storage level and eliminate
   * multiple copies, so those functions below will be DEPRECATED soon.
   */

  // Add a database
  void AddDatabase(Database *database);

  //===--------------------------------------------------------------------===//
  // BUILTIN FUNCTION
  //===--------------------------------------------------------------------===//

  void InitializeLanguages();

  void InitializeFunctions();

  void AddProcedure(TransactionContext *txn,
                    const std::string &name,
                    TypeId return_type,
                    const std::vector<TypeId> &argument_types,
                    uint prolang,
                    std::shared_ptr<::CodeContext> code_context,
                    const std::string &func_src);

  // TODO(Tianyu): Somebody should comment on what the difference between name
  //               and func_name is. I am confused.
  void AddBuiltinFunction(TransactionContext *txn,
                          const std::string &name,
                          BuiltInFuncType func,
                          const std::string &func_name,
                          TypeId return_type,
                          const std::vector<TypeId> &argument_types,
                          uint prolang);

  const FunctionData GetFunction(const std::string &name,
                                 const std::vector<TypeId> &argument_types);

 private:
  Catalog();

  void BootstrapSystemCatalogs(TransactionContext *txn,
                               Database *database);

  // The pool for new varlen tuple fields
  std::unique_ptr<AbstractPool> pool_;
  std::mutex catalog_mutex;
  // key: database oid
  // value: SystemCatalog object(including pg_table, pg_index and pg_attribute)
  std::unordered_map<uint, std::shared_ptr<SystemCatalogs>> catalog_map_;
};


