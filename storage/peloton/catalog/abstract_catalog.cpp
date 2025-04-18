//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_catalog.cpp
//
// Identification: src/catalog/abstract_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "abstract_catalog.h"

#include "storage/peloton/common/statement.h"

#include "catalog.h"
#include "database_catalog.h"
#include "table_catalog.h"

#include "storage/peloton/index/index_factory.h"
#include "storage/peloton/optimizer/optimizer.h"
#include "storage/peloton/parser/postgresparser.h"

#include "storage/peloton/planner/create_plan.h"
#include "storage/peloton/planner/delete_plan.h"
#include "storage/peloton/planner/index_scan_plan.h"
#include "storage/peloton/planner/insert_plan.h"
#include "storage/peloton/planner/seq_scan_plan.h"

#include "storage/peloton/executor/executor_context.h"
#include "storage/peloton/executor/create_executor.h"
#include "storage/peloton/executor/delete_executor.h"
#include "storage/peloton/executor/index_scan_executor.h"
#include "storage/peloton/executor/insert_executor.h"
#include "storage/peloton/executor/plan_executor.h"
#include "storage/peloton/executor/seq_scan_executor.h"
#include "storage/peloton/executor/update_executor.h"
#include "storage/peloton/expression/constant_value_expression.h"

#include "storage/peloton/store/database.h"
#include "storage/peloton/store/storage_manager.h"
#include "storage/peloton/store/table_factory.h"


AbstractCatalog::AbstractCatalog(Database *pg_catalog,
                                 Schema *catalog_table_schema,
                                 uint catalog_table_oid,
                                 std::string catalog_table_name) {
  // set database_oid
  database_oid_ = pg_catalog->GetOid();
  // Create catalog_table_
  catalog_table_ = TableFactory::GetDataTable(
      database_oid_, catalog_table_oid, catalog_table_schema, catalog_table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, true, false, true);
  // Add catalog_table_ into pg_catalog database
  pg_catalog->AddTable(catalog_table_, true);
}

AbstractCatalog::AbstractCatalog(TransactionContext *txn,
                                 const std::string &catalog_table_ddl) {
  // Execute create catalog table
  auto &peloton_parser = PostgresParser::GetInstance();
  std::unique_ptr<ExecutorContext> context(
      new ExecutorContext(txn));
  auto create_plan = std::dynamic_pointer_cast<CreatePlan>(
     Optimizer().BuildPelotonPlanTree(
          peloton_parser.BuildParseTree(catalog_table_ddl), txn));
  CreateExecutor executor(create_plan.get(), context.get());

  executor.Init();
  executor.Execute();

  // get catalog table oid
  auto catalog_table_object =
      Catalog::GetInstance()->GetTableCatalogEntry(txn,
                                                   create_plan->GetDatabaseName(),
                                                   create_plan->GetSchemaName(),
                                                   create_plan->GetTableName());

  // set catalog_table_
//   try {
    catalog_table_ = StorageManager::GetInstance()->GetTableWithOid(
        catalog_table_object->GetDatabaseOid(),
        catalog_table_object->GetTableOid());
    // set database_oid
    database_oid_ = catalog_table_object->GetDatabaseOid();
  //} catch (CatalogException &e) {
    // LOG_TRACE("Can't find table %d! Return false",
    //           catalog_table_object->GetTableOid());
  //}
//}

/*@brief   insert tuple(reord) helper function
 * @param   tuple     tuple to be inserted
 * @param   txn       TransactionContext
 * @return  Whether insertion is Successful
 */
bool AbstractCatalog::InsertTuple(TransactionContext *txn,
                                  std::unique_ptr<Tuple> tuple) {
  if (txn == nullptr)
    // throw CatalogException("Insert tuple requires transaction");

  std::vector<Value> params;
  std::vector<std::string> columns;
  std::vector<std::vector<std::unique_ptr<AbstractExpression>>>
      values;
  values.push_back(
      std::vector<std::unique_ptr<AbstractExpression>>());
  std::vector<int> result_format(tuple->GetSchema()->GetColumnCount(), 0);
  for (size_t i = 0; i < tuple->GetSchema()->GetColumnCount(); i++) {
    params.push_back(tuple->GetValue(i));
    columns.push_back(tuple->GetSchema()->GetColumn(i).GetName());
    values[0].emplace_back(
        new ConstantValueExpression(tuple->GetValue(i)));
  }
  auto node =
      std::make_shared<InsertPlan>(catalog_table_, &columns, &values);

  ExecutionResult this_p_status;
  auto on_complete = [&this_p_status](
                         ExecutionResult p_status,
                         std::vector<ResultValue> &&values UNUSED_ATTRIBUTE) {
    this_p_status = p_status;
  };

  PlanExecutor::ExecutePlan(node, txn, params, result_format,
                                      on_complete);

  return this_p_status.m_result == ResultType::SUCCESS;
}

/*@brief   Delete a tuple using index scan
 * @param   index_offset  Offset of index for scan
 * @param   values        Values for search
 * @param   txn           TransactionContext
 * @return  Whether deletion is Successful
 */
bool AbstractCatalog::DeleteWithIndexScan(TransactionContext *txn,
                                          uint index_offset,
                                          std::vector<Value> values) {
  if (txn == nullptr)
    // throw CatalogException("Delete tuple requires transaction");

  std::unique_ptr<ExecutorContext> context(
      new ExecutorContext(txn));

  // Delete node
  DeletePlan delete_node(catalog_table_);
  DeleteExecutor delete_executor(&delete_node, context.get());

  // Index scan as child node
  std::vector<uint> column_offsets;  // No projection
  auto index = catalog_table_->GetIndex(index_offset);
  PELOTON_ASSERT(index != nullptr);
  std::vector<uint> key_column_offsets =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PELOTON_ASSERT(values.size() == key_column_offsets.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<AbstractExpression *> runtime_keys;

  IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_offsets, expr_types, values, runtime_keys);

  std::unique_ptr<IndexScanPlan> index_scan_node(
      new IndexScanPlan(catalog_table_, nullptr, column_offsets,
                                 index_scan_desc));

  IndexScanExecutor index_scan_executor(index_scan_node.get(),
                                                  context.get());

  // Parent-Child relationship
  delete_node.AddChild(std::move(index_scan_node));
  delete_executor.AddChild(&index_scan_executor);
  delete_executor.Init();
  bool status = delete_executor.Execute();

  return status;
}

/*@brief   Index scan helper function
 * @param   column_offsets    Column ids for search (projection)
 * @param   index_offset      Offset of index for scan
 * @param   values            Values for search
 * @param   txn               TransactionContext
 * @return  Unique pointer of vector of logical tiles
 */
std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
AbstractCatalog::GetResultWithIndexScan(
    TransactionContext *txn,
    std::vector<uint> column_offsets,
    uint index_offset,
    std::vector<Value> values) const {
//   if (txn == nullptr) throw CatalogException("Scan table requires transaction");

  // Index scan
  std::unique_ptr<ExecutorContext> context(
      new ExecutorContext(txn));

  auto index = catalog_table_->GetIndex(index_offset);
  std::vector<uint> key_column_offsets =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();
  PELOTON_ASSERT(values.size() == key_column_offsets.size());
  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<AbstractExpression *> runtime_keys;

  IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_offsets, expr_types, values, runtime_keys);

  IndexScanPlan index_scan_node(catalog_table_, nullptr,
                                         column_offsets, index_scan_desc);

  IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());

  // Execute
  index_scan_executor.Init();
  std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
      result_tiles(new std::vector<std::unique_ptr<LogicalTile>>());

  while (index_scan_executor.Execute()) {
    result_tiles->push_back(std::unique_ptr<LogicalTile>(
        index_scan_executor.GetOutput()));
  }

  return result_tiles;
}

/*@brief   Sequential scan helper function
 * NOTE: try to use efficient index scan instead of sequential scan, but you
 * shouldn't build too many indexes on one catalog table
 * @param   column_offsets    Column ids for search (projection)
 * @param   predicate         predicate for this sequential scan query
 * @param   txn               TransactionContext
 *
 * @return  Unique pointer of vector of logical tiles
 */
std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
AbstractCatalog::GetResultWithSeqScan(
    TransactionContext *txn,
    AbstractExpression *predicate,
    std::vector<uint> column_offsets) {
//   if (txn == nullptr) throw CatalogException("Scan table requires transaction");

  // Sequential scan
  std::unique_ptr<ExecutorContext> context(
      new ExecutorContext(txn));

  SeqScanPlan seq_scan_node(catalog_table_, predicate, column_offsets);
  SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  // Execute
  seq_scan_executor.Init();
  std::unique_ptr<std::vector<std::unique_ptr<LogicalTile>>>
      result_tiles(new std::vector<std::unique_ptr<LogicalTile>>());

  while (seq_scan_executor.Execute()) {
    result_tiles->push_back(
        std::unique_ptr<LogicalTile>(seq_scan_executor.GetOutput()));
  }

  return result_tiles;
}

/*@brief   Add index on catalog table
 * @param   key_attrs    indexed column offset(position)
 * @param   index_oid    index id(global unique)
 * @param   index_name   index name(global unique)
 * @param   index_constraint     index constraints
 * @return  Unique pointer of vector of logical tiles
 * Note: Use Catalog::CreateIndex() if you can, only ColumnCatalog and
 * IndexCatalog should need this
 */
void AbstractCatalog::AddIndex(const std::string &index_name,
                               uint index_oid,
                               const std::vector<uint> &key_attrs,
                               IndexConstraintType index_constraint) {
  auto schema = catalog_table_->GetSchema();
  auto key_schema = Schema::CopySchema(schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique_keys = (index_constraint == IndexConstraintType::PRIMARY_KEY) ||
                     (index_constraint == IndexConstraintType::UNIQUE);

  auto index_metadata = new IndexMetadata(
      index_name, index_oid, catalog_table_->GetOid(), CATALOG_DATABASE_OID,
      IndexType::BWTREE, index_constraint, schema, key_schema, key_attrs,
      unique_keys);

  std::shared_ptr<Index> key_index(
      IndexFactory::GetIndex(index_metadata));
  catalog_table_->AddIndex(key_index);

  // If you called AddIndex(), you need to insert the index record by yourself!
  // insert index record into index_catalog(pg_index) table
  // IndexCatalog::GetInstance()->InsertIndex(
  //     index_oid, index_name, COLUMN_CATALOG_OID, IndexType::BWTREE,
  //     index_constraint, unique_keys, pool_.get(), nullptr);

//   LOG_TRACE("Successfully created index '%s' for table '%d'",
//             index_name.c_str(), (int)catalog_table_->GetOid());
}

/*@brief   Update specific columns using index scan
 * @param   update_columns    Columns to be updated
 * @param   update_values     Values to be updated
 * @param   scan_values       Value to be scaned (used in index scan)
 * @param   index_offset      Offset of index for scan
 * @return  true if successfully executes
 */
bool AbstractCatalog::UpdateWithIndexScan(TransactionContext *txn,
                                          uint index_offset,
                                          std::vector<Value> scan_values,
                                          std::vector<uint> update_columns,
                                          std::vector<Value> update_values) {
//   if (txn == nullptr) throw CatalogException("Scan table requires transaction");

  std::unique_ptr<ExecutorContext> context(
      new ExecutorContext(txn));
  // Construct index scan executor
  auto index = catalog_table_->GetIndex(index_offset);
  std::vector<uint> key_column_offsets =
      index->GetMetadata()->GetKeySchema()->GetIndexedColumns();

  // NOTE: For indexed scan on catalog tables, we expect it not to be "partial
  // indexed scan"(efficiency purpose).That being said, indexed column number
  // must be equal to passed in "scan_values" size
  PELOTON_ASSERT(scan_values.size() == key_column_offsets.size());
  std::vector<ExpressionType> expr_types(scan_values.size(),
                                         ExpressionType::COMPARE_EQUAL);
  std::vector<AbstractExpression *> runtime_keys;

  IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_offsets, expr_types, scan_values,
      runtime_keys);

  IndexScanPlan index_scan_node(catalog_table_, nullptr,
                                         update_columns, index_scan_desc);

  IndexScanExecutor index_scan_executor(&index_scan_node,
                                                  context.get());
  // Construct update executor
  TargetList target_list;
  DirectMapList direct_map_list;

  size_t column_count = catalog_table_->GetSchema()->GetColumnCount();
  for (size_t col_itr = 0; col_itr < column_count; col_itr++) {
    // Skip any column for update
    if (std::find(std::begin(update_columns), std::end(update_columns),
                  col_itr) == std::end(update_columns)) {
      direct_map_list.emplace_back(col_itr, std::make_pair(0, col_itr));
    }
  }

  PELOTON_ASSERT(update_columns.size() == update_values.size());
  for (size_t i = 0; i < update_values.size(); i++) {
    DerivedAttribute update_attribute{
        new ConstantValueExpression(update_values[i])};
    target_list.emplace_back(update_columns[i], update_attribute);
  }

  std::unique_ptr<const ProjectInfo> project_info(
      new ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  UpdatePlan update_node(catalog_table_, std::move(project_info));

  UpdateExecutor update_executor(&update_node, context.get());
  update_executor.AddChild(&index_scan_executor);
  // Execute
  update_executor.Init();
  return update_executor.Execute();
}

