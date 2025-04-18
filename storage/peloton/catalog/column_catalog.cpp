//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_catalog.cpp
//
// Identification: src/catalog/column_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "column_catalog.h"

#include "catalog.h"
#include "system_catalogs.h"
#include "table_catalog.h"
#include "storage/peloton/concurrency/transaction_context.h"
#include "storage/peloton/store/data_table.h"
#include "storage/peloton/store/database.h"
#include "storage/peloton/type/ephemeral_pool.h"
#include "storage/peloton/type/value_factory.h"



ColumnCatalogEntry::ColumnCatalogEntry(LogicalTile *tile,
                                         int tupleId)
    : table_oid_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::TABLE_OID)
                    .GetAs<uint>()),
      column_name_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_NAME)
                      .ToString()),
      column_id_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_ID)
                    .GetAs<uint>()),
      column_offset_(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_OFFSET)
              .GetAs<uint>()),
      column_type_(StringToTypeId(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_TYPE)
              .ToString())),
      column_length_(
          tile->GetValue(tupleId, ColumnCatalog::ColumnId::COLUMN_LENGTH)
              .GetAs<uint32_t>()),
      is_inlined_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_INLINED)
                     .GetAs<bool>()),
      is_not_null_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::IS_NOT_NULL)
                      .GetAs<bool>()),
      has_default_(tile->GetValue(tupleId, ColumnCatalog::ColumnId::HAS_DEFAULT)
                      .GetAs<bool>()) {
  // deserialize default value if the column has default constraint
  if (has_default_) {
    auto dv_val =
        tile->GetValue(tupleId, ColumnCatalog::ColumnId::DEFAULT_VALUE_BIN);
    CopySerializeInput input_buffer(dv_val.GetData(), dv_val.GetLength());
    default_value_ = Value::DeserializeFrom(input_buffer, column_type_);
  }
}

ColumnCatalog::ColumnCatalog(TransactionContext *txn,
                             Database *pg_catalog,
                             AbstractPool *pool)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      COLUMN_CATALOG_OID,
                      COLUMN_CATALOG_NAME) {
  // Add indexes for pg_attribute
  AddIndex(COLUMN_CATALOG_NAME "_pkey",
           COLUMN_CATALOG_PKEY_OID,
           {ColumnId::TABLE_OID, ColumnId::COLUMN_NAME},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(COLUMN_CATALOG_NAME "_skey0",
           COLUMN_CATALOG_SKEY0_OID,
           {ColumnId::TABLE_OID, ColumnId::COLUMN_ID},
           IndexConstraintType::UNIQUE);
  AddIndex(COLUMN_CATALOG_NAME "_skey1",
           COLUMN_CATALOG_SKEY1_OID,
           {ColumnId::TABLE_OID},
           IndexConstraintType::DEFAULT);

  // Insert columns of pg_attribute table into pg_attribute itself
  uint32_t column_id = 0;
  for (auto column : catalog_table_->GetSchema()->GetColumns()) {
    InsertColumn(txn,
                 COLUMN_CATALOG_OID,
                 column.GetName(),
                 column_id,
                 column.GetOffset(),
                 column.GetType(),
                 column.GetLength(),
                 column.IsInlined(),
                 column.IsNotNull(),
                 column.HasDefault(),
                 column.GetDefaultValue(),
                 pool);
    column_id++;
  }
}

ColumnCatalog::~ColumnCatalog() {}

/*@brief   private function for initialize schema of pg_attribute
 * @return  unqiue pointer to schema
 */
std::unique_ptr<Schema> ColumnCatalog::InitializeSchema() {
  auto table_id_column = Column(
      TypeId::INTEGER, Type::GetTypeSize(TypeId::INTEGER),
      "table_oid", true);
  table_id_column.SetNotNull();

  auto column_name_column = Column(
      TypeId::VARCHAR, max_name_size_, "column_name", false);
  column_name_column.SetNotNull();

  auto column_id_column = Column(
      TypeId::INTEGER, Type::GetTypeSize(TypeId::INTEGER),
      "column_id", true);
  column_id_column.SetNotNull();

  auto column_offset_column = Column(
      TypeId::INTEGER, Type::GetTypeSize(TypeId::INTEGER),
      "column_offset", true);
  column_offset_column.SetNotNull();

  auto column_type_column = Column(
      TypeId::VARCHAR, max_name_size_, "column_type", false);
  column_type_column.SetNotNull();


  auto column_length_column = Column(
      TypeId::INTEGER, Type::GetTypeSize(TypeId::INTEGER),
      "column_length", true);
  column_length_column.SetNotNull();

  auto is_inlined_column = Column(
      TypeId::BOOLEAN, Type::GetTypeSize(TypeId::BOOLEAN),
      "is_inlined", true);
  is_inlined_column.SetNotNull();

  auto is_not_null_column = Column(
      TypeId::BOOLEAN, Type::GetTypeSize(TypeId::BOOLEAN),
      "is_not_null", true);
  is_not_null_column.SetNotNull();

  auto has_default_column = Column(
      TypeId::BOOLEAN, Type::GetTypeSize(TypeId::BOOLEAN),
      "has_default", true);
  has_default_column.SetNotNull();

  auto default_value_src_column = Column(
      TypeId::VARCHAR, Type::GetTypeSize(TypeId::VARCHAR),
      "default_value_src", false);

  auto default_value_bin_column = Column(
      TypeId::VARBINARY, Type::GetTypeSize(TypeId::VARBINARY),
      "default_value_bin", false);

  std::unique_ptr<Schema> column_catalog_schema(new Schema(
      {table_id_column, column_name_column, column_id_column,
       column_offset_column, column_type_column, column_length_column,
       is_inlined_column, is_not_null_column, has_default_column,
       default_value_src_column, default_value_bin_column}));

  column_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      COLUMN_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      COLUMN_CATALOG_OID,
      std::vector<uint>{ColumnId::TABLE_OID, ColumnId::COLUMN_NAME},
      COLUMN_CATALOG_PKEY_OID));

  column_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      COLUMN_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      COLUMN_CATALOG_OID,
      std::vector<uint>{ColumnId::TABLE_OID, ColumnId::COLUMN_ID},
      COLUMN_CATALOG_SKEY0_OID));

  return column_catalog_schema;
}

bool ColumnCatalog::InsertColumn(TransactionContext *txn,
                                 uint table_oid,
                                 const std::string &column_name,
                                 uint column_id,
                                 uint column_offset,
                                 TypeId column_type,
                                 size_t column_length,
                                 bool is_inlined,
                                 bool is_not_null,
                                 bool is_default,
                                 const std::shared_ptr<Value> default_value,
                                 AbstractPool *pool) {
  // Create the tuple first
  std::unique_ptr<Tuple> tuple(
      new Tuple(catalog_table_->GetSchema(), true));

  auto val0 = ValueFactory::GetIntegerValue(table_oid);
  auto val1 = ValueFactory::GetVarcharValue(column_name, nullptr);
  auto val2 = ValueFactory::GetIntegerValue(column_id);
  auto val3 = ValueFactory::GetIntegerValue(column_offset);
  auto val4 =
      ValueFactory::GetVarcharValue(TypeIdToString(column_type), nullptr);
  auto val5 = ValueFactory::GetIntegerValue(column_length);
  auto val6 = ValueFactory::GetBooleanValue(is_inlined);
  auto val7 = ValueFactory::GetBooleanValue(is_not_null);
  auto val8 = ValueFactory::GetBooleanValue(is_default);

  tuple->SetValue(ColumnId::TABLE_OID, val0, pool);
  tuple->SetValue(ColumnId::COLUMN_NAME, val1, pool);
  tuple->SetValue(ColumnId::COLUMN_ID, val2, pool);
  tuple->SetValue(ColumnId::COLUMN_OFFSET, val3, pool);
  tuple->SetValue(ColumnId::COLUMN_TYPE, val4, pool);
  tuple->SetValue(ColumnId::COLUMN_LENGTH, val5, pool);
  tuple->SetValue(ColumnId::IS_INLINED, val6, pool);
  tuple->SetValue(ColumnId::IS_NOT_NULL, val7, pool);
  tuple->SetValue(ColumnId::HAS_DEFAULT, val8, pool);

  // set default value if the column has default constraint
  if (is_default) {
    auto val9 =
        ValueFactory::GetVarcharValue(default_value->ToString(), nullptr);
    CopySerializeOutput output_buffer;
    default_value->SerializeTo(output_buffer);
    auto val10 = ValueFactory::GetVarbinaryValue(
        (unsigned char *)output_buffer.Data(), output_buffer.Size(), true,
        pool);

    tuple->SetValue(ColumnId::DEFAULT_VALUE_SRC, val9, pool);
    tuple->SetValue(ColumnId::DEFAULT_VALUE_BIN, val10, pool);
  }

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool ColumnCatalog::DeleteColumn(TransactionContext *txn,
                                 uint table_oid,
                                 const std::string &column_name) {
  uint index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(table_oid).Copy());
  values.push_back(
      ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictColumnCatalogEntry(column_name);

  return DeleteWithIndexScan(txn, index_offset, values);
}

/* @brief   delete all column records from the same table
 * this function is useful when calling DropTable
 * @param   table_oid
 * @param   txn  TransactionContext
 * @return  a vector of table oid
 */
bool ColumnCatalog::DeleteColumns(TransactionContext *txn, uint table_oid) {
  uint index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(table_oid).Copy());

  // delete columns from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictAllColumnCatalogEntries();

  return DeleteWithIndexScan(txn, index_offset, values);
}

bool ColumnCatalog::UpdateNotNullConstraint(TransactionContext *txn,
                                            uint table_oid,
                                            const std::string &column_name,
                                            bool is_not_null) {
  std::vector<uint> update_columns({ColumnId::IS_NOT_NULL});
  uint index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  // values to execute index scan
  std::vector<Value> scan_values;
  scan_values.push_back(ValueFactory::GetIntegerValue(table_oid).Copy());
  scan_values.push_back(
      ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // values to update
  std::vector<Value> update_values;
  update_values.push_back(
      ValueFactory::GetBooleanValue(is_not_null).Copy());

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictColumnCatalogEntry(column_name);

  return UpdateWithIndexScan(txn,
                             index_offset,
                             scan_values,
                             update_columns,
                             update_values);
}

bool ColumnCatalog::UpdateDefaultConstraint(TransactionContext *txn,
                                            uint table_oid,
                                            const std::string &column_name,
                                            bool has_default,
                                            const Value *default_value) {
  std::vector<uint> update_columns({ColumnId::HAS_DEFAULT,
                                     ColumnId::DEFAULT_VALUE_SRC,
                                     ColumnId::DEFAULT_VALUE_BIN});
  uint index_offset =
      IndexId::PRIMARY_KEY;  // Index of table_oid & column_name
  // values to execute index scan
  std::vector<Value> scan_values;
  scan_values.push_back(ValueFactory::GetIntegerValue(table_oid).Copy());
  scan_values.push_back(
      ValueFactory::GetVarcharValue(column_name, nullptr).Copy());

  // values to update
  std::vector<Value> update_values;
  update_values.push_back(
      ValueFactory::GetBooleanValue(has_default).Copy());
  if (has_default) {
    PELOTON_ASSERT(default_value != nullptr);
    update_values.push_back(
        ValueFactory::GetVarcharValue(default_value->ToString()).Copy());
    CopySerializeOutput output_buffer;
    default_value->SerializeTo(output_buffer);
    update_values.push_back(ValueFactory::GetVarbinaryValue(
                                (unsigned char *)output_buffer.Data(),
                                output_buffer.Size(), true).Copy());
  } else {
    update_values.push_back(
        ValueFactory::GetNullValueByType(TypeId::VARCHAR));
    update_values.push_back(
        ValueFactory::GetNullValueByType(TypeId::VARBINARY));
  }

  // delete column from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  table_object->EvictColumnCatalogEntry(column_name);

  return UpdateWithIndexScan(txn,
                             index_offset,
                             scan_values,
                             update_columns,
                             update_values);
}

const std::unordered_map<uint, std::shared_ptr<ColumnCatalogEntry>>
ColumnCatalog::GetColumnCatalogEntries(TransactionContext *txn,
                                       uint table_oid) {
  // try get from cache
  auto pg_table = Catalog::GetInstance()
                      ->GetSystemCatalogs(database_oid_)
                      ->GetTableCatalog();
  auto table_object = pg_table->GetTableCatalogEntry(txn, table_oid);
  PELOTON_ASSERT(table_object && table_object->GetTableOid() == table_oid);
  auto column_objects = table_object->GetColumnCatalogEntries(true);
  if (column_objects.size() != 0) return column_objects;

  // cache miss, get from pg_attribute
  std::vector<uint> column_ids(all_column_ids_);
  uint index_offset = IndexId::SKEY_TABLE_OID;  // Index of table_oid
  std::vector<Value> values;
  values.push_back(ValueFactory::GetIntegerValue(table_oid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(txn,
                             column_ids,
                             index_offset,
                             values);

  for (auto &tile : (*result_tiles)) {
    for (auto tuple_id : *tile) {
      auto column_object =
          std::make_shared<ColumnCatalogEntry>(tile.get(), tuple_id);
      table_object->InsertColumnCatalogEntry(column_object);
    }
  }

  return table_object->GetColumnCatalogEntries();
}

