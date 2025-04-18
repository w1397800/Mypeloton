//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraint.h
//
// Identification: src/include/catalog/constraint.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <string>
#include <utility>
#include <vector>
//#ifndef INTERNAL_TYPES_PELOTON
#include "storage/peloton/common/internal_types.h"
//#endif

#include "storage/peloton/type/value.h"


//===--------------------------------------------------------------------===//
// Constraint Class
//===--------------------------------------------------------------------===//

class Constraint  {
 public:
  // Constructor for primary key or unique
  Constraint(uint constraint_oid, ConstraintType type,
             std::string constraint_name, uint table_oid,
             std::vector<uint> column_ids, uint index_oid)
      : constraint_oid_(constraint_oid), constraint_name_(constraint_name),
        constraint_type_(type), table_oid_(table_oid),
        column_ids_(column_ids), index_oid_(index_oid) {}

  // Constructor for foreign key constraint
  Constraint(uint constraint_oid, ConstraintType type,
             std::string constraint_name, uint table_oid,
             std::vector<uint> column_ids,uint index_oid, uint sink_table_oid,
             std::vector<uint> sink_col_ids, FKConstrActionType update_action,
             FKConstrActionType delete_action)
      : constraint_oid_(constraint_oid), constraint_name_(constraint_name),
        constraint_type_(type), table_oid_(table_oid),
        column_ids_(column_ids), index_oid_(index_oid),
        fk_sink_table_oid_(sink_table_oid), fk_sink_col_ids_(sink_col_ids),
        fk_update_action_(update_action), fk_delete_action_(delete_action) {}

  // Constructor for check constraint
  Constraint(uint constraint_oid, ConstraintType type,
             std::string constraint_name, uint table_oid,
             std::vector<uint> column_ids, uint index_oid,
             std::pair<ExpressionType, Value> exp)
//             std::pair<ExpressionType, int> exp)
      : constraint_oid_(constraint_oid),  constraint_name_(constraint_name),
        constraint_type_(type), table_oid_(table_oid), column_ids_(column_ids),
        index_oid_(index_oid), check_exp_(exp) {}

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Set oid for catalog
  void SetConstraintOid(uint oid) { constraint_oid_ = oid; }

  inline uint GetConstraintOid() const { return constraint_oid_; }

  inline ConstraintType GetType() const { return constraint_type_; }

  // Get the table oid
  inline uint GetTableOid() const { return table_oid_; }

  // Get the column ids
  inline const std::vector<uint> &GetColumnIds() const { return column_ids_; }

  // Set index oid indicating the index constructing the constraint
  void SetIndexOid(uint oid) { index_oid_ = oid; }

  // Get the index oid
  inline uint GetIndexOid() const { return index_oid_; }

  inline std::string GetName() const { return constraint_name_; }

  // Get a string representation for debugging
  const std::string GetInfo() const ;
//  const std::string GetInfo() const override;

  inline uint GetFKSinkTableOid() const { return fk_sink_table_oid_; }

  inline const std::vector<uint> &GetFKSinkColumnIds() const { return fk_sink_col_ids_; }

  inline FKConstrActionType GetFKUpdateAction() const { return fk_update_action_; }

  inline FKConstrActionType GetFKDeleteAction() const { return fk_delete_action_; }

  inline std::pair<ExpressionType, Value> GetCheckExpression() const { return check_exp_; }
//  inline std::pair<ExpressionType, int> GetCheckExpression() const { return check_exp_; }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // constraint oid created by catalog
  uint constraint_oid_;

  std::string constraint_name_;

  // The type of constraint
  ConstraintType constraint_type_;

  // Table having this constraints
  uint table_oid_;

  // Column ids related the constraint
  std::vector<uint> column_ids_;

  // Index constructing the constraint
  uint index_oid_;

  // foreign key constraint information
  // The reference table (sink)
  uint fk_sink_table_oid_ = INVALID_OID;

  // Column ids in the reference table (sink)
  std::vector<uint> fk_sink_col_ids_;

  FKConstrActionType fk_update_action_ = FKConstrActionType::NOACTION;

  FKConstrActionType fk_delete_action_ = FKConstrActionType::NOACTION;

  // key string is column name for check constraint
  std::pair<ExpressionType, Value> check_exp_;
//  std::pair<ExpressionType, int> check_exp_;
};




