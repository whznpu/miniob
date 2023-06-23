#pragma once

#include "rc.h"
#include "sql/operator/operator.h"

class Trx;
class UpdateStmt;

class UpdateOperator : public Operator {
 public:
  UpdateOperator(UpdateStmt *update_stmt, Trx *trx)
      : update_stmt_(update_stmt), trx_(trx) {
    opertype_ = OperType::UPDATE_OPERATOR;
  }

  virtual ~UpdateOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }
  // int tuple_cell_num() const override
  // RC tuple_cell_spec_at(int index, TupleCellSpec &spec) const override
 private:
  UpdateStmt *update_stmt_ = nullptr;
  Trx *trx_ = nullptr;
};