/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2021/6/10.
//

#include "sql/operator/join_operator.h"

#include "common/log/log.h"
#include "sql/operator/table_scan_operator.h"
#include "storage/common/field.h"
#include "storage/record/record.h"

// TODO fixme

RC JoinOperator::open() {
  RC rc = RC::SUCCESS;

  RC rc_right = RC::SUCCESS;
  RC rc_left = RC::SUCCESS;
  if (children_.size() < 2) {
    LOG_WARN("join operator must has one child");
    return RC::INTERNAL;
  }
  rc_left = children_[0]->open();
  rc_right = children_[1]->open();

  if (!((rc_left == RC::SUCCESS) && (rc_right == RC::SUCCESS))) {
    LOG_WARN("child of join_operator cannot open.");
    return RC::INTERNAL;
  }
  // if ((children_[0]->opertype_ == OperType::SCAN_OPERATOR) &&
  //     (children_[1]->opertype_ == OperType::SCAN_OPERATOR)) {
  //   // TO DO
  // }
  return rc;
}
RC JoinOperator::init_composite_tuple() {
  this->composite_tuple_.add_tuple(
      &(((TableScanOperator *)children_[0])->tuple_));

  Operator *ptr = children_[1];
  while (ptr->opertype_ != OperType::SCAN_OPERATOR) {
    this->composite_tuple_.add_tuple(&(
        ((TableScanOperator *)(((JoinOperator *)ptr)->children_[0]))->tuple_));
    ptr = (((JoinOperator *)ptr)->children_[1]);
  }

  this->composite_tuple_.add_tuple(&(((TableScanOperator *)ptr)->tuple_));
  return RC::SUCCESS;
}

RC JoinOperator::next() {
  RC rc = RC::SUCCESS;
  Operator *left_operator = children_[0];
  Operator *right_operator = children_[1];
  // TO DO :使用composite_tuple
  //如果右节点没有到EOF
  if (((TableScanOperator *)left_operator)->is_first_acccess()) {
    (rc = left_operator->next());
  }
  if ((rc = right_operator->next()) != RC::RECORD_EOF) {
    Tuple *left_tuple = left_operator->current_tuple();
    Tuple *right_tuple = right_operator->current_tuple();
    if (left_tuple == nullptr || right_tuple == nullptr) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from left or right operator");
      return rc;
    }
    return rc;
  }
  //如果右子节点到EOF，左子节点没有到达EOF
  else if ((rc = left_operator->next()) != RC::RECORD_EOF) {
    TableScanOperator *scan_oper_tmp =
        new TableScanOperator(  //感觉是这个地方出了问题
            ((TableScanOperator *)right_operator)->table_external);

    rc = scan_oper_tmp->open();
    this->set_right_operator(scan_oper_tmp);
    if ((rc = right_operator->next()) == RC::RECORD_EOF) {
      return rc;
    }
    Tuple *left_tuple = left_operator->current_tuple();
    Tuple *right_tuple = right_operator->current_tuple();

    if (left_tuple == nullptr || right_tuple == nullptr) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from left or right operator");
      return rc;
    }
    return rc;
  } else if ((rc = right_operator->next()) != RC::RECORD_EOF) {
    Tuple *left_tuple = left_operator->current_tuple();
    Tuple *right_tuple = right_operator->current_tuple();
    if (left_tuple == nullptr || right_tuple == nullptr) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from left or right operator");
      return rc;
    }
    return rc;
  }
  return rc;
}

RC JoinOperator::close() {
  return (children_[0]->close() == RC::SUCCESS &&
          children_[1]->close() == RC::SUCCESS)
             ? RC::SUCCESS
             : RC::INTERNAL;
}

RC JoinOperator::set_left_operator(Operator *left) {
  if (left == nullptr) {
    LOG_WARN("Left operator of Join Operator cannot be nullptr");
    return RC::INTERNAL;
  }
  this->left_ = left;
  children_[0] = left;
  left_type_ = left->opertype_;

  return RC::SUCCESS;
}
RC JoinOperator::set_right_operator(Operator *right) {
  if (right == nullptr) {
    LOG_WARN("Right operator of Join Operator cannot be nullptr");
    return RC::INTERNAL;
  }
  this->right_ = right;
  children_[1] = right;
  right_type_ = right_->opertype_;
  return RC::SUCCESS;
}

RC JoinOperator::union_tuples(RowTuple *tuple1, RowTuple *tuple2,
                              RowTuple *tuple_ret) {
  if ((tuple1) == nullptr || (tuple2) == nullptr) {
    LOG_WARN("union_tuples() needs two non-nullptr parameter");
    return RC::INTERNAL;
  }
  for (int i = 0; i < tuple1->cell_num(); i++) {
    const TupleCellSpec *tuple_cell_tmp = new TupleCellSpec();
    RC rc_tmp = tuple1->cell_spec_at(i, tuple_cell_tmp);
    if (rc_tmp != RC::SUCCESS) {
      LOG_WARN("union_tuples tuple1 faliure, at index : %d", i);
    }
    const FieldExpr *field_expr =
        (const FieldExpr *)tuple_cell_tmp->expression();
    const Field &field = field_expr->field();
    std::cout << field.field_name() << std::endl;
    if (0 == strcmp("__trx", field.field_name())) {
      continue;
    }
    tuple_ret->add_cell_spec((TupleCellSpec *)tuple_cell_tmp);
  }
  for (int i = 0; i < tuple2->cell_num(); i++) {
    const TupleCellSpec *tuple_cell_tmp = new TupleCellSpec();
    RC rc_tmp = tuple2->cell_spec_at(i, tuple_cell_tmp);
    if (rc_tmp != RC::SUCCESS) {
      LOG_WARN("union_tuples tuple2 faliure, at index : %d", i);
    }
    const FieldExpr *field_expr =
        (const FieldExpr *)tuple_cell_tmp->expression();
    const Field &field = field_expr->field();
    std::cout << field.field_name() << std::endl;
    if (0 == strcmp("__trx", field.field_name())) {
      continue;
    }
    tuple_ret->add_cell_spec((TupleCellSpec *)tuple_cell_tmp);
  }

  return RC::SUCCESS;
}

CompositeTuple *JoinOperator::current_tuple() {
  // composite_tuple_.print_composite_tuple();
  return &composite_tuple_;
}

RC JoinOperator::my_next() {
  RC rc = RC::SUCCESS;
  bool tmp = false;
  if (left_type_ == OperType::SCAN_OPERATOR &&
      right_type_ == OperType::SCAN_OPERATOR) {
    //此时是叶子节点
    //第一次访问到左边表节点时 刷新一行
    if (((TableScanOperator *)left_)->is_first_acccess()) {
      ((TableScanOperator *)left_)->next();
      ((TableScanOperator *)left_)->current_tuple();
    }

    //右子节点到达表尾 刷新 左子节点和右子节点各走一步
    tmp = !((TableScanOperator *)right_)->has_next();
    if (tmp) {
      ((TableScanOperator *)right_)->fresh();
      ((TableScanOperator *)left_)->next();
      ((TableScanOperator *)left_)->current_tuple();
      ((TableScanOperator *)right_)->next();
      ((TableScanOperator *)right_)->current_tuple();
      return rc;
    } else {
      ((TableScanOperator *)right_)->next();
      ((TableScanOperator *)right_)->current_tuple();
      return rc;
    }

    return RC::SUCCESS;
  } else {
    //此时是中间节点

    if (((TableScanOperator *)left_)->is_first_acccess()) {
      ((TableScanOperator *)left_)->next();
      ((TableScanOperator *)left_)->current_tuple();
    }

    //右子节点到达表尾 刷新 左子节点和右子节点各走一步
    tmp = !((JoinOperator *)right_)->has_next();
    if (tmp) {
      ((JoinOperator *)right_)->fresh();
      ((TableScanOperator *)left_)->next();
      ((TableScanOperator *)left_)->current_tuple();
      ((JoinOperator *)right_)->my_next();
      return rc;
    } else {
      ((JoinOperator *)right_)->my_next();
      return rc;
    }
  }

  return rc;
}

RC JoinOperator::fresh() {
  //这里递归清理左右子节点
  if (left_type_ == OperType::SCAN_OPERATOR &&
      right_type_ == OperType::SCAN_OPERATOR) {
    //此时是叶子节点
    ((TableScanOperator *)left_)->fresh();
    ((TableScanOperator *)left_)->reset_is_first_access();

    ((TableScanOperator *)right_)->fresh();
  } else {
    //此时是中间节点
    ((TableScanOperator *)left_)->fresh();
    ((TableScanOperator *)left_)->reset_is_first_access();

    ((JoinOperator *)right_)->fresh();
  }
  return RC::SUCCESS;
}
