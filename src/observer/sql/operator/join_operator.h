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

#pragma once

#include "rc.h"
#include "sql/operator/operator.h"
#include "sql/operator/table_scan_operator.h"
#include "sql/parser/parse.h"

// TODO fixme
class JoinOperator : public Operator {
 public:
  JoinOperator(Operator *left, Operator *right) : left_(left), right_(right) {
    opertype_ = OperType::JOIN_OPERATOR;

    this->add_child(left);
    this->add_child(right);
  }

  virtual ~JoinOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  RC set_left_operator(Operator *left);
  RC set_right_operator(Operator *right);

  CompositeTuple *current_tuple() override;

  RC init_composite_tuple();

  RC union_tuples(RowTuple *tuple1, RowTuple *tuple2, RowTuple *tuple_ret);
  RC fresh();
  bool has_next() {
    if (left_type_ == OperType::SCAN_OPERATOR &&
        right_type_ == OperType::SCAN_OPERATOR) {
      // 这里应该是 || 而不是&&
      return (((TableScanOperator *)left_)->has_next()) ||
             (((TableScanOperator *)right_)->has_next());
    } else {
      return (((TableScanOperator *)left_)->has_next()) ||
             (((JoinOperator *)right_)->has_next());
    }
  }

  RC my_next();

  

 private:
  // RowTuple tuple_;
  CompositeTuple composite_tuple_;
  Operator *left_ = nullptr;
  Operator *right_ = nullptr;
  OperType left_type_;
  OperType right_type_;
  bool round_done_ = true;
};
