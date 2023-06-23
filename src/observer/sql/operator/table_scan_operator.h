/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2021/6/7.
//

#pragma once

#include "rc.h"
#include "sql/operator/operator.h"
#include "storage/record/record_manager.h"

class Table;

class TableScanOperator : public Operator {
 public:
  TableScanOperator(Table *table) : table_(table) {
    opertype_ = OperType::SCAN_OPERATOR;
    table_external = table;
  }

  virtual ~TableScanOperator() = default;

  RC open() override;
  RC next() override;
  RC close() override;

  RowTuple *current_tuple() override;
  RC get_row_tuple(RowTuple *&tuple) {
    tuple = &tuple_;
    return RC::SUCCESS;
  }
  RC get_table(Table *&table) {
    table = table_;
    return RC::SUCCESS;
  }

  // is_acccessed() 用来判断是否是第一次进入左节点
  bool is_first_acccess(void) {
    bool result = this->dirty_flag;
    if (result == false) {
      dirty_flag = true;
      return true;
    }
    return false;
  }

  RC reset_is_first_access(void) {
    this->dirty_flag = false;
    return RC::SUCCESS;
  }
  // reset_scanner_to_top() 负责刷新内部的record_scanner到表头
  RC reset_scanner_to_top() {
    RC rc = table_->get_record_scanner(record_scanner_);
    if (rc == RC::SUCCESS) {
      tuple_.set_schema(table_, table_->table_meta().field_metas());
    }
    return rc;
  }
  RC fresh();

  // table_has_next()用来检测当前的表扫描是否到末尾
  bool has_next() {
    //
    bool result = record_scanner_.has_next();
    return result;
  }

  // int tuple_cell_num() const override
  // {
  //   return tuple_.cell_num();
  // }

  // RC tuple_cell_spec_at(int index, TupleCellSpec &spec) const override;

  Table *table_external = nullptr;
  RowTuple tuple_;

 private:
  Table *table_ = nullptr;
  bool dirty_flag = false;  // 用来标记当前Operator是否已经访问过
  RecordFileScanner record_scanner_;
  Record current_record_;
};
