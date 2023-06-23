/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#pragma once

#include "rc.h"
#include "sql/stmt/stmt.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/filter_stmt.h"

class Table;

class UpdateStmt : public Stmt
{
public:

  UpdateStmt() = default;
  UpdateStmt(Table *table, const Value *values, int value_amount,int value_index,FilterStmt *filter_stmt);
  StmtType type() const override{
    return StmtType::UPDATE;
  };

public:
  // 静态方法
  static RC create(Db *db, const Updates &update_sql, Stmt *&stmt);

public:
  Table *table() const {return table_;}
  const Value *value() const { return value_; }
  int value_amount() const { return value_amount_; }
  FilterStmt *filter_stmt() const {return filter_stmt_; }
  // const char *attribute_name() const {return attribute_name_;}
  int value_index() const {return value_index_;}

private:
  Table *table_ = nullptr;
  // const char *attribute_name_;
  int value_index_=0;
  // const Value *values_ = nullptr;
  const Value *value_=nullptr;
  int value_amount_ = 0;
  FilterStmt *filter_stmt_ =nullptr;
};

