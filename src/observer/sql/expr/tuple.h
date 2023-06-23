/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2021/5/14.
//

#pragma once

#include <memory>
#include <vector>

#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/expr/tuple_cell.h"
#include "sql/parser/parse.h"
#include "storage/record/record.h"

class Table;
typedef enum {
  TUPLE = 0,
  ROW_TUPLE,
  COMPOSITE_TUPLE,
  PROJECT_TUPLE

} Tuple_Type;

class TupleCellSpec {
 public:
  TupleCellSpec() = default;
  TupleCellSpec(Expression *expr) : expression_(expr) {}

  ~TupleCellSpec() {
    if (expression_) {
      delete expression_;
      expression_ = nullptr;
    }
  }

  void set_alias(const char *alias) { this->alias_ = alias; }
  const char *alias() const { return alias_; }

  Expression *expression() const { return expression_; }

 private:
  const char *alias_ = nullptr;  //一个存储单元的别名
  Expression *expression_ = nullptr;
};

class Tuple {
 public:
  Tuple() = default;
  virtual ~Tuple() = default;

  virtual int cell_num() const = 0;  //一个tuple中的结构
  virtual RC cell_at(int index, TupleCell &cell)
      const = 0;  //从tuple中取出index处的cell传到TupleCell中
  virtual RC find_cell(const Field &field, TupleCell &cell) const = 0;
  //使用属性名找到对应的Cell并传到cell中输出

  virtual RC cell_spec_at(int index, const TupleCellSpec *&spec) const = 0;
  //从tuple中取出index处的cellspec传到TupleCellSpec中
  Tuple_Type type_ = Tuple_Type::TUPLE;
};

class RowTuple : public Tuple {
 public:
  RowTuple() { type_ = Tuple_Type::ROW_TUPLE; }
  virtual ~RowTuple() {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_record(Record *record) { this->record_ = record; }

  void set_schema(const Table *table, const std::vector<FieldMeta> *fields) {
    table_ = table;
    this->speces_.reserve(
        fields->size());  // 声明speces_至少保留fields->size()个元素
    for (const FieldMeta &field : *fields) {
      speces_.push_back(new TupleCellSpec(new FieldExpr(table, &field)));
    }
  }

  int cell_num() const override { return speces_.size(); }

  RC cell_at(int index, TupleCell &cell) const override {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }

    const TupleCellSpec *spec = speces_[index];
    FieldExpr *field_expr = (FieldExpr *)spec->expression();
    const FieldMeta *field_meta = field_expr->field().meta();
    cell.set_type(field_meta->type());
    cell.set_data(this->record_->data() + field_meta->offset());
    cell.set_length(field_meta->len());
    return RC::SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override {
    char *table_name = (char *)field.table_name();
    if (0 != strcmp(table_name, table_->name())) {
      return RC::NOTFOUND;
    }

    const char *field_name = field.field_name();
    for (size_t i = 0; i < speces_.size(); ++i) {
      const FieldExpr *field_expr = (const FieldExpr *)speces_[i]->expression();
      const Field &field_tmp = field_expr->field();
      if (0 == strcmp(field_name, field_tmp.field_name())) {  // TO CHECK
        return cell_at(i, cell);
      }
    }
    return RC::NOTFOUND;
  }

  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }
  void add_cell_spec(TupleCellSpec *spec) { speces_.push_back(spec); }

  void print_row_tuple() {
    TupleCell cell;
    RC rc = RC::SUCCESS;
    bool first_field = true;
    int cell_num_tmp = this->cell_num();
    for (int i = 0; i < cell_num_tmp; i++) {
      rc = this->cell_at(i, cell);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to print row tuple. index=%d, rc=%s", i, strrc(rc));
        break;
      }

      if (!first_field) {
        std::cout << " | ";
      } else {
        first_field = false;
      }
      cell.direct_to_string();
    }
    std::cout << std::endl;
    return;
  }

  Record &record() { return *record_; }

  const Record &record() const { return *record_; }

  std::vector<TupleCellSpec *> get_speces() { return speces_; }

  bool is_tuple_empty() { return table_->is_table_empty(); }

 private:
  Record *record_ = nullptr;
  const Table *table_ = nullptr;
  std::vector<TupleCellSpec *> speces_;
};

class CompositeTuple : public Tuple {
 public:
  CompositeTuple() { type_ = Tuple_Type::COMPOSITE_TUPLE; }
  virtual ~CompositeTuple() {
    for (Tuple *tuple : tuples_) {
      for (TupleCellSpec *spec : ((RowTuple *)tuple)->get_speces()) {
        delete spec;
      }
      ((RowTuple *)tuple)->get_speces().clear();
    }
  }
  int cell_num() const override {
    // TO DO 这里可能有bug 并不是很确定 而且我不想改了
    return cell_num_;
  }
  RC cell_at(int index, TupleCell &cell) const override {
    if (index < 0 || index >= cell_num_) {
      LOG_WARN("invalid argument. index=%d", index);
      return RC::INVALID_ARGUMENT;
    }
    int index_tmp = index;
    for (int i = 0; i < tuples_.size(); i++) {
      if (index_tmp < tuple_num_array.at(i)) {
        return tuples_.at(i)->cell_at(index_tmp, cell);
      } else {
        index_tmp -= tuple_num_array.at(i);
      }
    }
    return RC::SUCCESS;
  }

  RC find_cell(const Field &field, TupleCell &cell) const override {
    RC rc = RC::NOTFOUND;
    // char *table_name = (char *)field.table_name();

    for (Tuple *tuple_iter : tuples_) {
      if ((rc = ((RowTuple *)tuple_iter)->find_cell(field, cell)) !=
          RC::NOTFOUND) {
        return rc;
      }
    }
    return rc;
  }
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override {
    int index_tmp = index;
    for (long unsigned int i = 0; i < tuples_.size(); i++) {
      if (index_tmp < tuple_num_array.at(i)) {
        return tuples_.at(i)->cell_spec_at(index_tmp, spec);
      } else {
        index_tmp -= tuple_num_array.at(i);
      }
    }
    return RC::SUCCESS;
  }

  RC add_tuple(RowTuple *const &tuple) {
    if (tuple->is_tuple_empty()) {
      std::cout
          << "add_tuple(RowTuple *const &tuple) cannot insert a null tuple"
          << std::endl;
      return RC::EMPTY;
    }
    tuples_.push_back(tuple);
    cell_num_ += tuple->cell_num();  //这里可以修改一下只计算一个__trx属性
    return RC::SUCCESS;
  }

  void print_composite_tuple() {
    if (tuples_.size() == 0) {
      LOG_WARN("the composite_tuple have no element.");
      std::cout << "composite 中没有元素" << std::endl;
      return;
    }
    std::cout << "Print one Composite tuple." << std::endl;
    for (RowTuple *tuple : tuples_) {
      tuple->print_row_tuple();
    }

    return;
  }

 private:
  int cell_num_ = 0;
  std::vector<int> tuple_num_array;
  std::vector<RowTuple *> tuples_;
};

class ProjectTuple : public Tuple {
 public:
  ProjectTuple() { type_ = Tuple_Type::PROJECT_TUPLE; }
  virtual ~ProjectTuple() {
    for (TupleCellSpec *spec : speces_) {
      delete spec;
    }
    speces_.clear();
  }

  void set_tuple(Tuple *tuple) { this->tuple_ = tuple; }

  void add_cell_spec(TupleCellSpec *spec) { speces_.push_back(spec); }
  int cell_num() const override { return speces_.size(); }

  RC cell_at(int index, TupleCell &cell) const override {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::GENERIC_ERROR;
    }
    if (tuple_ == nullptr) {
      return RC::GENERIC_ERROR;
    }

    const TupleCellSpec *spec = speces_[index];

    // std::cout << "In ProjectTuple::cell_at(), spec to be output is "
    //           << std::string(spec->alias()) << std::endl;

    return spec->expression()->get_value(*tuple_, cell);
  }

  RC find_cell(const Field &field, TupleCell &cell) const override {
    return tuple_->find_cell(field, cell);
  }
  RC cell_spec_at(int index, const TupleCellSpec *&spec) const override {
    if (index < 0 || index >= static_cast<int>(speces_.size())) {
      return RC::NOTFOUND;
    }
    spec = speces_[index];
    return RC::SUCCESS;
  }

 private:
  std::vector<TupleCellSpec *> speces_;
  Tuple *tuple_ = nullptr;
};
