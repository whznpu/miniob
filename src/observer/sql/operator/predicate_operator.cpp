/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/predicate_operator.h"

#include "common/log/log.h"
#include "sql/operator/join_operator.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/field.h"
#include "storage/record/record.h"

RC PredicateOperator::open() {
  RC rc = RC::SUCCESS;
  if (children_.size() != 1) {
    LOG_WARN("predicate operator must has one child");
    return RC::INTERNAL;
  }
  rc = children_[0]->open();

  if (children_[0]->opertype_ == OperType::JOIN_OPERATOR) {
    rc = ((JoinOperator *)children_[0])->init_composite_tuple();
  }
  return rc;
}

RC PredicateOperator::next() {
  RC rc = RC::SUCCESS;
  Operator *oper = children_[0];
  if (oper->opertype_ == OperType::SCAN_OPERATOR) {
    while (RC::SUCCESS == (rc = oper->next())) {
      Tuple *tuple = oper->current_tuple();
      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get tuple from operator");
        break;
      }
      //当前的tuple如果不满足此算子的条件就直接滤掉
      if (this->children_[0]->opertype_ == OperType::JOIN_OPERATOR) {
        if (do_predicate_with_compositeTuple(
                static_cast<CompositeTuple &>(*tuple))) {
          return rc;
        }
      } else {
        if (do_predicate(static_cast<RowTuple &>(*tuple))) {
          return rc;
        }
      }
    }

    return rc;
  } else if (oper->opertype_ == OperType::JOIN_OPERATOR) {
    while (true) {
      bool tmp = (((JoinOperator *)oper)->has_next());
      if (!tmp) {
        rc = RC::INTERNAL;
        break;
      }
      rc = ((JoinOperator *)oper)->my_next();
      Tuple *tuple = ((JoinOperator *)oper)->current_tuple();
      if (nullptr == tuple) {
        rc = RC::INTERNAL;
        LOG_WARN("failed to get tuple from operator");
        break;
      }
      //当前的tuple如果不满足此算子的条件就直接滤掉
      if (this->children_[0]->opertype_ == OperType::JOIN_OPERATOR) {
        if (do_predicate_with_compositeTuple(
                static_cast<CompositeTuple &>(*tuple))) {
          return rc;
        }
      } else {
        if (do_predicate(static_cast<RowTuple &>(*tuple))) {
          return rc;
        }
      }
    }
    return rc;
  }
}

RC PredicateOperator::my_next() {
  RC rc = RC::SUCCESS;
  Operator *oper = children_[0];

  while ((((JoinOperator *)oper)->has_next())) {
    rc = ((JoinOperator *)oper)->my_next();
    Tuple *tuple = ((JoinOperator *)oper)->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }
    //当前的tuple如果不满足此算子的条件就直接滤掉
    if (this->children_[0]->opertype_ == OperType::JOIN_OPERATOR) {
      if (do_predicate_with_compositeTuple(
              static_cast<CompositeTuple &>(*tuple))) {
        return rc;
      }
    } else {
      if (do_predicate(static_cast<RowTuple &>(*tuple))) {
        return rc;
      }
    }
  }
  return rc;
}

RC PredicateOperator::close() {
  children_[0]->close();
  return RC::SUCCESS;
}

Tuple *PredicateOperator::current_tuple() {
  return children_[0]->current_tuple();
}

// 根据这个结果 返回相应的record 
bool PredicateOperator::do_predicate(RowTuple &tuple)
{
  if (filter_stmt_ == nullptr || filter_stmt_->filter_units().empty()) {
    return true;
  }


  int filter_len=filter_stmt_->filter_units().size();
  for (const FilterUnit *filter_unit : filter_stmt_->filter_units()) {
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();
    CompOp comp = filter_unit->comp();
    TupleCell left_cell;
    TupleCell right_cell;
    left_expr->get_value(tuple, left_cell);
    right_expr->get_value(tuple, right_cell);

    const int compare = left_cell.compare(right_cell);
    const int like_compare =left_cell.like_compare(right_cell);
    bool filter_result = false;
    switch (comp) {
      case EQUAL_TO: {
        filter_result = (0 == compare);
      } break;
      case LESS_EQUAL: {
        filter_result = (compare <= 0);
      } break;
      case NOT_EQUAL: {
        filter_result = (compare != 0);
      } break;
      case LESS_THAN: {
        filter_result = (compare < 0);
      } break;
      case GREAT_EQUAL: {
        filter_result = (compare >= 0);
      } break;
      case GREAT_THAN: {
        filter_result = (compare > 0);
      } break;
      case LIKE_THE: {
      filter_result = (like_compare==1);
    }break;
    case NOT_LIKE_THE: {
      filter_result = (like_compare==0);
    }break;
    default: {
        LOG_WARN("invalid compare type: %d", comp);
      } break;
    }
    if (!filter_result) {
      return false;
    }
  }
  return true;
}

bool PredicateOperator::do_predicate_with_compositeTuple(
    CompositeTuple &tuple) {
  if (filter_stmt_ == nullptr || filter_stmt_->filter_units().empty()) {
    return true;
  }


  int filter_len=filter_stmt_->filter_units().size();
  for (const FilterUnit *filter_unit : filter_stmt_->filter_units()) {
    Expression *left_expr = filter_unit->left();
    Expression *right_expr = filter_unit->right();
    CompOp comp = filter_unit->comp();
    TupleCell left_cell;
    TupleCell right_cell;
    left_expr->get_value(tuple, left_cell);
    right_expr->get_value(tuple, right_cell);

    const int compare = left_cell.compare(right_cell);
    const int like_compare =left_cell.like_compare(right_cell);
    bool filter_result = false;
    switch (comp) {
    case EQUAL_TO: {
      filter_result = (0 == compare); 
    } break;
    case LESS_EQUAL: {
      filter_result = (compare <= 0); 
    } break;
    case NOT_EQUAL: {
      filter_result = (compare != 0);
    } break;
    case LESS_THAN: {
      filter_result = (compare < 0);
    } break;
    case GREAT_EQUAL: {
      filter_result = (compare >= 0);
    } break;
    case GREAT_THAN: {
      filter_result = (compare > 0);
    } break;
    case LIKE_THE: {
      filter_result = (like_compare==1);
    }break;
    case NOT_LIKE_THE: {
      filter_result = (like_compare==0);
    }break;
    default: {
      LOG_WARN("invalid compare type: %d", comp);
    } break;
    }
    if (!filter_result) {
      return false;
    }
  }
  return true;
}

// int PredicateOperator::tuple_cell_num() const
// {
//   return children_[0]->tuple_cell_num();
// }
// RC PredicateOperator::tuple_cell_spec_at(int index, TupleCellSpec &spec)
// const
// {
//   return children_[0]->tuple_cell_spec_at(index, spec);
// }
