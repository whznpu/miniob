/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2022/07/05.
//

#include "sql/expr/tuple_cell.h"

#include <iomanip>

#include "common/log/log.h"
#include "storage/common/field.h"
#include "util/comparator.h"
#include "util/util.h"
#include <iomanip>
#include <string>
#include <regex>

void TupleCell::to_string(std::ostream &os) const {
  switch (attr_type_) {
    case INTS: {
      os << *(int *)data_;
    } break;  // 格式化输出 yyyy-mm-dd的格式
    // 输出结果是反着的 可能是
    case DATES: {
      int value = *(int *)data_;
      os << date2string(value);
    } break;
    case FLOATS: {
      float v = *(float *)data_;
      os << double2string(v);
    } break;
    case CHARS: {
      for (int i = 0; i < length_; i++) {
        if (data_[i] == '\0') {
          break;
        }
        os << data_[i];
      }
    } break;
    case TEXTS: {
      for (int i = 0; i < length_; i++) {
        if (data_[i] == '\0') {
          break;
        }
        os << data_[i];
      }
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
}
void TupleCell::direct_to_string() const {
  switch (attr_type_) {
    case INTS: {
      std::cout << *(int *)data_;
    } break;
    case FLOATS: {
      float v = *(float *)data_;
      std::cout << double2string(v);
    } break;
    case CHARS: {
      for (int i = 0; i < length_; i++) {
        if (data_[i] == '\0') {
          break;
        }
        std::cout << data_[i];
      }
    } break;
    default: {
      LOG_WARN("unsupported attr type: %d", attr_type_);
    } break;
  }
}

int TupleCell::compare(const TupleCell &other) const {
  if (this->attr_type_ == other.attr_type_) {
    switch (this->attr_type_) {
      case INTS:
        return compare_int(this->data_, other.data_);
      case FLOATS:
        return compare_float(this->data_, other.data_);
      case CHARS:
        return compare_string(this->data_, this->length_, other.data_,
                              other.length_);
      case DATES:
        return compare_int(this->data_, other.data_);  // 因为存储类型用的int
      case TEXTS:
        return compare_string(this->data_, this->length_, other.data_,
                              other.length_);
      default: {
        LOG_WARN("unsupported type: %d", this->attr_type_);
      }
    }
  } else if (this->attr_type_ == INTS && other.attr_type_ == FLOATS) {
    float this_data = *(int *)data_;
    return compare_float(&this_data, other.data_);
  } else if (this->attr_type_ == FLOATS && other.attr_type_ == INTS) {
    float other_data = *(int *)other.data_;
    return compare_float(data_, &other_data);
  } else if (this->attr_type_ == CHARS) {
    std::string this_chars_ptr = std::string(this->data_, this->length_);
    float this_result = std::atof(this_chars_ptr.c_str());

    if (other.attr_type_ == FLOATS) {
      return compare_float(&this_result, other.data_);
    } else if (other.attr_type_ == INTS) {
      float other_data_int = *(int *)other.data_;
      return compare_float(&this_result, &other_data_int);
    }
  } else if (other.attr_type_ == CHARS) {
    std::string other_chars_ptr = std::string(other.data_, other.length_);
    float other_result = std::atof(other_chars_ptr.c_str());
    // return compare_float(&this_result, &other_result);
    if (this->attr_type_ == FLOATS) {
      return compare_float(data_, &other_result);
    } else if (this->attr_type_ == INTS) {
      float this_data_float = *(int *)this->data_;
      return compare_float(&this_data_float, &other_result);
    }
  }
  LOG_WARN("not supported");
  return -1;  // TODO return rc?
}

int TupleCell::like_compare(const TupleCell &other) const
{
  // 这里实现两个表达式的模糊匹配
  // % _
  if(this->attr_type_!=CHARS||other.attr_type_!=CHARS){
    return -1;
  }
  // a是目标串 b是正则串 
  // const char* a=this->data_;
  // const char* b=other.data_;
  // 做个转换 将%->* _->. *->\* .->\.
  std::string target=std::string(this->data_);
  std::string regular=std::string(other.data_);
  regular=subreplace(regular,"*","\*");
  regular=subreplace(regular,".","\.");
  regular=subreplace(regular,"%",".*");
  regular=subreplace(regular,"_",".");
  std::regex rePattern(regular);
  int ret = std::regex_match(target, rePattern);

  return ret;

  
}