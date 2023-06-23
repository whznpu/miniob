/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"

#include <math.h>
#include <stdio.h>
#include <string.h>

#include <iostream>
#include <sstream>

#include "common/log/log.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

bool typecast(Value *value_to_cast, AttrType dest_field_type);

InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount,int value_list_length)
    : table_(table), values_(values), value_amount_(value_amount), value_list_length_(value_list_length) {}

RC InsertStmt::create(Db *db, const Inserts &inserts, Stmt *&stmt) {
  const char *table_name = inserts.relation_name;
  if (nullptr == db || nullptr == table_name || inserts.value_num <= 0) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d", db,
             table_name, inserts.value_num);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  // 循环 将值存储在列表的格式  不需要改值

  // check the fields number 循环检查
  int insert_num = inserts.value_insert_length;
  const int value_num = inserts.value_num / insert_num;
  const Value *value_all = inserts.values;
  for (int j = 0; j < insert_num; j++) {
    // const Value *values = &inserts.values[j * value_num];
    const Value *values =&value_all[j*value_num];

    const TableMeta &table_meta = table->table_meta();
    const int field_num = table_meta.field_num() - table_meta.sys_field_num();
    if (field_num != value_num) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d",
               value_num, field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values[i].type;
    if (field_type !=
        value_type) {  // TODO try to convert the value type to field type
      if (typecast((Value *)&(values[i]), field_type)) {
        //如果可以进行类型转换,将直接进行下一个field的判断.

        continue;
      }
      LOG_WARN(
          "field type mismatch. table=%s, field=%s, field type=%d, "
          "value_type=%d",
          table_name, field_meta->name(), field_type, value_type);
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num,insert_num);
  return RC::SUCCESS;
  }
}



int my_round(float value) {
  int result = INT32_MAX;
  float floor_v = floorf(value);
  float ceil_v = ceilf(value);
  result = (2 * value) >= (floor_v + ceil_v) ? ceil_v : floor_v;

  return result;
}

bool typecast(Value *value_to_cast, AttrType dest_field_type) {
  const AttrType origin_type = value_to_cast->type;
  switch (origin_type) {
    case AttrType::INTS: {
      int int_v;
      memcpy(&int_v, value_to_cast->data, sizeof(int_v));
      std::cout << int_v << std::endl;
      if (dest_field_type == AttrType::FLOATS) {
        float result = int_v;
        value_to_cast->type = AttrType::FLOATS;
        value_to_cast->data = malloc(sizeof(result));
        memcpy(value_to_cast->data, &result, sizeof(result));
        return true;
      } else if (dest_field_type == AttrType::CHARS) {
        char *result_chars;
        std::string str_tmp = std::to_string(int_v);
        value_to_cast->type = AttrType::CHARS;
        value_to_cast->data = strdup(str_tmp.c_str());
        return true;
      }
      return false;
    } break;
    case AttrType::FLOATS: {
      float floats_v;
      memcpy(&floats_v, value_to_cast->data, sizeof(floats_v));
      if (dest_field_type == AttrType::INTS) {
        int result = my_round(floats_v);

        value_to_cast->type = AttrType::INTS;
        value_to_cast->data = malloc(sizeof(result));
        memcpy(value_to_cast->data, &result, sizeof(result));
        return true;
      } else if (dest_field_type == AttrType::CHARS) {
        char *result_chars;
        std::string str_tmp = std::to_string(floats_v);
        int str_size = str_tmp.size();
        int cut_pos = str_tmp.find_last_of("0");
        //这部分逻辑用于消除浮点数转化为字符串之后的尾部零
        if (str_size - 1 == cut_pos) {
          while (cut_pos >= 0 && cut_pos < str_size) {
            cut_pos--;
            if (str_tmp[cut_pos] != '0') {
              break;
            }
          }
        }
        std::string res_str = str_tmp.substr(0, cut_pos + 1);

        value_to_cast->type = AttrType::CHARS;
        value_to_cast->data = strdup(res_str.c_str());
        return true;
      }
      return false;
    } break;
    case AttrType::CHARS: {
      if(dest_field_type==AttrType::TEXTS){
        return true;
      }
      char *char_v;
      char_v = strdup((const char *)(value_to_cast->data));
      std::string str_tmp(char_v);
      int cut_pos = str_tmp.find_first_not_of("0123456789.");
      std::string res_str = str_tmp.substr(0, cut_pos);
      cut_pos = res_str.find_last_not_of("0");
      std::string res_str2 = res_str.substr(0, cut_pos);

      if (dest_field_type == AttrType::INTS) {
        int result_v = std::atoi(res_str.c_str());
        value_to_cast->type = AttrType::INTS;
        memcpy(value_to_cast->data, &result_v, sizeof(result_v));
        return true;

      } else if (dest_field_type == AttrType::FLOATS) {
        float result_v = std::atof(res_str.c_str());
        value_to_cast->type = AttrType::FLOATS;
        memcpy(value_to_cast->data, &result_v, sizeof(result_v));
        return true;
      }

      return false;
    } break;
    case AttrType::TEXTS: {
      if(value_to_cast->type==AttrType::CHARS){
        return true;
      }
    }break;
    case AttrType::DATES: {
      return false;
    } break;
    defualt : {
      LOG_WARN(" Unkown AttrType, implement please");
      return false;
    } break;
  }
  return true;
}
