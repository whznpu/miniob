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

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/common/db.h"
#include "storage/common/table.h"

UpdateStmt::UpdateStmt(Table *table, const Value *value, int value_amount, int value_index, FilterStmt *filter_stmt)
    : table_(table), value_(value), value_amount_(value_amount), value_index_(value_index), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const Updates &update_sql, Stmt *&stmt)
{
  // TODO
  const char *table_name = update_sql.relation_name;
  // 检查db
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  // 检查表是否存在
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  // check the value type
  const Value *value = &(update_sql.value);
  // const int value_num= 1;
  const TableMeta &table_meta = table->table_meta();
  // 找到对应的filed序号并且传入stmt中
  const int sys_field_num = table_meta.sys_field_num();
  int value_index = 0, flag = 0;
  // 这里莫名其妙会报错 多一个
  for (int i = 0; i < table_meta.field_num() - 1; i++) {

    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    // 找到名字一样的列
    if (strcmp(field_meta->name(), update_sql.attribute_name) == 0) {
      const AttrType field_type = field_meta->type();
      const AttrType value_type = value[0].type;
      if (field_type != value_type) {
        if(field_type == TEXTS&& value_type==CHARS){
          // continue;
          value_index = i;
          flag = 1;
          break;
        }

        LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
            table_name,
            field_meta->name(),
            field_type,
            value_type);
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      value_index = i;
      flag = 1;
      break;
    }
  }
  if (flag == 0) {
    // 没找到就返回不匹配
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map, update_sql.conditions, update_sql.condition_num, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  stmt = new UpdateStmt(table, value, 1, value_index, filter_stmt);
  return rc;

  // stmt = nullptr;
  // return RC::INTERNAL;
}
