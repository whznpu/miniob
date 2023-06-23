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
// Created by Meiyi & Wangyunlai.wyl on 2021/5/18.
//

#include "storage/common/index_meta.h"
#include "storage/common/field_meta.h"
#include "storage/common/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "rc.h"
#include "json/json.h"
#include "util/util.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_IS_UNIQUE("is_unique");

RC IndexMeta::init(const char *name, const std::vector<FieldMeta> &fields, const bool &is_unique)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  for (auto field: fields) {
    fields_.push_back(field.name());
  }
  is_unique_ = is_unique;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  json_value[FIELD_FIELD_NAME] = union_with(fields_, '_');
  json_value[FIELD_IS_UNIQUE] = is_unique_ ? "1" : "0";
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &fields_value = json_value[FIELD_FIELD_NAME];
  const Json::Value &is_unique = json_value[FIELD_IS_UNIQUE];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::GENERIC_ERROR;
  }

  if (!fields_value.isString()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(),
        fields_value.toStyledString().c_str());
    return RC::GENERIC_ERROR;
  }

  std::string fields_str = fields_value.asCString();
  std::vector<std::string> fields = splitstr(fields_str, '_');
  std::vector<FieldMeta> fields_meta;
  for (auto field: fields) {
    const FieldMeta *field_meta = table.field(field.c_str());
    if (nullptr == field_meta) {
      LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    fields_meta.push_back(*field_meta);
  }

  return index.init(name_value.asCString(), fields_meta, is_unique.asString() == "1");
  // return index.init(name_value.asCString(), *field, false);
}

const char *IndexMeta::name() const
{
  return name_.c_str();
}

const std::vector<std::string> IndexMeta::fields() const
{
  return fields_;
}

const bool IndexMeta::is_unique() const
{
  return is_unique_;
}

void IndexMeta::desc(std::ostream &os) const
{
  os << "index name=" << name_ << ", fields=";
  for (auto field: fields_) {
    os << field << ",";
  }
  os << " is_unique=" << is_unique_;
}