/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its
affiliates. All rights reserved. miniob is licensed under Mulan PSL v2. You can
use this software according to the terms and conditions of the Mulan PSL v2. You
may obtain a copy of Mulan PSL v2 at: http://license.coscl.org.cn/MulanPSL2 THIS
SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by wangyunlai on 2022/9/28
//

#pragma once

#include <string>
#include <vector>
#include "sql/parser/parse_defs.h"

std::string double2string(double v);
std::string date2string(int v);

std::vector<std::string> splitstr(const std::string& str, char tag);

std::string union_with(const std::vector<std::string> strs, char tag);

int strcmp_prefix(const char *s1, const char *s2, size_t n);
bool typecast_whz(Value *value_to_cast, AttrType dest_field_type);
int my_round_whz(float value);
