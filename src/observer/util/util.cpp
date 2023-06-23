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

#include "util/util.h"

#include <string.h>

#include <vector>
#include <iostream>
#include <math.h>
#include "common/log/log.h"

std::string double2string(double v) {
  char buf[256];
  snprintf(buf, sizeof(buf), "%.2f", v);
  size_t len = strlen(buf);
  while (buf[len - 1] == '0') {
    len--;
  }
  if (buf[len - 1] == '.') {
    len--;
  }

  return std::string(buf, len);
}
std::string date2string(int v) {
  char buf[16] = {0};
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d", v / 10000, (v % 10000) / 100,
           v % 100);  // 注意这里月份和天数，不足两位时需要填充0
  return std::string(buf);
}

std::vector<std::string> splitstr(const std::string& str, char tag) {
  std::vector<std::string> li;
  std::string subStr;

  //遍历字符串，同时将i位置的字符放入到子串中，当遇到tag（需要切割的字符时）完成一次切割
  //遍历结束之后即可得到切割后的字符串数组
  for (size_t i = 0; i < str.length(); i++) {
    if (tag == str[i]) {
      if (!subStr.empty()) {
        li.push_back(subStr);
        subStr.clear();
      }
    } else {
      subStr.push_back(str[i]);
    }
  }

  if (!subStr.empty()) {
    li.push_back(subStr);
  }

  return li;
}

std::string union_with(const std::vector<std::string> strs, char tag) {
  std::string result;
  for (auto str : strs) {
    result = result + str + tag;
  }
  return result;
}

int strcmp_prefix(const char *s1, const char *s2, size_t n) {
  for (size_t i = 0; i < n; i++) {
    if (s1[i] < s2[i]) {
      return -1;
    }
    if (s1[i] > s2[i]) {
      return 1;
    }
  }
  return 0;
}

int my_round_whz(float value) {
  int result = INT32_MAX;
  float floor_v = floorf(value);
  float ceil_v = ceilf(value);
  result = (2 * value) >= (floor_v + ceil_v) ? ceil_v : floor_v;

  return result;
}

bool typecast_whz(Value *value_to_cast, AttrType dest_field_type) {
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
        int result = my_round_whz(floats_v);

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