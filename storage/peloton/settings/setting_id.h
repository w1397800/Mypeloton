//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// settings_type.h
//
// Identification: src/include/settings/settings_type.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once



enum class SettingId {
  #define __SETTING_ENUM__
  #include "storage/peloton/settings/settings_macro.h"
  #include "storage/peloton/settings/settings.h"
  #undef __SETTING_ENUM__
};


