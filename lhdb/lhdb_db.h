//
//  lhdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_LHDB_DB_H_
#define YCSB_C_LHDB_DB_H_

#include <iostream>
#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"
#include "level_hashing.h"

namespace ycsbc {

class LhdbDB : public DB {
 public:
  LhdbDB() {}
  ~LhdbDB() {}

  void Init();

  void Cleanup();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(key, result);
  }

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return kNotImplemented;
  }

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return kNotImplemented;
  }

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return kNotImplemented;
  }

  Status ReadSingleEntry(const std::string &key, std::vector<Field> &result);
  Status InsertSingleEntry(const std::string &key, std::vector<Field> &values);
  //Status DeleteSingleEntry(const std::string &key);

  Status (LeveldbDB::*method_read_)(const std::string &key, std::vector<Field> &result);
  Status (LeveldbDB::*method_insert_)(const std::string &key, std::vector<Field> &values);
  //Status (LeveldbDB::*method_delete_)(const std::string &key);

  std::string field_prefix_;

  static leveldb::level_hash *db_;
  static int ref_cnt_;
  static std::mutex mu_;
};

DB *NewLhdbDB();

} // ycsbc

#endif // YCSB_C_LHDB_DB_H_

