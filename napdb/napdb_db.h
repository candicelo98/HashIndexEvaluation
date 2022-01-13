//
//  napdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_NAPDB_DB_H_
#define YCSB_C_NAPDB_DB_H_

#include <iostream>
#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"

namespace ycsbc {

class NapdbDB : public DB {
 public:
  NapdbDB() {}
  ~NapdbDB() {}

  /* Initializes any state for accessing this DB */
  void Init();

  /* Clears any state for accessing this DB */
  void Cleanup();

  /* Read a record from the database */
  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    return (this->*(method_read_))(key, result);
  }
  
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return (this->*(method_insert_))(key, values);
  }

  Status Delete(const std::string &table, const std::string &key) {
    return (this->*(method_delete_))(key);
  }

  /* Do not support method Scan. */
  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    return kNotImplemented;
  }

  /* Do not support method Update. */
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    return kNotImplemented;
  }

 private:
  Status ReadSingleEntry(const std::string &key, std::vector<Field> &result);
  Status InsertSingleEntry(const std::string &key, std::vector<Field> &values);
  Status DeleteSingleEntry(const std::string &key);

  Status (NapdaDB::*method_read_)(const std::string &key, std::vector<Field> &result);
  Status (NapdbDB::*method_insert_)(const std::string &key, std::vector<Field> &values);
  Status (NapdbDB::*method_delete_)(const std::string &key);

  static int ref_cnt_;
  static std::mutex mu_;
  static nap::HT *db_;

  std::string field_prefix_;
};

DB *NewNapdbDB();

} // ycsbc

#endif // YCSB_C_NAPDB_DB_H_

