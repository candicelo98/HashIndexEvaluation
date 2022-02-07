//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "lhdb_db.h"
#include "core/properties.h"
#include "core/utils.h"
#include "core/core_workload.h"
#include "core/db_factory.h"

namespace {
  const std::string PROP_WRITE_LATENCY = "lhdb.write_latency";
  const std::string PROP_WRITE_LATENCY_DEFAULT = "250";

  const std::string PROP_LEVEL_SIZE = "lhdb.level_size";
  const std::string PROP_LEVEL_SIZE_DEFAULT = "21";
} // anonymous

namespace ycsbc {

level_hash *LhdbDB::db_ = nullptr;
int LhdbDB::ref_cnt_ = 0;
std::mutex LhdbDB::mu_;

void LhdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  const std::string &format = props.GetProperty(PROP_FORMAT, PROP_FORMAT_DEFAULT);

  method_read_ = &LeveldbDB::ReadSingleEntry;
  method_insert_ = &LeveldbDB::InsertSingleEntry;
  // method_delete_ = &LeveldbDB::DeleteSingleEntry;
  
  field_prefix_ = props.GetProperty(CoreWorkload::FIELD_NAME_PREFIX,
                                    CoreWorkload::FIELD_NAME_PREFIX_DEFAULT);

  ref_cnt_++;
  if (db_) {
    return;
  }

  /* if (props.GetProperty(PROP_DESTROY, PROP_DESTROY_DEFAULT) == "true") {
    s = leveldb::DestroyDB(db_path, opt);
    if (!s.ok()) {
      throw utils::Exception(std::string("LevelDB DestroyDB: ") + s.ToString());
    }
  } */

  int write_latency = std::stoi(props.GetProperty(PROP_WRITE_LATENCY,
			 			  PROP_WRITE_LATENCY_DEFAULT));
  printf("write latency: %d\n", write_latency);
  init_pflush(2000, write_latency);

  int level_size = std::stoi(props.GetProperty(PROP_LEVEL_SIZE,
			  		       PROP_LEVEL_SIZE_DEFAULT));
  printf("level size: %d\n", level_size);

  db_ = level_init(level_size);
  if (!db_) {
    throw utils::Exception(std::string("LhdbDB fail to initialize"));
  }
}

void LeveldbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}



DB::Status LeveldbDB::ReadSingleEntry(const std::string &key, std::vector<Field> &result) {
  uint8_t key_local[KEY_LEN];
  
  snprintf(key_local, KEY_LEN, "%s", key);

  char* data = (char*)level_dynamic_query(db_, key);

  if (!data) {
    return kNotFound;
  }
  
  result->push_back({field_prefix_.append(std::to_string(0)), data});

  return kOK;
}


DB::Status LeveldbDB::InsertSingleEntry(const std::string &key, std::vector<Field> &values) {
  uint8_t key_local[KEY_LEN];

  snprintf(key_local, KEY_LEN, "%s", key);

  level_insert(db_, key_local, values[0].value.data());
  
  return kOK;
}

/* DB::Status LeveldbDB::DeleteSingleEntry(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::Status s = db_->Delete(wopt, key);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Delete: ") + s.ToString());
  }
  return k;
} */


DB *NewLhdbDB() {
  return new LhdbDB;
}

const bool registered = DBFactory::RegisterDB("lhdb", NewLhdbDB);

} // ycsbc
