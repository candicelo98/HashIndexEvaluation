//
//  leveldb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "napdb_db.h"
#include "core/properties.h"
#include "core/utils.h"
#include "core/core_workload.h"
#include "core/db_factory.h"

#include "include/slice.h"

namespace {
  const std::string PROP_NUM_OF_THREAD = "napdb.num_of_thread";
  const std::string PROP_NUM_OF_THREAD_DEFAULT = "8";

  const std::string PROP_DRAM_ENTRIES = "napdb.dram_entries";
  const std::string PROP_DRAM_ENTRIES_DEFAULT = "100000"; // 100k

  const std::string PROP_NVM_ENTRIES_INIT = "napdb.nvm_entries_init";
  const std::string PROP_NVM_ENTRIES_INIT_DEFAULT = "524288"; // 2M/4
} // anonymous

nap::HT *NapdbDB::db_ = nullptr;
int NapdDB::ref_cnt_ = 0;
std::mutex NapdbDB::mu_;

void NapdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  
  method_read_ = &LeveldbDB::ReadSingleEntry;
  method_insert_ = &LeveldbDB::InsertSingleEntry;
  method_delete_ = &LeveldbDB::DeleteSingleEntry;
  
  field_prefix_ = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);

  ref_cnt_++;
  if(db_){
    return;
  }

  int thread_cnt_ = std::stoi(props.GetProperty(PROP_NUM_OF_THREAD, PROP_NUM_OF_THREAD_DEFAULT));
  if (thread_cnt_ < 2) {
    throw utils::Exception("Total thread count less than 2");
  }
  int dram_ety_cnt_ = std::stoi(props.GetProperty(PROP_DRAM_ENTRIES, PROP_DRAM_ENTRIES_DEFAULT));
  int nvm_ety_cnt_ = std::stoi(props.GetProperty(PROP_NVM_ENTRIES, PROP_NVM_ENTRIES_DEFAUKT));

  if(!nap::HT::newHashTable(dram_ety_cnt_, nvm_entry_cnt_, &db_)){
    throw::utils::Exception("Fail to create hash table");
  }
}

void NapdbDB::Cleanup() {
  const std::lock_guard<std::mutex> lock(mu_);
  if (--ref_cnt_) {
    return;
  }
  delete db_;
}

DB::Status NapdbDB::ReadSingleEntry(const std::string &key, std::vector<Field> &result){
  std::string data;
  bool s = db_->Get(Slice(key), data);
  if(!s){
    return kNotFound;
  }else{
    values.push_back({field_prefix_.append(0), data});
  }
  return kOK;
}

DB::Status NapdbDB::InsertSingleEntry(const std::string &key, std::vector<Field> &values){
  db_->Put(Slice(key), Slice(values[0].value));
  return kOK;
}

DB::Status NapdbDB::DeleteSingleEntry(const std::string &key){
  return kNotImplemented;
}

DB *NewNapdbDB() {
  return new NapdbDB;
}

const bool registered = DBFactory::RegisterDB("leveldb", NewLeveldbDB);

} // ycsbc
