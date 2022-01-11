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

namespace {
  const std::string PROP_NUM_OF_THREAD = "napdb.num_of_thread";
  const std::string PROP_NUM_OF_THREAD_DEFAULT = "8";

  const std::string PROP_DRAM_ENTRIES = "napdb.dram_entries";
  const std::string PROP_DRAM_ENTRIES_DEFAULT = "100000"; // 100k

  const std::string PROP_NVM_ENTRIES_INIT = "napdb.nvm_entries_init";
  const std::string PROP_NVM_ENTRIES_INIT_DEFAULT = "524288"; // 2M/4
} // anonymous

namespace ycsbc {

nap::HT *NapdbDB::db_ = nullptr;
int NapdDB::ref_cnt_ = 0;
std::mutex NapdbDB::mu_;

void NapdbDB::Init() {
  const std::lock_guard<std::mutex> lock(mu_);

  const utils::Properties &props = *props_;
  
  method_read_ = &LeveldbDB::ReadSingleEntry;
  method_insert_ = &LeveldbDB::InsertSingleEntry;
  method_delete_ = &LeveldbDB::DeleteSingleEntry;
  
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

void LeveldbDB::GetOptions(const utils::Properties &props, leveldb::Options *opt) {
  size_t writer_buffer_size = std::stol(props.GetProperty(PROP_WRITE_BUFFER_SIZE,
                                                          PROP_WRITE_BUFFER_SIZE_DEFAULT));
  if (writer_buffer_size > 0) {
    opt->write_buffer_size = writer_buffer_size;
  }
  size_t max_file_size = std::stol(props.GetProperty(PROP_MAX_FILE_SIZE,
                                                     PROP_MAX_FILE_SIZE_DEFAULT));
  if (max_file_size > 0) {
    opt->max_file_size = max_file_size;
  }
  size_t cache_size = std::stol(props.GetProperty(PROP_CACHE_SIZE,
                                                  PROP_CACHE_SIZE_DEFAULT));
  if (cache_size >= 0) {
    opt->block_cache = leveldb::NewLRUCache(cache_size);
  }
  int max_open_files = std::stoi(props.GetProperty(PROP_MAX_OPEN_FILES,
                                                   PROP_MAX_OPEN_FILES_DEFAULT));
  if (max_open_files > 0) {
    opt->max_open_files = max_open_files;
  }
  std::string compression = props.GetProperty(PROP_COMPRESSION,
                                              PROP_COMPRESSION_DEFAULT);
  if (compression == "snappy") {
    opt->compression = leveldb::kSnappyCompression;
  } else {
    opt->compression = leveldb::kNoCompression;
  }
  int filter_bits = std::stoi(props.GetProperty(PROP_FILTER_BITS,
                                                PROP_FILTER_BITS_DEFAULT));
  if (filter_bits > 0) {
    opt->filter_policy = leveldb::NewBloomFilterPolicy(filter_bits);
  }
}


DB::Status LeveldbDB::InsertCompKey(const std::string &table, const std::string &key,
                                    std::vector<Field> &values) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (Field &field : values) {
    comp_key = BuildCompKey(key, field.name);
    batch.Put(comp_key, field.value);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

DB::Status LeveldbDB::DeleteCompKey(const std::string &table, const std::string &key) {
  leveldb::WriteOptions wopt;
  leveldb::WriteBatch batch;

  std::string comp_key;
  for (int i = 0; i < fieldcount_; i++) {
    comp_key = BuildCompKey(key, field_prefix_ + std::to_string(i));
    batch.Delete(comp_key);
  }

  leveldb::Status s = db_->Write(wopt, &batch);
  if (!s.ok()) {
    throw utils::Exception(std::string("LevelDB Write: ") + s.ToString());
  }
  return kOK;
}

DB *NewLeveldbDB() {
  return new LeveldbDB;
}

const bool registered = DBFactory::RegisterDB("leveldb", NewLeveldbDB);

} // ycsbc
