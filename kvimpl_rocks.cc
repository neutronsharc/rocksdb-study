#include <time.h>
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "debug.h"
#include "hash.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "rocksdb_tuning.h"


static void ProcessOneRequest(void* p) {
    KVRequest* request = (KVRequest*)p;
    //DumpKVRequest(request);
    switch (request->type) {
    case GET:
      break;
    case PUT:
      break;
    case DELETE:
      break;
    default:
      printf("unknown rqst\n");
    }
}

bool RocksDBShard::OpenDB(const string& dbPath,
                          int blockCacheMB,
                          rocksdb::Env* env) {
  options_.env = env;
  TuneUniversalStyleCompaction(&options_, blockCacheMB);

  writeOptions_.disableWAL = true;

  // save index/filter/data blocks in block cache.
  readOptions_.fill_cache = true;
  readOptions_.verify_checksums = true;

  rocksdb::Status s = rocksdb::DB::Open(options_, dbPath, &db_);
  assert(s.ok());
  printf("Have opened DB %s\n", dbPath.c_str());

  dbPath_ = dbPath;
  return true;
}

bool RocksDBShard::Get(KVRequest*  p)  {
  string value;
  rocksdb::Status status = db_->Get(readOptions_, p->key, &value);
  if (status.ok()) {
    p->vlen = value.length();
    p->value = (char*)malloc(p->vlen);
    if (!p->value) {
      err("key %s: fail to malloc %d bytes\n", p->key, p->vlen);
      p->retcode = NO_MEM;
    } else {
      memcpy(p->value, value.data(), p->vlen);
      dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
      p->retcode = SUCCESS;
    }
  } else {
    dbg("key %s not exist\n", p->key);
    p->value = NULL;
    p->retcode = NOT_EXIST;
  }
  // Send completion signal.
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

bool RocksDBShard::Put(KVRequest*  p)  {
  rocksdb::Slice key(p->key, p->keylen);
  rocksdb::Slice value(p->value, p->vlen);
  rocksdb::Status status = db_->Put(writeOptions_, key, value);
  dbg("key %s: put value %s\n", p->key, p->value);
  if (status.ok()) {
    p->retcode = SUCCESS;
  } else {
    p->retcode = FAILURE;
  }
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

bool RocksDBShard::Delete(KVRequest*  p) {
  rocksdb::Status status = db_->Delete(writeOptions_, p->key);
  dbg("delete key %s: ret = %s\n", p->key, status.ToString().c_str());
  if (status.ok()) {
  } else {
    dbg("delete key %s failed\n", p->key);
  }
  p->retcode = SUCCESS;
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

bool RocksDBShard::MultiGet(vector<KVRequest*> requests) {
  vector<rocksdb::Slice> keySlices;
  vector<string> values;
  int numRequests = requests.size();

  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests.at(i);
    assert(p->type == GET);
    keySlices.push_back(rocksdb::Slice(p->key, p->keylen));
  }

  vector<rocksdb::Status> rets = db_->MultiGet(readOptions_, keySlices, &values);
  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests.at(i);
    if (rets[i].ok()) {
      p->vlen = values[i].length();
      p->value = (char*)malloc(p->vlen);
      if (!p->value) {
        err("key %s: fail to malloc %d bytes\n", p->key, p->vlen);
        p->retcode = NO_MEM;
      } else {
        memcpy(p->value, values[i].data(), p->vlen);
        dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
        p->retcode = SUCCESS;
      }
    } else {
      dbg("failed to get key %s: ret: %s\n", p->key, rets[i].ToString().c_str());
      p->vlen = 0;
      p->value = NULL;
      p->retcode = NOT_EXIST;
    }
  }
  return true;
}

// Multi-get.
bool RocksDBShard::MultiGet(KVRequest* requests, int numRequests) {
  vector<rocksdb::Slice> keySlices;
  vector<string> values;

  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    assert(p->type == GET);
    keySlices.push_back(rocksdb::Slice(p->key, p->keylen));
  }

  vector<rocksdb::Status> rets = db_->MultiGet(readOptions_, keySlices, &values);
  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    if (rets[i].ok()) {
      p->vlen = values[i].length();
      p->value = (char*)malloc(p->vlen);
      if (!p->value) {
        err("key %s: fail to malloc %d bytes\n", p->key, p->vlen);
        p->retcode = NO_MEM;
      } else {
        memcpy(p->value, values[i].data(), p->vlen);
        dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
        p->retcode = SUCCESS;
      }
    } else {
      dbg("failed to get key %s: ret: %s\n", p->key, rets[i].ToString().c_str());
      p->vlen = 0;
      p->value = NULL;
      p->retcode = NOT_EXIST;
    }
  }
  return true;

}


bool RocksDBInterface::Open(const char* dbPath,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB) {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(numShards * 8, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(numShards * 2, rocksdb::Env::Priority::HIGH);

  int perShardCacheMB = blockCacheMB / numShards;

  for (int i = 0; i < numShards; i++) {
    RocksDBShard* shard = new RocksDBShard();
    assert(shard != NULL);

    string shardPath = string(dbPath) + string("/shard-") + std::to_string(i);
    assert(shard->OpenDB(shardPath, perShardCacheMB, env));

    dbShards_.push_back(shard);
  }
  numberOfShards_ = dbShards_.size();
  return true;
}

bool RocksDBInterface::OpenDB(const char* dbPath,
                              int numIOThreads,
                              int blockCacheMB) {

  TuneUniversalStyleCompaction(&options_, blockCacheMB);
  //options_.compaction_options_universal.max_size_amplification_percent = 20;
  //options_.compression = rocksdb::kNoCompression;
  /*
  // Set num of threads in Low/High thread pools.
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(16, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options_.IncreaseParallelism();
  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  //options_.OptimizeLevelStyleCompaction();
  options_.OptimizeUniversalStyleCompaction();

  // point lookup: will create hash index, 10-bits bloom filter,
  // a block-cache of this size in MB, 4KB block size,
  // Usually let's use 1024 MB block cache.
  unsigned long block_cache_mb = blockCacheMB;
  options_.OptimizeForPointLookup(block_cache_mb);

  // create the DB if it's not already present
  options_.create_if_missing = true;
  options_.max_open_files = 4096;
  //options_.allow_os_buffer = false;
  options_.write_buffer_size = 1024L * 1024 * 128;
  options_.max_write_buffer_number = 64;

  // Default write-buffer size = 128MB, generally we want
  // wb-szie * min-wb-to-merge * l0-file-num equal-to L1 file size
  // = (max_bytes_for_level_base)  = 512MB
  options_.min_write_buffer_number_to_merge = 2;
  options_.level0_file_num_compaction_trigger = 4;

  //options_.compression = rocksdb::kNoCompression;
  options_.compression = rocksdb::kSnappyCompression;

  //options_.disable_auto_compactions = true;
  //options_.max_background_compactions = 16;
  options_.max_background_flushes = 2;
  options_.env = env;
  */

  writeOptions_.disableWAL = true;

  // save index/filter/data blocks in block cache.
  readOptions_.fill_cache = true;
  readOptions_.verify_checksums = true;

  rocksdb::Status s = rocksdb::DB::Open(options_, dbPath, &db_);
  assert(s.ok());
  printf("Have opened DB %s\n", dbPath);

  dbPath_ = dbPath;

  // Open the thread pool.
  numIOThreads_ = numIOThreads;
  threadPool_.reset(new ThreadPool(numIOThreads, this));
  //threadPool_->SetDataProcessor(ProcessOneRequest);
  threadPool_->SetKVStore(this);
  printf("Have created IO thread pool of %d threads\n", numIOThreads);

  return true;
}

// Post a request to worker thread pool.
void RocksDBInterface::PostRequest(KVRequest* p) {
  threadPool_->AddWork(p);
}

// Process the given request in sync way.
bool RocksDBInterface::ProcessRequest(KVRequest* request) {
  switch (request->type) {
  case GET:
    return Get(request);
  case PUT:
    return Put(request);
  case DELETE:
    return Delete(request);
  default:
    err("unknown rqst: \n");
    DumpKVRequest(request);
    request->retcode = FAILURE;
    if (request->reserved) {
      MultiCompletion* comp = (MultiCompletion*)request->reserved;
      comp->AddFinish();
    }
  }
  dbg("have processed rqsts\n");
  return true;
}

bool RocksDBInterface::Get(KVRequest*  p)  {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = dbShards_[hv % numberOfShards_];
  return db->Get(p);

  string value;
  rocksdb::Status status = db_->Get(readOptions_, p->key, &value);
  if (status.ok()) {
    p->vlen = value.length();
    p->value = (char*)malloc(p->vlen);
    if (!p->value) {
      err("key %s: fail to malloc %d bytes\n", p->key, p->vlen);
      p->retcode = NO_MEM;
    } else {
      memcpy(p->value, value.data(), p->vlen);
      dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
      p->retcode = SUCCESS;
    }
  } else {
    dbg("key %s not exist\n", p->key);
    p->value = NULL;
    p->retcode = NOT_EXIST;
  }
  // Send completion signal.
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

bool RocksDBInterface::Put(KVRequest*  p)  {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = dbShards_[hv % numberOfShards_];
  return db->Put(p);

  rocksdb::Slice key(p->key, p->keylen);
  rocksdb::Slice value(p->value, p->vlen);
  rocksdb::Status status = db_->Put(writeOptions_, key, value);
  dbg("key %s: put value %s\n", p->key, p->value);
  if (status.ok()) {
    p->retcode = SUCCESS;
  } else {
    p->retcode = FAILURE;
  }
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

bool RocksDBInterface::Delete(KVRequest*  p) {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = dbShards_[hv % numberOfShards_];
  return db->Delete(p);

  rocksdb::Status status = db_->Delete(writeOptions_, p->key);
  dbg("delete key %s: ret = %s\n", p->key, status.ToString().c_str());
  if (status.ok()) {
  } else {
    dbg("delete key %s failed\n", p->key);
  }
  p->retcode = SUCCESS;
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

// Multi-get.
bool RocksDBInterface::MultiGet(KVRequest* requests, int numRequests) {
  vector<KVRequest*> perShardRqsts[numberOfShards_];

  for (int i = 0; i < numRequests; i++) {
    KVRequest* p = requests + i;
    uint32_t hv = bobhash(p->key, p->keylen, 0);
    int shardId = hv % numberOfShards_;
    perShardRqsts[shardId].push_back(requests + i);
  }

  for (int i = 0; i < numberOfShards_; i++) {
    if (perShardRqsts[i].size() > 0) {
      dbShards_[i]->MultiGet(perShardRqsts[i]);
    }
    // TODO: append a multi-completion to requests.
  }
  return true;

  vector<rocksdb::Slice> keySlices;
  vector<string> values;

  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    assert(p->type == GET);
    keySlices.push_back(rocksdb::Slice(p->key, p->keylen));
  }

  vector<rocksdb::Status> rets = db_->MultiGet(readOptions_, keySlices, &values);
  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    if (rets[i].ok()) {
      p->vlen = values[i].length();
      p->value = (char*)malloc(p->vlen);
      if (!p->value) {
        err("key %s: fail to malloc %d bytes\n", p->key, p->vlen);
        p->retcode = NO_MEM;
      } else {
        memcpy(p->value, values[i].data(), p->vlen);
        dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
        p->retcode = SUCCESS;
      }
    } else {
      dbg("failed to get key %s: ret: %s\n", p->key, rets[i].ToString().c_str());
      p->value = NULL;
      p->retcode = NOT_EXIST;
    }
  }
  return true;

}

