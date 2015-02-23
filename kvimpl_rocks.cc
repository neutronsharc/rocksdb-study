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
#include <typeinfo>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "debug.h"
#include "hash.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "rocksdb_tuning.h"

/*
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
} */

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

bool RocksDBShard::MultiGet(vector<KVRequest*> &requests) {
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
    if (p->reserved) {
      MultiCompletion* comp = (MultiCompletion*)p->reserved;
      comp->AddFinish();
    }
  }
  return true;
}

// Multi-get.
bool RocksDBShard::MultiGet(KVRequest* requests, int numRequests) {
  vector<KVRequest*> vecRqsts;
  for (int i = 0; i < numRequests; i++) {
    vecRqsts.push_back(requests + i);
  }
  return MultiGet(vecRqsts);
}


bool RocksDBInterface::Open(const char* dbPath,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB) {
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(numShards * 4, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(numShards * 2, rocksdb::Env::Priority::HIGH);

  int perShardCacheMB = blockCacheMB / numShards;

  // Open the thread pool.
  numIOThreads_ = numIOThreads;
  threadPool_.reset(new ThreadPool(numIOThreads, this));
  threadPool_->SetKVStore(this);
  printf("DB interface %s: created IO pool with %d threads\n",
         dbPath, numIOThreads);

  // Open DB shards.
  dbPath_ = dbPath;
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
void RocksDBInterface::PostRequest(void* p) {
  threadPool_->AddWork(p);
}

// Process the given request in sync way.
bool RocksDBInterface::ProcessRequest(void* p) {
  QueuedTask* task = (QueuedTask*)p;

  if (task->type == MULTI_GET) {
    PerShardMultiGet* mget = task->task.mget;
    int shardID = mget->shardID;
    return dbShards_[shardID]->MultiGet(mget->requests);
  }

  KVRequest* request = (KVRequest*)(task->task.request);
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

// Multi-get. The individual get rqsts will be distributed to all shards
// within this DB interface.
bool RocksDBInterface::MultiGet(KVRequest* requests, int numRequests) {
  PerShardMultiGet perShardRqsts[numberOfShards_];

  int shardsUsed = 0;
  int lastShardUsed = -1;

  // Assign the get requests to DB shards.
  for (int i = 0; i < numRequests; i++) {
    KVRequest* p = requests + i;
    p->reserved = NULL;
    uint32_t hv = bobhash(p->key, p->keylen, 0);
    int shardId = hv % numberOfShards_;
    if (perShardRqsts[shardId].requests.size() == 0) {
      shardsUsed++;
      lastShardUsed = shardId;;
    }
    perShardRqsts[shardId].requests.push_back(requests + i);
    perShardRqsts[shardId].shardID = shardId;
  }

  // If only one shard is used.
  if (shardsUsed == 1) {
    return dbShards_[lastShardUsed]->MultiGet(
                perShardRqsts[lastShardUsed].requests);
  }

  // We have split the big multi-get into multiple per-shard mget.
  MultiCompletion comp(numRequests);
  for (int i = 0; i < numRequests; i++) {
    KVRequest *p = requests + i;
    p->reserved = (void*)&comp;
  }
  QueuedTask tasks[shardsUsed];
  int postedShards = 0;
  for (int i = 0; i < numberOfShards_; i++) {
    if (perShardRqsts[i].requests.size() == 0) {
      continue;
    }
    dbg("post mget %d rqsts to shard %d\n",
        perShardRqsts[i].requests.size(), i);

    tasks[postedShards].type = MULTI_GET;
    tasks[postedShards].task.mget = &perShardRqsts[i];

    PostRequest((void*)(tasks + postedShards));
    postedShards++;
  }
  // wait for these requests to complete.
  comp.WaitForCompletion();
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

