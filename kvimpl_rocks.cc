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
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "debug.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"


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

bool RocksDBInterface::OpenDB(const char* dbPath, int numIOThreads) {
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options_.IncreaseParallelism();
  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options_.OptimizeLevelStyleCompaction();
  //options_.OptimizeUniversalStyleCompaction();

  // point lookup: will create hash index, 10-bits bloom filter,
  // a block-cache of this size in MB, 4KB block size,
  unsigned long block_cache_mb = 256;
  options_.OptimizeForPointLookup(block_cache_mb);

  // create the DB if it's not already present
  options_.create_if_missing = true;
  options_.max_open_files = 4096;
  options_.allow_os_buffer = false;
  options_.write_buffer_size = 1024L * 1024 * 4;
  options_.max_write_buffer_number = 200;
  options_.min_write_buffer_number_to_merge = 2;
  options_.compression = rocksdb::kNoCompression;

  writeOptions_.disableWAL = true;

  readOptions_.verify_checksums = true;
  // save index/filter/data blocks in block cache.
  readOptions_.fill_cache = true;


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

// Process the given request in sync way.
bool RocksDBInterface::ProcessRequest(KVRequest* request) {
  //DumpKVRequest(request);
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
  string value;
  rocksdb::Status status = db_->Get(readOptions_, p->key, &value);
  if (status.ok()) {
    p->vlen = value.length();
    p->value = (char*)malloc(p->vlen);
    if (!p->value) {
      err("key %s: fail to malloc %d bytes\n", p->vlen);
      p->retcode = NO_MEM;
    } else {
      memcpy(p->value, value.data(), p->vlen);
      dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, value.c_str());
      p->retcode = SUCCESS;
    }
  } else {
    err("key %s not exist\n", p->key);
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
  rocksdb::Status status = db_->Delete(writeOptions_, p->key);
  dbg("delete key %s: ret = %s\n", p->key, status.ToString().c_str());
  if (status.ok()) {
  } else {
    err("delete key %s failed\n", p->key);
  }
  p->retcode = SUCCESS;
  if (p->reserved) {
    MultiCompletion* comp = (MultiCompletion*)p->reserved;
    comp->AddFinish();
  }
  return true;
}

