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

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"

#include "debug.h"
#include "hash.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "rocksdb_tuning.h"

using namespace std;

bool RocksDBShard::OpenDBWithRetry(rocksdb::Options& opt,
                                   const string& path,
                                   rocksdb::DB **db) {
  rocksdb::Status status;

  do {
    status = rocksdb::DB::Open(opt, path, db);
    if (status.ok()) {
     return true;
    } else if (status.IsCorruption()) {
      err("db path corrupted: %s\n", path.c_str());
      status = rocksdb::RepairDB(path, opt);
      if (!status.ok()) {
        err("cannot repare db at : %s\n", path.c_str());
        return false;
      }
    } else {
      err("failed to init db %s, error = %s\n",
          path.c_str(), status.ToString().c_str());
      return false;
    }
  } while (1);

}

bool RocksDBShard::OpenForBulkLoad(const string& path) {
  uint64_t MB = 1024L * 1024;

  db_path_ = path;

  // Disable WAL in bulk load mode.
  disable_wal_ = true;

  options_.PrepareForBulkLoad();

  options_.write_buffer_size = 64 * MB;
  options_.max_write_buffer_number = 32;
  options_.min_write_buffer_number_to_merge = 2;
  //options_.allow_os_buffer = false;

  options_.create_if_missing = true;
  options_.create_missing_column_families = true;
  options_.max_open_files = 10000;

  // Block table params.
  rocksdb::BlockBasedTableOptions blk_options;
  blk_options.block_size = 1024L * 8;
  uint64_t blk_cache_size = 128 * MB;
  blk_options.cache_index_and_filter_blocks = true;
  blk_options.block_cache = rocksdb::NewLRUCache(blk_cache_size, 6);
  blk_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blk_options));

  rocksdb::Status status;
  logger_.reset(new RocksDBLogger());
  //options_.Dump(logger_.get());

  // NOTE:: Below code will re-direct all RDB logs to stdout.
  //options_.info_log = logger_;
  //options_.info_log_level = rocksdb::INFO_LEVEL; //INFO_LEVEL;

  return OpenDBWithRetry(options_, db_path_, &db_);
}

bool RocksDBShard::OpenDB(const std::string& path) {
  // User only passes db path, so we disable WAL.
  return OpenDB(path, "");
}

bool RocksDBShard::OpenDB(const std::string& path,
                          const std::string& wal_path) {

  int bkgnd_threads = 12;
  int max_bkgnd_flushes = 2;
  uint64_t MB = 1024L * 1024;
  uint64_t GB = 1024L * 1024 * 1024;

  db_path_ = path;

  options_.IncreaseParallelism(bkgnd_threads);

  options_.OptimizeLevelStyleCompaction();

  // General params.
  options_.create_if_missing = true;
  options_.create_missing_column_families = true;
  options_.max_open_files = 5000;

  // Block table params.
  rocksdb::BlockBasedTableOptions blk_options;
  blk_options.block_size = 1024L * 8;
  uint64_t blk_cache_size = 1000 * MB;
  blk_options.cache_index_and_filter_blocks = true;
  blk_options.block_cache = rocksdb::NewLRUCache(blk_cache_size, 6);
  blk_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));

  options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(blk_options));

  options_.compression = rocksdb::kSnappyCompression;

  // Write buffer, flush, and sync.
  options_.write_buffer_size = 64 * MB;
  options_.max_write_buffer_number = 8;
  options_.min_write_buffer_number_to_merge = 4;
  options_.max_background_flushes = max_bkgnd_flushes;
  options_.disableDataSync = true;
  options_.use_fsync = false;
  options_.allow_mmap_reads = true;
  options_.allow_mmap_writes = false;
  options_.allow_os_buffer = true;

  // WAL log.
  wal_path_ = wal_path;
  if (wal_path_.length() > 0) {
    disable_wal_ = false;
  } else {
    disable_wal_ = true;
  }
  options_.WAL_ttl_seconds = 3600 * 10;
  options_.WAL_size_limit_MB = 100 * GB;

  // Compaction params.
  //options_.disable_auto_compactions = false;
  options_.disable_auto_compactions = true;
  options_.max_background_compactions = bkgnd_threads - max_bkgnd_flushes;
  options_.target_file_size_base = 128 * MB;
  options_.target_file_size_multiplier = 1;
  options_.level0_file_num_compaction_trigger = 2;

  // HIGH background threads for flushes.
  options_.env->SetBackgroundThreads(max_bkgnd_flushes,
                                     rocksdb::Env::Priority::HIGH);
  // LOW background threads for compactions.
  options_.env->SetBackgroundThreads(bkgnd_threads - max_bkgnd_flushes,
                                     rocksdb::Env::Priority::LOW);

  stats_ = rocksdb::CreateDBStatistics();

  logger_.reset(new RocksDBLogger());
  //options_.Dump(logger_.get());

  // NOTE:: Below code will re-direct all RDB logs to stdout.
  //options_.info_log = logger_;
  //options_.info_log_level = rocksdb::INFO_LEVEL; //INFO_LEVEL;

  return OpenDBWithRetry(options_, db_path_, &db_);
}

void RocksDBShard::CloseDB() {
  if (db_) {
    dbg("will close rocksdb at %s\n", db_path_.c_str());
    delete db_;
    db_ = nullptr;
  }
}

bool RocksDBShard::SetAutoCompaction(bool enable) {
  std::unordered_map<std::string, std::string> opt;
  opt["disable_auto_compactions"] = enable ? "false" : "true";

  rocksdb::Status status = db_->SetOptions(db_->DefaultColumnFamily(), opt);
  if (status.ok()) {
    return true;
  } else {
    err("failed to set auto-compaction to %s: %s\n",
        enable ? "on" : "off", status.ToString().c_str());
    return false;
  }
}

void RocksDBShard::Compact() {
  uint64_t t1 = options_.env->NowMicros();
  printf("\bBegin compacting shard %s\n", db_path_.c_str());
  CompactRange(nullptr, nullptr);
  printf("\tFinished compacting shard %s in %.3f seconds\n",
         db_path_.c_str(),
         (options_.env->NowMicros() - t1) / 1000000.0);
}

void RocksDBShard::CompactRange(rocksdb::Slice* begin, rocksdb::Slice* end) {
  if (db_) {
    rocksdb::CompactRangeOptions opt;
    opt.change_level = true;
    db_->CompactRange(opt, begin, end);
  }
}


rocksdb::Status RocksDBShard::Put(const char* key,
                                  int klen,
                                  const char* value,
                                  int vlen) {
  rocksdb::Slice k(key, klen);
  rocksdb::Slice v(value, vlen);

  rocksdb::WriteOptions wopt;
  wopt.disableWAL = disable_wal_;

  rocksdb::Status status = db_->Put(wopt, k, v);
  return status;
}

rocksdb::Status RocksDBShard::Get(const string& key, string* value) {
  rocksdb::ReadOptions ropt;
  ropt.fill_cache = true;
  ropt.verify_checksums = true;

  rocksdb::Slice skey(key.data(), key.size());

  rocksdb::Status status = db_->Get(ropt, skey, value);
  return status;
}


bool RocksDBShard::CreateCheckpoint(const string& path) {
  rocksdb::Checkpoint *ckpt;
  rocksdb::Status status = rocksdb::Checkpoint::Create(db_, &ckpt);
  assert(status.ok());

  uint64_t seq1 = LatestSequenceNumber();
  status = ckpt->CreateCheckpoint(path);
  assert(status.ok());
  uint64_t seq2 = LatestSequenceNumber();

  // get list of files in the checkpoint.
  rocksdb::DB* ckpt_db;
  rocksdb::Options opt;
  status = rocksdb::DB::Open(opt, path, &ckpt_db);
  assert(status.ok());

  uint64_t seq3 = ckpt_db->GetLatestSequenceNumber();
  dbg("created ckpt %s, before-seq = %ld, after-seq = %ld, ckpt seq = %ld\n",
      path.c_str(), seq1, seq2, seq3);

  ckpt_paths_.push_back(path);
  sequences_.push_back(seq3);

  std::vector<string> files;
  uint64_t size;

  status = ckpt_db->GetLiveFiles(files, &size);
  assert(status.ok());
  printf("ckpt %s has %ld files, manifest size %ld:\n",
         path.c_str(), files.size(), size);
  for (auto& s: files) {
    printf("\t%s\n", s.c_str());
  }

  delete ckpt_db;
  delete ckpt;
  return true;
}


/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
/////////////////////////////////////////////////////
//    obsolete
//
bool RocksDBShard::OpenDB(const std::string& dbPath,
                          int blockCacheMB,
                          CompactionStyle cstyle,
                          rocksdb::Env* env) {
  options_.env = env;
  if (cstyle == LEVEL_COMPACTION) {
    TuneLevelStyleCompaction(&options_, blockCacheMB);
  } else {
    TuneUniversalStyleCompaction(&options_, blockCacheMB);
  }

  write_options_.disableWAL = true;

  // save index/filter/data blocks in block cache.
  read_options_.fill_cache = true;
  read_options_.verify_checksums = true;

  rocksdb::Status s = rocksdb::DB::Open(options_, dbPath, &db_);
  if (!s.ok()) {
    printf("Failed to open shard %s: %s\n", dbPath.data(), s.ToString().c_str());
    assert(0);
  }
  printf("Have opened DB %s\n", dbPath.c_str());

  db_path_ = dbPath;
  return true;
}

bool RocksDBShard::Get(KVRequest*  p)  {
  string value;
  rocksdb::Slice key(p->key, p->keylen);
  rocksdb::Status status = db_->Get(read_options_, key, &value);
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
  rocksdb::Status status = db_->Put(write_options_, key, value);
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
  rocksdb::Slice key(p->key, p->keylen);
  rocksdb::Status status = db_->Delete(write_options_, key);
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

  vector<rocksdb::Status> rets = db_->MultiGet(read_options_, keySlices, &values);
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
        dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, values[i].c_str());
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

uint64_t RocksDBShard::GetNumberOfRecords() {
  uint64_t num;
  bool ret = db_->GetIntProperty("rocksdb.estimate-num-keys", &num);
  if (ret) {
    return num;
  } else {
    printf("Failed to get number of keys at shard %s\n", db_path_.c_str());
    return 0;
  }
}

uint64_t RocksDBShard::GetDataSize() {
  rocksdb::Range range(" ", "~");
  uint64_t size;
  db_->GetApproximateSizes(&range, 1, &size);
  return size;
}

uint64_t RocksDBShard::GetMemoryUsage() {
  uint64_t num;
  bool ret = db_->GetIntProperty("rocksdb.estimate-table-readers-mem", &num);
  if (ret) {
    return num;
  } else {
    printf("Failed to get memory usage at shard %s\n", db_path_.c_str());
    return 0;
  }
}



bool RocksDBEngine::Open(const char* dbPath,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB) {
  const char *paths[1] = {dbPath};
  // Default to universal-style compaction.
  return Open(paths, 1, numShards, numIOThreads, blockCacheMB,
              UNIVERSAL_COMPACTION);
}


// Open a multi-shard DB. Also prepare an io-thread pool associated with
// this DB to run rqsts against individual shards.
bool RocksDBEngine::Open(const char* dbPath,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB,
                            CompactionStyle cstyle) {
  const char *paths[1] = {dbPath};
  return Open(paths, 1, numShards, numIOThreads, blockCacheMB, cstyle);
}

bool RocksDBEngine::Open(const char* dbPaths[],
                            int numPaths,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB) {

  return Open(dbPaths, numPaths, numShards, numIOThreads,
              blockCacheMB, UNIVERSAL_COMPACTION);
}

// Open a multi-shard DB that spans multiple directories.
// Also prepare an io-thread pool associated with
// this DB to run rqsts against individual shards.
bool RocksDBEngine::Open(const char* dbPaths[],
                            int numPaths,
                            int numShards,
                            int numIOThreads,
                            int blockCacheMB,
                            CompactionStyle cstyle) {
  rocksdb::Env *env = rocksdb::Env::Default();
  int bgThreadsLow, bgThreadsHigh;
  if (cstyle == UNIVERSAL_COMPACTION) {
    // Universal-compaction uses large number of shards, should restrict number of
    // compaction threads to contain space amplification.
    bgThreadsLow = numShards * 0.6;
    bgThreadsHigh = numShards + 8;
    printf("Choose universal compaction with %d shards, bg-thread-low %d, "
           "bg-thread-high %d\n",
           numShards, bgThreadsLow, bgThreadsHigh);
  } else {
    bgThreadsLow = numShards * 3;
    bgThreadsHigh = numShards + 8;
    printf("Choose level compaction with %d shards, bg-thread-low %d, "
           "bg-thread-high %d\n",
           numShards, bgThreadsLow, bgThreadsHigh);
  }
  env->SetBackgroundThreads(bgThreadsLow, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(bgThreadsHigh, rocksdb::Env::Priority::HIGH);

  int perShardCacheMB = blockCacheMB / numShards;

  // Open the thread pool.
  num_io_threads_ = numIOThreads;
  thread_pool_.reset(new ThreadPool(numIOThreads, this));
  thread_pool_->SetKVStore(this);
  printf("created IO pool with %d threads\n", numIOThreads);

  // Open DB shards.
  printf("Will open DB at %d locations\n", numPaths);
  for (int i = 0; i < numPaths; i++) {
    db_paths_.push_back(dbPaths[i]);
    printf("\t%s\n", dbPaths[i]);
  }

  for (int i = 0; i < numShards; i++) {
    RocksDBShard* shard = new RocksDBShard();
    assert(shard != NULL);

    string shardPath = db_paths_[i % numPaths] + string("/shard-") + std::to_string(i);
    assert(shard->OpenDB(shardPath, perShardCacheMB, cstyle, env));

    db_shards_.push_back(shard);
  }
  num_shards_ = db_shards_.size();
  return true;
}


// Open RocksDB in only 1 shard.
// TODO: delete this method. Obsoleted.
bool RocksDBEngine::OpenDB(const char* dbPath,
                           int numIOThreads,
                           int blockCacheMB) {

  TuneUniversalStyleCompaction(&options_, blockCacheMB);
  write_options_.disableWAL = true;

  // save index/filter/data blocks in block cache.
  read_options_.fill_cache = true;
  read_options_.verify_checksums = true;

  rocksdb::Status s = rocksdb::DB::Open(options_, dbPath, &db_);
  assert(s.ok());
  printf("Have opened DB %s\n", dbPath);

  //db_path_ = dbPath;

  // Open the thread pool.
  num_io_threads_ = numIOThreads;
  thread_pool_.reset(new ThreadPool(numIOThreads, this));
  //thread_pool_->SetDataProcessor(ProcessOneRequest);
  thread_pool_->SetKVStore(this);
  printf("Have created IO thread pool of %d threads\n", numIOThreads);

  return true;
}

// Post a request to worker thread pool.
void RocksDBEngine::PostRequest(QueuedTask* p) {
  thread_pool_->AddWork((void*)p);
}

// Process the given request in sync way.
bool RocksDBEngine::ProcessRequest(void* p) {
  QueuedTask* task = (QueuedTask*)p;

  if (task->type == MULTI_GET) {
    PerShardMultiGet* mget = task->task.mget;
    int shardID = mget->shardID;
    return db_shards_[shardID]->MultiGet(mget->requests);
  }

  // Single request,
  KVRequest* request = (KVRequest*)(task->task.request);
  switch (request->type) {
  case GET:
    return Get(request);

  case PUT:
    return Put(request);

  case DELETE:
    return Delete(request);

  case GET_NUMBER_RECORDS:
    return GetNumberOfRecords(request);

  case GET_DATA_SIZE:
    return GetDataSize(request);

  case GET_MEMORY_USAGE:
    return GetMemoryUsage(request);

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

bool RocksDBEngine::Get(KVRequest*  p)  {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = db_shards_[hv % num_shards_];
  return db->Get(p);

  string value;
  rocksdb::Status status = db_->Get(read_options_, p->key, &value);
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

bool RocksDBEngine::Put(KVRequest*  p)  {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = db_shards_[hv % num_shards_];
  return db->Put(p);

  rocksdb::Slice key(p->key, p->keylen);
  rocksdb::Slice value(p->value, p->vlen);
  rocksdb::Status status = db_->Put(write_options_, key, value);
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

bool RocksDBEngine::Delete(KVRequest*  p) {
  uint32_t hv = bobhash(p->key, p->keylen, 0);
  RocksDBShard* db = db_shards_[hv % num_shards_];
  return db->Delete(p);

  rocksdb::Status status = db_->Delete(write_options_, p->key);
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
bool RocksDBEngine::MultiGet(KVRequest* requests, int numRequests) {
  PerShardMultiGet perShardRqsts[num_shards_];

  int shardsUsed = 0;
  int lastShardUsed = -1;

  // Assign the get requests to DB shards.
  for (int i = 0; i < numRequests; i++) {
    KVRequest* p = requests + i;
    p->reserved = NULL;
    uint32_t hv = bobhash(p->key, p->keylen, 0);
    int shardId = hv % num_shards_;
    if (perShardRqsts[shardId].requests.size() == 0) {
      shardsUsed++;
      lastShardUsed = shardId;;
    }
    perShardRqsts[shardId].requests.push_back(requests + i);
    perShardRqsts[shardId].shardID = shardId;
  }

  // If only one shard is used.
  if (shardsUsed == 1) {
    return db_shards_[lastShardUsed]->MultiGet(
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
  for (int i = 0; i < num_shards_; i++) {
    if (perShardRqsts[i].requests.size() == 0) {
      continue;
    }
    dbg("post mget %ld rqsts to shard %d\n",
        perShardRqsts[i].requests.size(), i);

    tasks[postedShards].type = MULTI_GET;
    tasks[postedShards].task.mget = &perShardRqsts[i];

    PostRequest(tasks + postedShards);
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

  vector<rocksdb::Status> rets = db_->MultiGet(read_options_, keySlices, &values);
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
        dbg("key %s: vlen = %d, value %s\n", p->key, p->vlen, values[i].c_str());
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

bool RocksDBEngine::GetNumberOfRecords(KVRequest* request) {
  uint64_t num = 0;
  for (int i = 0; i < db_shards_.size(); i++) {
    num += db_shards_[i]->GetNumberOfRecords();
  }
  request->vlen = num;
  return true;
}

bool RocksDBEngine::GetDataSize(KVRequest* request) {
  uint64_t num = 0;
  for (int i = 0; i < db_shards_.size(); i++) {
    num += db_shards_[i]->GetDataSize();
  }
  request->vlen = num;
  return true;
}

bool RocksDBEngine::GetMemoryUsage(KVRequest* request) {
  uint64_t num = 0;
  for (int i = 0; i < db_shards_.size(); i++) {
    num += db_shards_[i]->GetMemoryUsage();
  }
  request->vlen = num;
  return true;
}
