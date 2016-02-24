#ifndef __ROCKS_INTERFACE__
#define __ROCKS_INTERFACE__

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
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "kvinterface.h"
#include "kvstore.h"
#include "multi_completion.h"
#include "threadpool.h"

using namespace std;

// types of the tasks queued to the common io thread pool.
enum {
  SINGLE_REQUEST = 0,
  MULTI_GET = 1,
};

// Compaction style.
enum CompactionStyle {
  LEVEL_COMPACTION = 0,
  UNIVERSAL_COMPACTION = 1,
};

struct PerShardMultiGet {
  int shardID;
  vector<KVRequest*> requests;
};

struct QueuedTask {
  // Type of the task: a single rqst, or a multi-get for a specific shard.
  int type;

  union {
    PerShardMultiGet* mget;  // a multi-get targeted at one specific shard.
    KVRequest* request;     // single request
  } task;
};

class RocksDBShard {
 public:
  RocksDBShard() {}

  ~RocksDBShard() {
    if (db_) {
      delete db_;
    }
  }

  bool OpenDB(const string& dbPath,
              int blockCacheMB,
              CompactionStyle cstyle,
              rocksdb::Env* env);

  bool Get(KVRequest*  p);

  bool MultiGet(KVRequest* requests, int numRequests);

  bool MultiGet(vector<KVRequest*> &requests);

  bool Put(KVRequest*  p);

  bool Delete(KVRequest*  p);

  uint64_t GetNumberOfRecords();

  uint64_t GetDataSize();

  uint64_t GetMemoryUsage();

  string dbPath_;
  rocksdb::DB* db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;
};

class RocksDBInterface : public KVStore {
 public:
  RocksDBInterface() : db_(NULL) {}

  // To close a RocksDB interface, just delete the db obj.
  ~RocksDBInterface() {
    for (RocksDBShard* shard : db_shards_) {
      printf("close DB %s\n", shard->dbPath_.c_str());
      delete shard;
    }
    if (db_) {
      printf("close DB at: ");
      for (int i = 0; i < db_paths_.size(); i++) {
        printf("%s ", db_paths_[i].c_str());
      }
      printf("\n");
      delete db_;
      db_ = NULL;
    }
  }

  bool OpenDB(const char* dbPath, int numIOThreads, int blockCacheMB);

  bool Open(const char* dbPath,
            int numShards,
            int numIOThreads,
            int blockCacheMB);

  bool Open(const char* dbPath,
            int numShards,
            int numIOThreads,
            int blockCacheMB,
            CompactionStyle cstyle);

  bool Open(const char* dbPaths[],
            int numPaths,
            int numShards,
            int numIOThreads,
            int blockCacheMB);

  bool Open(const char* dbPaths[],
            int numPaths,
            int numShards,
            int numIOThreads,
            int blockCacheMB,
            CompactionStyle cstyle);

  void PostRequest(QueuedTask* p);

  virtual bool ProcessRequest(void* p);

  bool Get(KVRequest*  p);

  bool MultiGet(KVRequest* requests, int numRequests);

  bool Put(KVRequest*  p);

  bool Delete(KVRequest*  p);

  bool GetNumberOfRecords(KVRequest* p);

  bool GetDataSize(KVRequest* p);

  bool GetMemoryUsage(KVRequest* p);

  vector<string> db_paths_;
  rocksdb::DB *db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;

  int num_io_threads_;
  unique_ptr<ThreadPool> thread_pool_;

  // DB shards. Shards of the same DB are in dir "/DB path/<shard-x>/"
  int num_shards_;
  vector<RocksDBShard*> db_shards_;
};


#endif  // __ROCKS_INTERFACE__

