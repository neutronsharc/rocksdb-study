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

  string dbPath_;
  rocksdb::DB* db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions writeOptions_;
  rocksdb::ReadOptions readOptions_;
};

class RocksDBInterface : public KVStore {
 public:
  RocksDBInterface() : db_(NULL) {}

  // To close a RocksDB interface, just delete the db obj.
  ~RocksDBInterface() {
    for (RocksDBShard* shard : dbShards_) {
      printf("close DB %s\n", shard->dbPath_.c_str());
      delete shard;
    }
    if (db_) {
      printf("close DB at: ");
      for (int i = 0; i < dbPaths_.size(); i++) {
        printf("%s ", dbPaths_[i].c_str());
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

  void PostRequest(void* p);

  virtual bool ProcessRequest(void* p);

  bool Get(KVRequest*  p);

  bool MultiGet(KVRequest* requests, int numRequests);

  bool Put(KVRequest*  p);

  bool Delete(KVRequest*  p);

  vector<string> dbPaths_;
  rocksdb::DB *db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions writeOptions_;
  rocksdb::ReadOptions readOptions_;

  int numIOThreads_;
  unique_ptr<ThreadPool> threadPool_;

  // DB shards. Shards of the same DB are in dir "/DB path/<shard-x>/"
  int numberOfShards_;
  vector<RocksDBShard*> dbShards_;
};


#endif  // __ROCKS_INTERFACE__

