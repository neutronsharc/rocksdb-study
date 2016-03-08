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
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/checkpoint.h"

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


class RocksDBLogger: public rocksdb::Logger {
 public:
  virtual void Logv(const char* format, va_list ap) {
    char logbuf[1024];
    int len = vsnprintf(logbuf, sizeof(logbuf), format, ap);
    if (len < sizeof(logbuf)) {
      printf("%s\n", logbuf);
    }
  }
  virtual void LogHeader(const char* format, va_list ap) {
    Logv(format, ap);
  }
};


class RocksDBShard {
 public:
  RocksDBShard() : db_(nullptr) {}

  ~RocksDBShard() {
    CloseDB();
  }

  bool OpenDBWithRetry(rocksdb::Options& opt,
                       const string& path,
                       rocksdb::DB **db);

  // compact the entire shard.
  void Compact();

  void CompactRange(rocksdb::Slice* begin, rocksdb::Slice* end);

  bool OpenForBulkLoad(const string& path);

  bool OpenDB(const std::string& path, const std::string& wal_path);

  bool OpenDB(const std::string& path);

  bool OpenDB(const string& dbPath,
              int blockCacheMB,
              CompactionStyle cstyle,
              rocksdb::Env* env);

  bool SetAutoCompaction(bool enable);

  void CloseDB();

  rocksdb::Status Put(const char* key, int klen, const char* value, int vlen);

  rocksdb::Status Get(const string& key, string* value);

  // Create a checkpoint in given dir.
  bool CreateCheckpoint(const string& path);

  static void DestroyDB(const string& path) {
    rocksdb::Options opt;
    opt.create_if_missing = true;
    rocksdb::DestroyDB(path, opt);
  }

  uint64_t LatestSequenceNumber() {
    return db_->GetLatestSequenceNumber();
  }

  /////////////////////
  std::shared_ptr<RocksDBLogger> logger_;
  string db_path_;
  string wal_path_;
  bool disable_wal_;
  rocksdb::DB* db_;
  rocksdb::Options options_;
  std::shared_ptr<rocksdb::Statistics> stats_;

  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;

  // a list of ckpt paths, one checkpoint in each path.
  std::vector<string> ckpt_paths_;
  // sequence no. of each ckpt.
  std::vector<uint64_t> sequences_;

  /////////////////////////
  // Obsolete.
  bool Get(KVRequest*  p);

  bool MultiGet(KVRequest* requests, int numRequests);

  bool MultiGet(vector<KVRequest*> &requests);

  bool Put(KVRequest*  p);

  bool Delete(KVRequest*  p);

  uint64_t GetNumberOfRecords();

  uint64_t GetDataSize();

  uint64_t GetMemoryUsage();

};


class RocksDBEngine : public KVStore {
 public:
  RocksDBEngine() : db_(NULL) {}

  // To close a RocksDB interface, just delete the db obj.
  ~RocksDBEngine() {
    for (RocksDBShard* shard : db_shards_) {
      printf("close DB %s\n", shard->db_path_.c_str());
      delete shard;
    }
    if (db_) {
      dbg("close DB at %s\n", db_path_.c_str());
      delete db_;
      db_ = NULL;
    }
  }

  bool CompactRange(rocksdb::Slice* begin, rocksdb::Slice* end) {}

  bool OpenForBulkLoad() {}

  bool OpenDB(const char* path, int numIOThreads, int blockCacheMB);

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

  void set_db_path(string& path) { db_path_ = path; }

  void set_wal_path(string& path) { wal_path_ = path; }

 private:
  vector<string> db_paths_;
  string db_path_;
  string wal_path_;
  rocksdb::Options options_;
  rocksdb::WriteOptions write_options_;
  rocksdb::ReadOptions read_options_;
  rocksdb::DB *db_;

  int num_io_threads_;
  unique_ptr<ThreadPool> thread_pool_;

  // DB shards. Shards of the same DB are in dir "/DB path/<shard-x>/"
  int num_shards_;
  vector<RocksDBShard*> db_shards_;
};


#endif  // __ROCKS_INTERFACE__

