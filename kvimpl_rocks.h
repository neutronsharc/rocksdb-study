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

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "threadpool.h"
#include "kvinterface.h"
#include "kvstore.h"

using namespace std;

class RocksDBInterface : public KVStore {
 public:
  RocksDBInterface() : db_(NULL) {}

  // To close a RocksDB interface, just delete the db obj.
  ~RocksDBInterface() {
    if (db_) {
      printf("close DB %s\n", dbPath_.c_str());
      delete db_;
      db_ = NULL;
    }
  }

  bool OpenDB(const char* dbPath, int numIOThreads);

  void PostRequest(KVRequest* p) {
    threadPool_->AddWork(p);
  }

  virtual bool ProcessRequest(KVRequest* p);

  bool Get(KVRequest*  p);

  bool Put(KVRequest*  p);

  bool Delete(KVRequest*  p);

 private:
  string dbPath_;
  rocksdb::DB *db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions writeOptions_;
  rocksdb::ReadOptions readOptions_;

  int numIOThreads_;
  unique_ptr<ThreadPool> threadPool_;

};

#endif  // __ROCKS_INTERFACE__

