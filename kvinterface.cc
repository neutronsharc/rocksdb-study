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

using namespace std;

class RocksDBInterface {
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

  static void ProcessOneRequest(void* p) {
    KVRequest* request = (KVRequest*)p;
    DumpKVRequest(request);
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

  bool OpenDB(char* dbPath, int numIOThreads) {
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

    // Open the thread pool.
    numIOThreads_ = numIOThreads;
    threadPool_.reset(new ThreadPool(numIOThreads));
    threadPool_->SetDataProcessor(ProcessOneRequest);
    printf("Have created IO thread pool of %d threads\n", numIOThreads);

    rocksdb::Status s = rocksdb::DB::Open(options_, dbPath, &db_);
    assert(s.ok());
    dbPath_ = dbPath;
    printf("Have opened DB %s\n", dbPath);
  }

  int Process(KVRequest* request, int numRequests) {
    for (int i = 0; i < numRequests; i++) {
      threadPool_->AddTask((void*)request);
      request++;
    }
    printf("have posted %d rqsts\n", numRequests);
  }

 private:
  string dbPath_;
  rocksdb::DB *db_;
  rocksdb::Options options_;
  rocksdb::WriteOptions writeOptions_;
  rocksdb::ReadOptions readOptions_;

  int numIOThreads_;
  unique_ptr<ThreadPool> threadPool_;

};
