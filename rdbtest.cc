// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#include <time.h>
#include <semaphore.h>
#include <pthread.h>

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <iomanip>
#include <string>


#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using namespace std;
using namespace rocksdb;

struct WorkerOptions {
  // worker thread ID
  int id;

  // Number of r/w to perform
  unsigned long numWrites;
  unsigned long numReads;

  // number of failures.
  unsigned long writeFailures;
  unsigned long readFailures;

  // ratio of write ops, 0.0 ~ 1.0. Main thread will set this value.
  double writeRatio;

  // record operation latency in milli-sec.
  int *writeLatency;
  int *readLatency;

  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;
};

std::string kDBPath = "/mnt/ssd/mycc";

long to_microsec(struct timespec *ts) {
  return ts->tv_sec * 1000000 + ts->tv_nsec / 1000;
}

long get_diff_microsec(struct timespec *begin, struct timespec *end) {
  return to_microsec(end) - to_microsec(begin);
}

long to_millisec(struct timespec *ts) {
  return ts->tv_sec * 1000 + ts->tv_nsec / 1000000;
}

long get_diff_millisec(struct timespec *begin, struct timespec *end) {
  return to_millisec(end) - to_millisec(begin);
}

unsigned long get_random(unsigned long max_val) {
  long v = lrand48();
  return v % max_val;
}

void PrintStats(int *latency, int size, const char *header) {
  int lat_50 = latency[(int)(size * 0.5)];
  int lat_90 = latency[(int)(size * 0.9)];
  int lat_95 = latency[(int)(size * 0.95)];
  int lat_99 = latency[(int)(size * 0.99)];
  int lat_999 = latency[(int)(size * 0.999)];
  int lat_max = latency[size - 1];
  cout << header << endl;
  cout << setw(15) << "50 %"
       << setw(15) << "90 %"
       << setw(15) << "95 %"
       << setw(15) << "99 %"
       << setw(15) << "99.9 %"
       << setw(15) << "max" << endl;
  cout << setw(15) << lat_50 / 1000.0
       << setw(15) << lat_90 / 1000.0
       << setw(15) << lat_95 / 1000.0
       << setw(15) << lat_99 / 1000.0
       << setw(15) << lat_999 / 1000.0
       << setw(15) << lat_max / 1000.0 << endl;
}

int main() {

  // Prepare general DB options.
  rocksdb::Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options.OptimizeLevelStyleCompaction();
  //options.OptimizeUniversalStyleCompaction();

  // point lookup: will create hash index, 10-bits bloom filter,
  // a block-cache of this size in MB, 4KB block size,
  unsigned long block_cache_mb = 64;
  options.OptimizeForPointLookup(block_cache_mb);

  // create the DB if it's not already present
  options.create_if_missing = true;
  options.max_open_files = 4096;
  options.allow_os_buffer = false;
  options.write_buffer_size = 1024L * 1024 * 4;
  options.max_write_buffer_number = 200;
  options.min_write_buffer_number_to_merge = 2;
  options.compression = kNoCompression;

  rocksdb::WriteOptions writeOptions;
  writeOptions.disableWAL = true;

  rocksdb::ReadOptions readOptions;
  readOptions.verify_checksums = true;
  readOptions.fill_cache = true;

  // open DB
  cout << "will open DB " << kDBPath << endl;
  rocksdb::DB* db;
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  struct timespec tbegin, tend;
  struct timespec objBegin, objEnd;
  long elapsedMicroSec = 0;
  long elapsedMilliSec = 0;

  // Init random number.
  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  srand48(tbegin.tv_nsec);

  // Put key-value
  //s = db->Put(WriteOptions(), "key", "value");
  //assert(s.ok());

  // seq write objs.
  int numObjs = 1000 * 1000 * 6;
  int writeObjCount = numObjs;
  int readObjCount = numObjs / 6;

  char key[100];
  char charvalue[1024];

  int *putLatency = (int*)malloc(numObjs * sizeof(int));
  int *getLatency = (int*)malloc(numObjs * sizeof(int));
  memset(putLatency, 0, writeObjCount * sizeof(int));
  memset(getLatency, 0, readObjCount* sizeof(int));
  long objSize = 1023;


  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  for (int i = 0; i < writeObjCount; i++) {
    sprintf(key, "key-%d", i);
    sprintf(charvalue, "value-%d", i);
    memset(charvalue + strlen(charvalue), 'A', 1024 - strlen(charvalue));
    charvalue[1023] = 0;

    clock_gettime(CLOCK_MONOTONIC, &objBegin);
    //s = db->Put(WriteOptions(), key, charvalue);
    s = db->Put(writeOptions, key, charvalue);
    clock_gettime(CLOCK_MONOTONIC, &objEnd);
    putLatency[i] = get_diff_microsec(&objBegin, &objEnd);

    assert(s.ok());
    if ((i + 1) % 100000 == 0) {
      cout << "put " << i + 1 << endl;
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &tend);

  elapsedMicroSec = get_diff_microsec(&tbegin, &tend);
  elapsedMilliSec = get_diff_millisec(&tbegin, &tend);
  cout << "has put " << writeObjCount << " objs" << " in "
       << elapsedMilliSec / 1000.0 << " seconds, "
       << "data = " << (objSize * writeObjCount / 1024.0 / 1024) << " MB, "
       << "IOPS = " << writeObjCount / (elapsedMilliSec / 1000.0) << endl;
  cout << "avg put lat (ms) = " << (double)elapsedMilliSec / writeObjCount << endl;

  sort(putLatency, putLatency + writeObjCount);
  PrintStats(putLatency, writeObjCount, "Put latency in ms");


  //std::string value;
  // get value
  //s = db->Get(ReadOptions(), "key", &value);
  //assert(s.ok());
  //assert(value == "value");

  // seq read objs.
  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  for (int i = 0; i < readObjCount; i++) {
    std::string value;
    unsigned long objID = get_random(writeObjCount);
    sprintf(key, "key-%d", (int)(objID));

    clock_gettime(CLOCK_MONOTONIC, &objBegin);
    //s = db->Get(ReadOptions(), key, &value);
    s = db->Get(readOptions, key, &value);
    clock_gettime(CLOCK_MONOTONIC, &objEnd);
    getLatency[i] = get_diff_microsec(&objBegin, &objEnd);

    assert(s.ok());
    assert(value.length() == 1023);
    int rval;
    sscanf(value.c_str(), "value-%d", &rval);
    assert(rval == (int)objID);
    if ((i + 1) % 100000 == 0) {
      cout << "get " << i + 1 << endl;
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &tend);

  elapsedMicroSec = get_diff_microsec(&tbegin, &tend);
  elapsedMilliSec = get_diff_millisec(&tbegin, &tend);
  cout << "has get " << readObjCount << " objs" << " in "
       << elapsedMilliSec / 1000.0 << " seconds, "
       << "IOPS = " << readObjCount / (elapsedMilliSec / 1000.0) << endl;
  cout << "avg get lat (ms) = " << (double)elapsedMilliSec / readObjCount << endl;

  sort(getLatency, getLatency + readObjCount);
  PrintStats(getLatency, readObjCount, "Get latency in ms");

  free(getLatency);
  free(putLatency);

  delete db;
  return 0;
}
