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

using namespace std;

struct WorkerTask {
  // worker thread ID
  int id;

  bool doRead;
  bool doWrite;

  // Number of r/w to perform
  unsigned long numWrites;
  unsigned long numReads;

  // number of failures.
  unsigned long writeFailures;
  unsigned long readFailures;

  // ratio of write ops, 0.0 ~ 1.0. Main thread will set this value.
  double writeRatio;

  // record operation latency in micro-sec.
  int *writeLatency;
  int *readLatency;

  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;

  rocksdb::DB *db;
  rocksdb::WriteOptions writeOptions;
  rocksdb::ReadOptions readOptions;

  // A lock to sync output.
  mutex *outputLock;
};

string kDBPath = "/ssd/test/rocks";

unsigned long time_microsec() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

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
  return std::rand() % max_val;
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

void Worker(WorkerTask *task) {

  char key[128];
  char charvalue[1024];
  int objSize = 1023;

  rocksdb::Status status;
  unsigned long tBeginUs, tEndUs, t1, t2;

  printf("worker %d waiting...\n", task->id);
  sem_wait(&task->sem_begin);
  printf("worker %d started...\n", task->id);

  long elapsedMicroSec, elapsedMilliSec;

  if (task->doWrite) {
    printf("worker %d will do %d writes...\n", task->id, task->numWrites);
    tBeginUs = time_microsec();
    for (int i = 0; i < task->numWrites; i++) {
      sprintf(key, "task-%d-key-%d", task->id, i);
      sprintf(charvalue, "task-%d-value-%d", task->id, i);
      memset(charvalue + strlen(charvalue), 'A', 1024 - strlen(charvalue));
      charvalue[1023] = 0;

      t1 = time_microsec();
      status = task->db->Put(task->writeOptions, key, charvalue);
      t2 = time_microsec();
      task->writeLatency[i] = t2 - t1;
      assert(status.ok());
      if ((i + 1) % 100000 == 0) {
        printf("task %d: write %d \n", task->id, i + 1);
      }
    }
    tEndUs = time_microsec();
    printf("task %d finished write ...\n", task->id);
    elapsedMicroSec = tEndUs - tBeginUs;
    elapsedMilliSec = elapsedMicroSec / 1000;
    task->outputLock->lock();
    cout << "thread " << task->id << " has written " << task->numWrites << " objs" << " in "
         << elapsedMilliSec / 1000.0 << " seconds, "
         << "data = " << (objSize * task->numWrites / 1024.0 / 1024) << " MB, "
         << "IOPS = " << task->numWrites / (elapsedMilliSec / 1000.0) << endl;
    cout << "avg write lat (ms) = " << (double)elapsedMilliSec / task->numWrites << endl;
    task->outputLock->unlock();
  }

  sem_post(&task->sem_end);

  sem_wait(&task->sem_begin);
  printf("worker %d started read...\n", task->id);

  if (task->doRead) {
    printf("worker %d will do %d reads ...\n", task->id, task->numReads);
    tBeginUs = time_microsec();
    for (int i = 0; i < task->numReads; i++) {
      unsigned long objID = get_random(task->numWrites);
      sprintf(key, "task-%d-key-%d", task->id, (int)(objID));

      string value;
      t1 = time_microsec();
      status = task->db->Get(task->readOptions, key, &value);
      t2 = time_microsec();
      task->readLatency[i] = t2 - t1;
      assert(status.ok());
      assert(value.length() == 1023);
      int rid, rval;
      sscanf(value.c_str(), "task-%d-value-%d", &rid, &rval);
      assert(rid == task->id);
      assert(rval == (int)objID);
      if ((i + 1) % 100000 == 0) {
        printf("task %d: read %d \n", task->id, i + 1);
      }
    }
    tEndUs = time_microsec();
    elapsedMicroSec = tEndUs - tBeginUs;
    elapsedMilliSec = elapsedMicroSec / 1000;
    task->outputLock->lock();
    cout << "thread " << task->id << " has read " << task->numReads << " objs" << " in "
         << elapsedMilliSec / 1000.0 << " seconds, "
         << "data = " << (objSize * task->numReads/ 1024.0 / 1024) << " MB, "
         << "IOPS = " << task->numReads / (elapsedMilliSec / 1000.0) << endl;
    cout << "avg read lat (ms) = " << (double)elapsedMilliSec / task->numReads << endl;
    task->outputLock->unlock();
  }

  sem_post(&task->sem_end); printf("task %d finished read...\n", task->id);

}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf("usage: %s [do write 1/0] [do read 1/0] [num of threads]\n");
    return -1;
  }

  bool doWrite = atoi(argv[1]) == 0 ? false : true;
  bool doRead = atoi(argv[2]) == 0 ? false : true;
  int numTasks = atoi(argv[3]);

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
  unsigned long block_cache_mb = 256;
  options.OptimizeForPointLookup(block_cache_mb);

  // create the DB if it's not already present
  options.create_if_missing = true;
  options.max_open_files = 4096;
  options.allow_os_buffer = false;
  options.write_buffer_size = 1024L * 1024 * 4;
  options.max_write_buffer_number = 200;
  options.min_write_buffer_number_to_merge = 2;
  options.compression = rocksdb::kNoCompression;

  rocksdb::WriteOptions writeOptions;
  writeOptions.disableWAL = true;

  rocksdb::ReadOptions readOptions;
  readOptions.verify_checksums = true;
  // save index/filter/data blocks in block cache.
  readOptions.fill_cache = true;

  std::thread  *workers = new std::thread[numTasks];
  mutex outputLock;

  // open DB
  cout << "will run " << numTasks << " threads on DB " << kDBPath << endl;
  rocksdb::DB* db;
  rocksdb::Status s = rocksdb::DB::Open(options, kDBPath, &db);
  assert(s.ok());

  struct timespec tbegin, tend;
  struct timespec objBegin, objEnd;
  long elapsedMicroSec = 0;
  long elapsedMilliSec = 0;

  // Init random number.
  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  std::srand(tbegin.tv_nsec);

  // Put key-value
  //s = db->Put(WriteOptions(), "key", "value");
  //assert(s.ok());

  // seq write objs.
  int numObjs = 1000 * 1000 * 60;
  int writeObjCount = numObjs;
  int readObjCount = numObjs;

  int *writeLatency = new int[numObjs];
  int *readLatency = new int[numObjs];
  memset(writeLatency, 0, numObjs * sizeof(int));
  memset(readLatency, 0, numObjs * sizeof(int));

  int perTaskWrite = writeObjCount / numTasks;
  int perTaskRead = readObjCount / numTasks;
  WorkerTask *tasks = new WorkerTask[numTasks];

  for (int i = 0; i < numTasks; i++) {
    tasks[i].id = i;
    tasks[i].doRead = doRead;
    tasks[i].doWrite = doWrite;
    tasks[i].numWrites = perTaskWrite;
    tasks[i].numReads = perTaskRead;
    tasks[i].writeLatency = writeLatency + perTaskWrite * i;
    tasks[i].readLatency = readLatency + perTaskRead* i;

    sem_init(&tasks[i].sem_begin, 0, 0);
    sem_init(&tasks[i].sem_end, 0, 0);

    tasks[i].db = db;
    tasks[i].writeOptions = writeOptions;
    tasks[i].readOptions = readOptions;

    tasks[i].outputLock = &outputLock;

    workers[i] = std::thread(Worker, tasks + i);
  }

  printf("Main: will start write phase...\n");
  unsigned long t1, t2, timeTotal;
  t1 = time_microsec();
  // start worker to write.
  for (int i = 0; i < numTasks; i++) {
    sem_post(&tasks[i].sem_begin);
  }
  // wait for write to finish.
  for (int i = 0; i < numTasks; i++) {
    sem_wait(&tasks[i].sem_end);
  }
  timeTotal = time_microsec() - t1;
  // output stats

  if (doWrite) {
    sort(writeLatency, writeLatency + writeObjCount);
    printf("Overall write IOPS = %f\n", writeObjCount / (timeTotal / 1000000.0));
    PrintStats(writeLatency, writeObjCount, "\nOverall write latency in ms");
    sleep(10);
  }

  // start worker to read.
  printf("Main: will start read phase...\n");
  t1 = time_microsec();
  for (int i = 0; i < numTasks; i++) {
    sem_post(&tasks[i].sem_begin);
  }
  // wait for read to finish.
  for (int i = 0; i < numTasks; i++) {
    sem_wait(&tasks[i].sem_end);
  }
  timeTotal = time_microsec() - t1;
  for (int i = 0; i < numTasks; i++) {
    if (workers[i].joinable()) {
      workers[i].join();
      printf("joined thread %d\n", i);
    }
  }
  // output stats
  if (doRead) {
    sort(readLatency, readLatency + readObjCount);
    printf("Overall read IOPS = %f\n", readObjCount / (timeTotal / 1000000.0));
    PrintStats(readLatency, readObjCount, "\nOverall read latency in ms");
  }

  delete [] workers;
  delete [] readLatency;
  delete [] writeLatency;
  delete db;
  return 0;


  char key[100];
  char charvalue[1024];

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
    writeLatency[i] = get_diff_microsec(&objBegin, &objEnd);

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

  sort(writeLatency, writeLatency + writeObjCount);
  PrintStats(writeLatency, writeObjCount, "Put latency in ms");


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
    readLatency[i] = get_diff_microsec(&objBegin, &objEnd);

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

  sort(readLatency, readLatency + readObjCount);
  PrintStats(readLatency, readObjCount, "Get latency in ms");

  delete [] workers;
  delete [] readLatency;
  delete [] writeLatency;
  delete db;
  return 0;
}
