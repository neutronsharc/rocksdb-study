#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <bsd/stdlib.h>

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

#include "threadpool.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "rocksdb_tuning.h"
#include "utils.h"
#include "hdr_histogram.h"

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

  // Target qps issued by this worker.
  unsigned long targetQPS;

  // Target qps for write ops by this worker.
  unsigned long writeTargetQPS;

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

  RocksDBInterface* dbIface;

  // A lock to sync output.
  mutex *outputLock;
};


string dbPath;
bool doWrite = false;
bool doRead = false;
int numThreads = 1;
int objSize = 1000;
long numObjs = 1000L;
long numOps = 1000L;
long dbCacheMB = 5000L;
long totalTargetQPS = 1000000L;
long totalWriteTargetQPS = 1000000L;
int numKeysPerRead = 1;
int numShards = 8;

vector<rocksdb::ColumnFamilyHandle*> familyHandles;

uint64_t GetMemoryUsage(RocksDBInterface *dbIface);

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
  int lat_min = latency[0];
  int lat_10 = latency[(int)(size * 0.1)];
  int lat_20 = latency[(int)(size * 0.2)];
  int lat_50 = latency[(int)(size * 0.5)];
  int lat_90 = latency[(int)(size * 0.9)];
  int lat_95 = latency[(int)(size * 0.95)];
  int lat_99 = latency[(int)(size * 0.99)];
  int lat_999 = latency[(int)(size * 0.999)];
  int lat_max = latency[size - 1];
  cout << header << endl;
  cout << setw(15) << "min"
       << setw(15) << "10 %"
       << setw(15) << "20 %"
       << setw(15) << "50 %"
       << setw(15) << "90 %"
       << setw(15) << "95 %"
       << setw(15) << "99 %"
       << setw(15) << "99.9 %"
       << setw(15) << "max" << endl;
  cout << setw(15) << lat_min / 1000.0
       << setw(15) << lat_10 / 1000.0
       << setw(15) << lat_20 / 1000.0
       << setw(15) << lat_50 / 1000.0
       << setw(15) << lat_90 / 1000.0
       << setw(15) << lat_95 / 1000.0
       << setw(15) << lat_99 / 1000.0
       << setw(15) << lat_999 / 1000.0
       << setw(15) << lat_max / 1000.0 << endl;
}

void Worker(WorkerTask *task) {

  char key[128];
  char charvalue[10002];
  assert(objSize <= 10000);

  rocksdb::Status status;
  unsigned long tBeginUs, tEndUs, t1, t2;

  long elapsedMicroSec;

  if (task->doWrite) {
    printf("worker %d will do %d writes...\n", task->id, task->numWrites);
    sem_wait(&task->sem_begin);

    tBeginUs = time_microsec();
    for (int i = 0; i < task->numWrites; i++) {
      sprintf(key, "task-%d-key-%d", task->id, i);
      arc4random_buf(charvalue, objSize);
      sprintf(charvalue, "valueof-task-%d-key-%d", task->id, i);
      charvalue[strlen(charvalue)] = ' ';
      // memcached protocol requires '\r\n' at end of value.
      charvalue[objSize] = '\r';
      charvalue[objSize + 1] = '\n';
      //rocksdb::Slice keyslice(key, strlen(key));
      //rocksdb::Slice valueslice(charvalue, objSize + 2);
      KVRequest rqst;
      memset(&rqst, 0, sizeof(rqst));
      rqst.type = PUT;
      rqst.key = key;
      rqst.keylen = strlen(key);
      rqst.value = charvalue;
      rqst.vlen = objSize + 2;

      t1 = time_microsec();
      //status = task->db->Put(task->writeOptions, keyslice, valueslice);
      assert(task->dbIface->Put(&rqst) == true);
      t2 = time_microsec();
      task->writeLatency[i] = t2 - t1;
      //assert(status.ok());
      if ((i + 1) % 500000 == 0) {
        printf("task %d: write %d \n", task->id, i + 1);
      }
      // Throttle to target QPS.
      unsigned long actualSpentTime = time_microsec() - tBeginUs;
      unsigned long targetSpentTime =
        (unsigned long)((i + 1.0) * 1000000 / task->writeTargetQPS);
      if (actualSpentTime < targetSpentTime) {
        usleep(targetSpentTime - actualSpentTime);
      }
    }
    tEndUs = time_microsec();
    printf("task %d finished write ...\n", task->id);
    elapsedMicroSec = tEndUs - tBeginUs;
    task->outputLock->lock();
    cout << "thread " << task->id << " has written " << task->numWrites << " objs" << " in "
         << elapsedMicroSec / 1000000.0 << " seconds, "
         << "data = " << (objSize * task->numWrites / 1024.0 / 1024) << " MB, "
         << "IOPS = " << task->numWrites * 1000000.0 / elapsedMicroSec << endl;
    task->outputLock->unlock();

    sem_post(&task->sem_end);
  }

  char keys[100][200];

  if (task->doRead) {
    sem_wait(&task->sem_begin);
    int warmups = 100000;
    // Warm up read.
    printf("worker %d will do %d warm-up reads ...\n", task->id, warmups);
    for (int i = 0; i < warmups; i++) {
      unsigned long objID = get_random(task->numWrites);
      sprintf(key, "task-%d-key-%d", task->id, (int)(objID));
      KVRequest rqst;
      memset(&rqst, 0, sizeof(rqst));
      rqst.type = GET;
      rqst.key = key;
      rqst.keylen = strlen(key);
      assert(task->dbIface->Get(&rqst) == true);
      if (rqst.retcode != SUCCESS) {
        printf("cannot find key %s\n", rqst.key);
      } else {
        free(rqst.value);
      }
    }
    sem_post(&task->sem_end);
  }

  if (task->doRead) {
    sem_wait(&task->sem_begin);
    printf("worker %d will do %d reads ...\n", task->id, task->numReads);
    tBeginUs = time_microsec();
    for (int i = 0; i < task->numReads; i++) {
      if (numKeysPerRead > 1) {
        KVRequest rqsts[numKeysPerRead];
        memset(rqsts, 0, sizeof(KVRequest) * numKeysPerRead);
        //vector<rocksdb::Slice> keySlices;
        //vector<string> values;
        t1 = time_microsec();
        for (int k = 0; k < numKeysPerRead; k++) {
          unsigned long objID = get_random(task->numWrites);
          sprintf(keys[k], "task-%d-key-%d", task->id, (int)(objID));
          //keySlices.push_back(
          //  rocksdb::Slice(keys[k], strlen(keys[k])));
          rqsts[k].type = GET;
          rqsts[k].key = keys[k];
          rqsts[k].keylen = strlen(keys[k]);
          rqsts[k].reserved = NULL;
        }
        //vector<rocksdb::Status> rets = task->db->MultiGet(task->readOptions,
        //                                                  keySlices,
        //                                                  &values);
        assert(task->dbIface->MultiGet(rqsts, numKeysPerRead));
        t2 = time_microsec();
        task->readLatency[i] = t2 - t1;
        for (int k = 0; k < numKeysPerRead; k++) {
          //if (!rets[k].ok()) {
          //  printf("failed to get key %s: ret = %s\n", keys[k],
          //      rets[k].ToString().c_str());
          //  continue;
          //}
          //assert(values[k].length() == objSize + 2);
          if (rqsts[k].retcode != SUCCESS) {
            printf("failed to get key %s\n", rqsts[k].key);
            continue;
          }
          assert(rqsts[k].vlen == objSize + 2);
          int tidAtKey, vidAtKey, tidAtValue, vidAtValue;
          sscanf(rqsts[k].key, "task-%d-key-%d", &tidAtKey, &vidAtKey);
          sscanf(rqsts[k].value, "valueof-task-%d-key-%d", &tidAtValue, &vidAtValue);
          assert(tidAtKey == task->id);
          assert(tidAtKey == tidAtValue);
          assert(vidAtKey == vidAtValue);
          free(rqsts[k].value);
        }
      } else {
        unsigned long objID = get_random(task->numWrites);
        sprintf(key, "task-%d-key-%d", task->id, (int)(objID));
        KVRequest rqst;
        memset(&rqst, 0, sizeof(rqst));
        rqst.key = key;
        rqst.keylen = strlen(key);
        rqst.type = GET;
        rqst.reserved = NULL;

        //string value;
        t1 = time_microsec();
        //status = task->db->Get(task->readOptions, key, &value);
        assert(task->dbIface->Get(&rqst) == true);
        t2 = time_microsec();
        task->readLatency[i] = t2 - t1;
        //assert(status.ok());
        //assert(value.length() == objSize || value.length() == objSize + 2);
        if (rqst.retcode != SUCCESS) {
          printf("failed to get key %s\n", rqst.key);
        } else {
          assert(rqst.vlen == objSize || rqst.vlen == objSize + 2);
          int rid, rval;
          //sscanf(value.c_str(), "task-%d-value-%d", &rid, &rval);
          sscanf(rqst.value, "valueof-task-%d-key-%d", &rid, &rval);
          assert(rid == task->id);
          assert(rval == (int)objID);
          free(rqst.value);
        }
      }
      if ((i + 1) % 500000 == 0) {
        printf("task %d: read %d, see cache-table mem usage: %ld\n",
               task->id, i + 1, GetMemoryUsage(task->dbIface));
      }
      // Throttle to target QPS.
      unsigned long actualSpentTime = time_microsec() - tBeginUs;
      unsigned long targetSpentTime =
        (unsigned long)((i + 1.0) * 1000000 / task->targetQPS);
      if (actualSpentTime < targetSpentTime) {
        usleep(targetSpentTime - actualSpentTime);
      }
    }
    tEndUs = time_microsec();
    elapsedMicroSec = tEndUs - tBeginUs;
    task->outputLock->lock();
    cout << "thread " << task->id << " has read "
         << task->numReads * numKeysPerRead << " objs in "
         << elapsedMicroSec / 1000000.0 << " seconds, "
         << "data = "
         << (objSize * task->numReads * numKeysPerRead / 1024.0 / 1024)
         << " MB, "
         << "IOPS = " << task->numReads * 1000000.0 / elapsedMicroSec << endl;
    task->outputLock->unlock();
    sem_post(&task->sem_end);
  }

  printf("task %d finished...\n", task->id);
}

void TryKVInterface(string &dbpath, int numThreads, int cacheMB) {
  void* hdl = OpenDB(dbpath.c_str(), numThreads, cacheMB);

  char key1[128], key2[128];
  char charvalue[1024];
  KVRequest write[2];

  sprintf(key1, "key-1");
  write[0].key = key1;
  write[0].keylen = 5;;
  write[0].type = PUT;
  sprintf(charvalue, "value-1");
  write[0].value = charvalue;
  write[0].vlen = strlen(charvalue);

  sprintf(key2, "key-2");
  write[1].key = key2;
  write[1].keylen = 5;;
  write[1].type = PUT;
  sprintf(charvalue + 64, "value-2");
  write[1].value = charvalue + 64;
  write[1].vlen = strlen(charvalue + 64);

  assert(KVRunCommand(hdl, write, 2) == 2);

  CloseDB(hdl);
}

void TryRocksDB(string &dbpath, int numThreads, int cacheMB) {
  RocksDBInterface rdb;
  assert(rdb.OpenDB(dbpath.c_str(), numThreads, cacheMB) == true);

  char key[128];
  char charvalue[1024];
  int objSize = 1023;

  KVRequest write;
  write.type = PUT;

  sprintf(key, "key-1");
  write.key = key;
  write.keylen = strlen(key);

  sprintf(charvalue, "value-%d", 10);
  memset(charvalue + strlen(charvalue), 'A', 1024 - strlen(charvalue));
  charvalue[10] = 0;
  charvalue[1022] = '\r';
  charvalue[1023] = '\n';
  write.value = charvalue;
  write.vlen = 1023; //strlen(charvalue);

  //rdb.PostRequest(&write);
  rdb.ProcessRequest((void*)&write);


  KVRequest read;
  read.type = GET;
  read.key = key;
  read.keylen = strlen(key);
  rdb.ProcessRequest((void*)&read);
  printf("get value = %s\n", read.value);
  assert(read.vlen == write.vlen);
  assert(memcmp(charvalue, read.value, read.vlen) == 0);
  free(read.value);
}

void TryThreadPool() {
  ThreadPool *pool = new ThreadPool(2, NULL);
  pool->AddWork((void*)1);
  pool->AddWork((void*)2);
  pool->AddWork((void*)3);
  pool->AddWork((void*)4);
  pool->AddWork((void*)5);
  delete pool;
}

void help() {
  printf("Test RocksDB raw performance, mixed r/w ratio: \n");
  printf("parameters: \n");
  printf("-p <dbpath>          : rocksdb paths separated by ','. Must provide.\n");
  printf("-w                   : re-write entire DB before test. Def to not\n");
  printf("-r                   : perform read benchmark after writing. Def to not\n");
  printf("-s <obj size>        : object size in bytes. Def = 1000\n");
  printf("-S <shards>          : number of shards. Def = 8\n");
  printf("-n <num of objs>     : total number of objs. Def = 1000\n");
  printf("-o <num of reads>    : total number of read ops. Def = 1000\n");
  printf("-t <num of threads>  : number of threads to run. Def = 1\n");
  printf("-c <DB cache>        : DB cache in MB. Def = 5000\n");
  printf("-q <QPS>             : Total read target QPS by all threads. \n"
         "                       Def = 1000000 op/sec\n");
  printf("-e <write QPS>       : Total write target QPS by all threads. \n"
         "                       Def = 1000000 op/sec\n");
  printf("-k <multiget keys>   : multi-get these number of keys in one get.\n"
         "                       def = 1 key\n");
  printf("-x <key>             : write this key with random value of given size\n");
  printf("-y <key>             : read this key from DB\n");
  printf("-h                   : this message\n");
}

void Write(string key, RocksDBInterface *dbIface) {
  char buf[objSize + 1];
  arc4random_buf(buf, objSize);
  sprintf(buf, "%s", key.c_str());
  buf[strlen(buf)] = ' ';
  buf[objSize] = 0;
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.key = key.data();
  rqst.keylen = key.size();
  rqst.value = buf;
  rqst.vlen = objSize;

  assert(dbIface->Put(&rqst) == true);
  printf("write key: %s, vlen = %ld, value = %s\n",
         rqst.key, rqst.vlen, rqst.value);
}

void Read(string key, RocksDBInterface *dbIface) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.key = key.data();
  rqst.keylen = key.size();

  assert(dbIface->Get(&rqst) == true);
  if (rqst.retcode != SUCCESS) {
    printf("cannot find key %s\n", rqst.key);
  } else {
    printf("key: %s, vlen=%ld, value: %s\n",
           rqst.key, rqst.vlen, rqst.value);
    free(rqst.value);
  }
}

uint64_t GetMemoryUsage(RocksDBInterface *dbIface) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_MEMORY_USAGE;
  dbIface->GetMemoryUsage(&rqst);
  return rqst.vlen;
}

int main(int argc, char** argv) {
  if (argc == 1) {
    help();
    return 0;
  }

  int c;
  vector<char*> dbPaths;
  bool singleRead = false, singleWrite = false;
  string singleReadKey, singleWriteKey;

  while ((c = getopt(argc, argv, "e:p:wrhs:n:t:c:q:k:o:S:x:y:")) != EOF) {
    switch(c) {
      case 'h':
        help();
        return 0;
      case 'p':
        dbPath = optarg;
        dbPaths = SplitString(optarg, ",");
        printf("db path : %s\n", optarg);
        break;
      case 'w':
        doWrite = true;
        printf("will re-write all before test.\n");
        break;
      case 'x':
        singleWrite = true;
        singleWriteKey = optarg;
        printf("will single write key %s\n", optarg);
        break;
      case 'y':
        singleRead = true;
        singleReadKey = optarg;
        printf("will single read key %s\n", optarg);
        break;
      case 'r':
        doRead = true;
        printf("will run read test.\n");
        break;
      case 's':
        objSize = atoi(optarg);
        printf("object size = %d\n", objSize);
        break;
      case 'S':
        numShards = atoi(optarg);
        printf("num of shards = %d\n", numShards);
        break;
      case 'o':
        numOps = atol(optarg);
        printf("total number of reads to perform = %d\n", numOps);
        break;
      case 'n':
        numObjs = atol(optarg);
        printf("total number of objects = %d\n", numObjs);
        break;
      case 't':
        numThreads = atoi(optarg);
        printf("will use %d threads\n", numThreads);
        break;
      case 'c':
        dbCacheMB = atoi(optarg);
        printf("will use %d MB DB block cache\n", dbCacheMB);
        break;
      case 'q':
        totalTargetQPS = atol(optarg);
        printf("total read target QPS = %d\n", totalTargetQPS);
        break;
      case 'e':
        totalWriteTargetQPS = atol(optarg);
        printf("total write target QPS = %d\n", totalWriteTargetQPS);
        break;
      case 'k':
        numKeysPerRead = atoi(optarg);
        printf("Multi-get size = %d\n", numKeysPerRead);
        break;
      case '?':
        help();
        return 0;
      default:
        help();
        return 0;
    }
  }
  if (optind < argc) {
    help();
    return 0;
  }

  int numTasks = numThreads;

  //TryThreadPool();
  //TryRocksDB(kDBPath, numTasks, cacheMB);
  //TryKVInterface(kDBPath, numTasks, cacheMB);
  //return 0;

  std::thread  *workers = new std::thread[numTasks];
  mutex outputLock;

  cout << "will run " << numTasks << " benchmark threads on DB " << dbPath << endl;

  RocksDBInterface iface;
  rocksdb::DB* db = NULL;
  rocksdb::Status s;
  // Prepare general DB options.
  rocksdb::Options options;
  //TuneUniversalStyleCompaction(&options, dbCacheMB);
  //TuneLevelStyleCompaction(&options, dbCacheMB);

#if 0
  // Open normal DB
  s = rocksdb::DB::Open(options, dbPath, &db);
  assert(s.ok());
#else
  // Open DB interface with sharding.
  int iothreads = numShards;
  //assert(iface.Open(dbPath.c_str(), numShards, iothreads, dbCacheMB));
  assert(iface.Open((const char**)&dbPaths[0], dbPaths.size(), numShards, iothreads, dbCacheMB));
#endif

  if (singleWrite) {
    Write(singleWriteKey, &iface);
  }
  if (singleRead) {
    Read(singleReadKey, &iface);
  }
  if (singleWrite || singleRead) {
    return 0;
  }

  struct timespec tbegin, tend;
  struct timespec objBegin, objEnd;
  long elapsedMicroSec = 0;
  long elapsedMilliSec = 0;

  // Init random number.
  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  std::srand(tbegin.tv_nsec);

  // seq write objs.
  //int numObjs = 1000 * 1000 * 60;
  long writeObjCount = numObjs;
  long readObjCount = numOps;

  int *writeLatency = NULL;
  int *readLatency = NULL;
  if (doWrite) {
    writeLatency = new int[writeObjCount];
    memset(writeLatency, 0, writeObjCount * sizeof(int));
  }
  if (doRead) {
    readLatency = new int[readObjCount];
    memset(readLatency, 0, readObjCount * sizeof(int));
  }

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
    tasks[i].readLatency = readLatency + perTaskRead * i;
    tasks[i].targetQPS = totalTargetQPS / numTasks;
    tasks[i].writeTargetQPS = totalWriteTargetQPS / numTasks;

    sem_init(&tasks[i].sem_begin, 0, 0);
    sem_init(&tasks[i].sem_end, 0, 0);

    //tasks[i].db = db;
    tasks[i].dbIface = &iface;
    //tasks[i].writeOptions = writeOptions;
    //tasks[i].readOptions = readOptions;

    tasks[i].outputLock = &outputLock;

    workers[i] = std::thread(Worker, tasks + i);
  }

  unsigned long t1, t2, timeTotal;
  if (doWrite) {
    printf("Main: will start write phase...\n");
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
    sort(writeLatency, writeLatency + writeObjCount);
    printf("Overall write IOPS = %f\n", writeObjCount / (timeTotal / 1000000.0));
    PrintStats(writeLatency, writeObjCount, "\nOverall write latency in ms");
    printf("\nwait for background activities to settle...\n");
    sleep(3);
    printf("\ntable-cache memory usage after write: %ld\n",
           GetMemoryUsage(&iface));
  }

  // start worker to read.
  if (doRead) {
    // Warm up phase.
    printf("\n\nMain: warm up read...\n");
    for (int i = 0; i < numTasks; i++) {
      sem_post(&tasks[i].sem_begin);
    }
    // wait for read to finish.
    for (int i = 0; i < numTasks; i++) {
      sem_wait(&tasks[i].sem_end);
    }

    printf("\ntable-cache memory usage after warmup-read: %ld\n",
           GetMemoryUsage(&iface));

    printf("\n\nMain: will start read phase...\n");

    t1 = time_microsec();
    for (int i = 0; i < numTasks; i++) {
      sem_post(&tasks[i].sem_begin);
    }
    // wait for read to finish.
    for (int i = 0; i < numTasks; i++) {
      sem_wait(&tasks[i].sem_end);
    }
    timeTotal = time_microsec() - t1;
    printf("\ntable-cache memory usage after read: %ld\n",
           GetMemoryUsage(&iface));
  }

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
  return 0;
}
