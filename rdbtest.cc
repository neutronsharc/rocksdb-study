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

namespace {

struct WorkerTask {
  // worker thread ID
  int id;

  bool do_read;
  bool do_write;

  // Number of r/w to perform
  unsigned long num_writes;
  unsigned long num_reads;

  // number of failures.
  unsigned long write_failures;
  unsigned long read_failures;

  // Target qps issued by this worker.
  unsigned long target_qps;

  // Target qps for write ops by this worker.
  unsigned long write_target_qps;

  // ratio of write ops, 0.0 ~ 1.0. Main thread will set this value.
  double write_ratio;

  // record operation latency in micro-sec.
  int *write_latency;
  int *read_latency;

  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;

  rocksdb::DB *db;
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;

  RocksDBInterface* db_iface;

  // A lock to sync output.
  mutex *output_lock;
};


string db_path;
bool do_write = false;
bool do_read = false;
int num_threads = 1;
int obj_size = 1000;
long num_objs = 1000L;
long num_ops = 1000L;
long db_cache_mb = 5000L;
long total_target_qps = 1000000L;
long total_write_target_qps = 1000000L;
int num_keys_per_read = 1;
int num_shards = 8;

vector<rocksdb::ColumnFamilyHandle*> family_handles;

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
  assert(obj_size <= 10000);

  rocksdb::Status status;
  unsigned long tBeginUs, tEndUs, t1, t2;

  long elapsed_usec;

  if (task->do_write) {
    printf("worker %d will do %d writes...\n", task->id, task->num_writes);
    sem_wait(&task->sem_begin);

    tBeginUs = time_microsec();
    for (int i = 0; i < task->num_writes; i++) {
      sprintf(key, "task-%d-key-%d", task->id, i);
      arc4random_buf(charvalue, obj_size);
      sprintf(charvalue, "valueof-task-%d-key-%d", task->id, i);
      charvalue[strlen(charvalue)] = ' ';
      // memcached protocol requires '\r\n' at end of value.
      charvalue[obj_size] = '\r';
      charvalue[obj_size + 1] = '\n';
      //rocksdb::Slice keyslice(key, strlen(key));
      //rocksdb::Slice valueslice(charvalue, obj_size + 2);
      KVRequest rqst;
      memset(&rqst, 0, sizeof(rqst));
      rqst.type = PUT;
      rqst.key = key;
      rqst.keylen = strlen(key);
      rqst.value = charvalue;
      rqst.vlen = obj_size + 2;

      t1 = time_microsec();
      //status = task->db->Put(task->write_options, keyslice, valueslice);
      assert(task->db_iface->Put(&rqst) == true);
      t2 = time_microsec();
      task->write_latency[i] = t2 - t1;
      //assert(status.ok());
      if ((i + 1) % 500000 == 0) {
        printf("task %d: write %d \n", task->id, i + 1);
      }
      // Throttle to target QPS.
      unsigned long actual_spent_time = time_microsec() - tBeginUs;
      unsigned long target_spent_time =
        (unsigned long)((i + 1.0) * 1000000 / task->write_target_qps);
      if (actual_spent_time < target_spent_time) {
        usleep(target_spent_time - actual_spent_time);
      }
    }
    tEndUs = time_microsec();
    printf("task %d finished write ...\n", task->id);
    elapsed_usec = tEndUs - tBeginUs;
    task->output_lock->lock();
    cout << "thread " << task->id << " has written " << task->num_writes << " objs" << " in "
         << elapsed_usec / 1000000.0 << " seconds, "
         << "data = " << (obj_size * task->num_writes / 1024.0 / 1024) << " MB, "
         << "IOPS = " << task->num_writes * 1000000.0 / elapsed_usec << endl;
    task->output_lock->unlock();

    sem_post(&task->sem_end);
  }

  char keys[100][200];

  if (task->do_read) {
    sem_wait(&task->sem_begin);
    int warmups = 100000;
    // Warm up read.
    printf("worker %d will do %d warm-up reads ...\n", task->id, warmups);
    for (int i = 0; i < warmups; i++) {
      unsigned long objID = get_random(task->num_writes);
      sprintf(key, "task-%d-key-%d", task->id, (int)(objID));
      KVRequest rqst;
      memset(&rqst, 0, sizeof(rqst));
      rqst.type = GET;
      rqst.key = key;
      rqst.keylen = strlen(key);
      assert(task->db_iface->Get(&rqst) == true);
      if (rqst.retcode != SUCCESS) {
        printf("cannot find key %s\n", rqst.key);
      } else {
        free(rqst.value);
      }
    }
    sem_post(&task->sem_end);
  }

  if (task->do_read) {
    sem_wait(&task->sem_begin);
    printf("worker %d will do %d reads ...\n", task->id, task->num_reads);
    tBeginUs = time_microsec();
    for (int i = 0; i < task->num_reads; i++) {
      if (num_keys_per_read > 1) {
        KVRequest rqsts[num_keys_per_read];
        memset(rqsts, 0, sizeof(KVRequest) * num_keys_per_read);
        //vector<rocksdb::Slice> keySlices;
        //vector<string> values;
        t1 = time_microsec();
        for (int k = 0; k < num_keys_per_read; k++) {
          unsigned long objID = get_random(task->num_writes);
          sprintf(keys[k], "task-%d-key-%d", task->id, (int)(objID));
          //keySlices.push_back(
          //  rocksdb::Slice(keys[k], strlen(keys[k])));
          rqsts[k].type = GET;
          rqsts[k].key = keys[k];
          rqsts[k].keylen = strlen(keys[k]);
          rqsts[k].reserved = NULL;
        }
        //vector<rocksdb::Status> rets = task->db->MultiGet(task->read_options,
        //                                                  keySlices,
        //                                                  &values);
        assert(task->db_iface->MultiGet(rqsts, num_keys_per_read));
        t2 = time_microsec();
        task->read_latency[i] = t2 - t1;
        for (int k = 0; k < num_keys_per_read; k++) {
          //if (!rets[k].ok()) {
          //  printf("failed to get key %s: ret = %s\n", keys[k],
          //      rets[k].ToString().c_str());
          //  continue;
          //}
          //assert(values[k].length() == obj_size + 2);
          if (rqsts[k].retcode != SUCCESS) {
            printf("failed to get key %s\n", rqsts[k].key);
            continue;
          }
          assert(rqsts[k].vlen == obj_size + 2);
          int tidAtKey, vidAtKey, tidAtValue, vidAtValue;
          sscanf(rqsts[k].key, "task-%d-key-%d", &tidAtKey, &vidAtKey);
          sscanf(rqsts[k].value, "valueof-task-%d-key-%d", &tidAtValue, &vidAtValue);
          assert(tidAtKey == task->id);
          assert(tidAtKey == tidAtValue);
          assert(vidAtKey == vidAtValue);
          free(rqsts[k].value);
        }
      } else {
        unsigned long objID = get_random(task->num_writes);
        sprintf(key, "task-%d-key-%d", task->id, (int)(objID));
        KVRequest rqst;
        memset(&rqst, 0, sizeof(rqst));
        rqst.key = key;
        rqst.keylen = strlen(key);
        rqst.type = GET;
        rqst.reserved = NULL;

        //string value;
        t1 = time_microsec();
        //status = task->db->Get(task->read_options, key, &value);
        assert(task->db_iface->Get(&rqst) == true);
        t2 = time_microsec();
        task->read_latency[i] = t2 - t1;
        //assert(status.ok());
        //assert(value.length() == obj_size || value.length() == obj_size + 2);
        if (rqst.retcode != SUCCESS) {
          printf("failed to get key %s\n", rqst.key);
        } else {
          assert(rqst.vlen == obj_size || rqst.vlen == obj_size + 2);
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
               task->id, i + 1, GetMemoryUsage(task->db_iface));
      }
      // Throttle to target QPS.
      unsigned long actualSpentTime = time_microsec() - tBeginUs;
      unsigned long targetSpentTime =
        (unsigned long)((i + 1.0) * 1000000 / task->target_qps);
      if (actualSpentTime < targetSpentTime) {
        usleep(targetSpentTime - actualSpentTime);
      }
    }
    tEndUs = time_microsec();
    elapsed_usec = tEndUs - tBeginUs;
    task->output_lock->lock();
    cout << "thread " << task->id << " has read "
         << task->num_reads * num_keys_per_read << " objs in "
         << elapsed_usec / 1000000.0 << " seconds, "
         << "data = "
         << (obj_size * task->num_reads * num_keys_per_read / 1024.0 / 1024)
         << " MB, "
         << "IOPS = " << task->num_reads * 1000000.0 / elapsed_usec << endl;
    task->output_lock->unlock();
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

void TryRocksDB(string &dbpath, int num_threads, int cache_mb) {
  RocksDBInterface rdb;
  assert(rdb.OpenDB(dbpath.c_str(), num_threads, cache_mb) == true);

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
  printf("-w                   : overwrite entire DB before test. Def to not\n");
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
  char buf[obj_size + 1];
  arc4random_buf(buf, obj_size);
  sprintf(buf, "%s", key.c_str());
  buf[strlen(buf)] = ' ';
  buf[obj_size] = 0;
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.key = key.data();
  rqst.keylen = key.size();
  rqst.value = buf;
  rqst.vlen = obj_size;

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
  vector<char*> db_paths;
  bool single_read = false, single_write = false;
  string single_read_key, single_write_key;

  while ((c = getopt(argc, argv, "e:p:orhs:n:t:c:q:k:o:S:x:y:")) != EOF) {
    switch(c) {
      case 'h':
        help();
        return 0;
      case 'p':
        db_path = optarg;
        db_paths = SplitString(optarg, ",");
        printf("db path : %s\n", optarg);
        break;
      case 'w':
        do_write = true;
        printf("will re-write all before test.\n");
        break;
      case 'x':
        single_write = true;
        single_write_key = optarg;
        printf("will single write key %s\n", optarg);
        break;
      case 'y':
        single_read = true;
        single_read_key = optarg;
        printf("will single read key %s\n", optarg);
        break;
      case 'r':
        do_read = true;
        printf("will run read test.\n");
        break;
      case 's':
        obj_size = atoi(optarg);
        printf("object size = %d\n", obj_size);
        break;
      case 'S':
        num_shards = atoi(optarg);
        printf("num of shards = %d\n", num_shards);
        break;
      case 'o':
        num_ops = atol(optarg);
        printf("total number of reads to perform = %d\n", num_ops);
        break;
      case 'n':
        num_objs = atol(optarg);
        printf("total number of objects = %d\n", num_objs);
        break;
      case 't':
        num_threads = atoi(optarg);
        printf("will use %d threads\n", num_threads);
        break;
      case 'c':
        db_cache_mb = atoi(optarg);
        printf("will use %d MB DB block cache\n", db_cache_mb);
        break;
      case 'q':
        total_target_qps = atol(optarg);
        printf("total read target QPS = %d\n", total_target_qps);
        break;
      case 'e':
        total_write_target_qps = atol(optarg);
        printf("total write target QPS = %d\n", total_write_target_qps);
        break;
      case 'k':
        num_keys_per_read = atoi(optarg);
        printf("Multi-get size = %d\n", num_keys_per_read);
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

  int num_tasks = num_threads;

  //TryThreadPool();
  //TryRocksDB(kDBPath, numTasks, cacheMB);
  //TryKVInterface(kDBPath, numTasks, cacheMB);
  //return 0;

  std::thread  *workers = new std::thread[num_tasks];
  mutex outputLock;

  cout << "will run " << num_tasks << " benchmark threads on DB " << db_path << endl;

  RocksDBInterface iface;
  rocksdb::DB* db = NULL;
  rocksdb::Status s;
  // Prepare general DB options.
  rocksdb::Options options;
  //TuneUniversalStyleCompaction(&options, db_cache_mb);
  //TuneLevelStyleCompaction(&options, db_cache_mb);

#if 0
  // Open normal DB
  s = rocksdb::DB::Open(options, db_path, &db);
  assert(s.ok());
#else
  // Open DB interface with sharding.
  int iothreads = num_shards;
  //assert(iface.Open(db_path.c_str(), num_shards, iothreads, db_cache_mb));
  assert(iface.Open((const char**)&db_paths[0], db_paths.size(), num_shards, iothreads, db_cache_mb));
#endif

  if (single_write) {
    Write(single_write_key, &iface);
  }
  if (single_read) {
    Read(single_read_key, &iface);
  }
  if (single_write || single_read) {
    return 0;
  }

  struct timespec tbegin, tend;
  struct timespec objBegin, objEnd;
  long elapsed_usec = 0;
  long elapsed_msec = 0;

  // Init random number.
  clock_gettime(CLOCK_MONOTONIC, &tbegin);
  std::srand(tbegin.tv_nsec);

  // seq write objs.
  //int num_objs = 1000 * 1000 * 60;
  long write_obj_count = num_objs;
  long read_obj_count = num_ops;

  int *write_latency = NULL;
  int *read_latency = NULL;
  if (do_write) {
    write_latency = new int[write_obj_count];
    memset(write_latency, 0, write_obj_count * sizeof(int));
  }
  if (do_read) {
    read_latency = new int[read_obj_count];
    memset(read_latency, 0, read_obj_count * sizeof(int));
  }

  int per_task_write = write_obj_count / num_tasks;
  int per_task_read = read_obj_count / num_tasks;
  WorkerTask *tasks = new WorkerTask[num_tasks];

  for (int i = 0; i < num_tasks; i++) {
    tasks[i].id = i;
    tasks[i].do_read = do_read;
    tasks[i].do_write = do_write;
    tasks[i].num_writes = per_task_write;
    tasks[i].num_reads = per_task_read;
    tasks[i].write_latency = write_latency + per_task_write * i;
    tasks[i].read_latency = read_latency + per_task_read * i;
    tasks[i].target_qps = total_target_qps / num_tasks;
    tasks[i].write_target_qps = total_write_target_qps / num_tasks;

    sem_init(&tasks[i].sem_begin, 0, 0);
    sem_init(&tasks[i].sem_end, 0, 0);

    //tasks[i].db = db;
    tasks[i].db_iface = &iface;
    //tasks[i].write_options = write_options;
    //tasks[i].read_options = read_options;

    tasks[i].output_lock = &outputLock;

    workers[i] = std::thread(Worker, tasks + i);
  }

  unsigned long t1, t2, time_total;
  if (do_write) {
    printf("Main: will start write phase...\n");
    t1 = time_microsec();
    // start worker to write.
    for (int i = 0; i < num_tasks; i++) {
      sem_post(&tasks[i].sem_begin);
    }
    // wait for write to finish.
    for (int i = 0; i < num_tasks; i++) {
      sem_wait(&tasks[i].sem_end);
    }
    time_total = time_microsec() - t1;

    // output stats
    sort(write_latency, write_latency + write_obj_count);
    printf("Overall write IOPS = %f\n", write_obj_count / (time_total / 1000000.0));
    PrintStats(write_latency, write_obj_count, "\nOverall write latency in ms");
    printf("\nwait for background activities to settle...\n");
    sleep(3);
    printf("\ntable-cache memory usage after write: %ld\n",
           GetMemoryUsage(&iface));
  }

  // start worker to read.
  if (do_read) {
    // Warm up phase.
    printf("\n\nMain: warm up read...\n");
    for (int i = 0; i < num_tasks; i++) {
      sem_post(&tasks[i].sem_begin);
    }
    // wait for read to finish.
    for (int i = 0; i < num_tasks; i++) {
      sem_wait(&tasks[i].sem_end);
    }

    printf("\ntable-cache memory usage after warmup-read: %ld\n",
           GetMemoryUsage(&iface));

    printf("\n\nMain: will start read phase...\n");

    t1 = time_microsec();
    for (int i = 0; i < num_tasks; i++) {
      sem_post(&tasks[i].sem_begin);
    }
    // wait for read to finish.
    for (int i = 0; i < num_tasks; i++) {
      sem_wait(&tasks[i].sem_end);
    }
    time_total = time_microsec() - t1;
    printf("\ntable-cache memory usage after read: %ld\n",
           GetMemoryUsage(&iface));
  }

  for (int i = 0; i < num_tasks; i++) {
    if (workers[i].joinable()) {
      workers[i].join();
      printf("joined thread %d\n", i);
    }
  }
  // output stats
  if (do_read) {
    sort(read_latency, read_latency + read_obj_count);
    printf("Overall read IOPS = %f\n", read_obj_count / (time_total / 1000000.0));
    PrintStats(read_latency, read_obj_count, "\nOverall read latency in ms");
  }

  delete [] workers;
  delete [] read_latency;
  delete [] write_latency;
  return 0;
}

}  // namespace
