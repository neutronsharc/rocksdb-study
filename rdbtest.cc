#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <bsd/stdlib.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <typeinfo>
#include <iostream>
#include <chrono>
#include <ctime>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "threadpool.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "utils.h"
#include "hdr_histogram.h"

using namespace std;

using namespace std::chrono;

static uint64_t GetRandom(uint64_t max_val) {
  return std::rand() % max_val;
}

struct TaskContext {
  // worker thread ID
  int id;

  // Init db with these many objs.
  uint64_t init_num_objs;

  // Obj id to create if next op is write.
  uint64_t next_obj_id;

  // Target qps by this worker.
  uint64_t target_qps;

  // ratio of write ops, 0.0 ~ 1.0. Main thread will set this value.
  double write_ratio;

  // This task should run this many usec.
  uint64_t runtime_usec;

  uint64_t num_reads;
  uint64_t read_bytes;
  uint64_t read_miss;
  uint64_t read_failure;

  uint64_t num_writes;
  uint64_t write_bytes;
  uint64_t write_failure;

  // When the task starts.
  uint64_t start_usec;
  // total # of ops done by this task.
  uint64_t total_ops;

  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;

  RocksDBShard *db;

  /////////////////////

  bool ShouldWrite();

  void RateLimit();
};


void TaskContext::RateLimit() {
  uint64_t target_time_usec = total_ops * 1000000 / target_qps;
  uint64_t actual_time_usec = NowInUsec() - start_usec;

  if (actual_time_usec < target_time_usec) {
    usleep(target_time_usec - actual_time_usec);
  }
}

bool TaskContext::ShouldWrite() {
  if (write_ratio == 0) {
    return false;
  }

  uint64_t maxv = 1000000;
  uint64_t thresh = maxv * write_ratio;
  uint64_t v = GetRandom(maxv);
  return v < thresh;
}


// Operation stats.
struct OpStats {
  uint64_t reads;
  uint64_t read_bytes;
  uint64_t read_miss;
  uint64_t read_failure;

  uint64_t writes;
  uint64_t write_bytes;
  uint64_t write_failure;
};

// Timer context struct.
struct TimerContext {
  timer_t *timer;

  // array of task contexts
  std::vector<TaskContext>* tasks;

  // number of tasks
  int ntasks;

  // cumulated stats of all tasks.
  OpStats  stats;

};


static uint32_t procid = -1;
static int timer_cycle_sec = 2;

static std::vector<int> obj_sizes(1, 4000);

static std::string db_path;

static int num_threads = 1;
static uint64_t init_num_objs = 1000000L;
static uint64_t total_target_qps = 10000L;
static int num_keys_per_read = 1;
static double write_ratio = 0;
static int runtime_sec = 30;

static uint64_t db_cache_mb = 5000L;

static bool overwrite_all = false;
static uint64_t total_write_target_qps = 1000000L;
static int num_shards = 8;
static uint64_t num_ops = 1000L;
static bool count_latency = false;

static pthread_mutex_t read_histo_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t write_histo_lock = PTHREAD_MUTEX_INITIALIZER;
static struct hdr_histogram *read_histo = NULL;
static struct hdr_histogram *write_histo = NULL;


static void TimerCallback(union sigval sv) {
  TimerContext *tc = (TimerContext*)sv.sival_ptr;

  vector<TaskContext>& tasks = *tc->tasks;
  int ntasks = tc->ntasks;
  //printf("in timer callback: %d tasks, task ctx %p\n", ntasks, tasks);

  OpStats last_stats;
  memcpy(&last_stats, &tc->stats, sizeof(OpStats));
  memset(&tc->stats, 0, sizeof(OpStats));

  for (int i = 0; i < ntasks; i++) {
    tc->stats.reads += tasks[i].num_reads;
    tc->stats.read_bytes += tasks[i].read_bytes;
    tc->stats.read_failure += tasks[i].read_failure;
    tc->stats.read_miss += tasks[i].read_miss;

    tc->stats.writes += tasks[i].num_writes;
    tc->stats.write_bytes  += tasks[i].write_bytes;
    tc->stats.write_failure  += tasks[i].write_failure;
  }

  uint64_t last_obj_id;
  if (overwrite_all) {
    last_obj_id = tc->stats.writes;
  } else {
    last_obj_id = tc->stats.writes + init_num_objs;
  }

  printf("%s: proc %d in past %d seconds:  %ld reads (%ld failure, %ld miss), "
         "%ld writes (%ld failure), latest write # %ld\n",
         TimestampString().c_str(),
         procid,
         timer_cycle_sec,
         tc->stats.reads - last_stats.reads,
         tc->stats.read_failure - last_stats.read_failure,
         tc->stats.read_miss - last_stats.read_miss,
         tc->stats.writes - last_stats.writes,
         tc->stats.write_failure - last_stats.write_failure,
         last_obj_id
         );

}

static void AddReadLatHisto(uint64_t lat) {
  pthread_mutex_lock(&read_histo_lock);
  hdr_record_value(read_histo, lat);
  pthread_mutex_unlock(&read_histo_lock);
}

static void AddWriteLatHisto(uint64_t lat) {
  pthread_mutex_lock(&write_histo_lock);
  hdr_record_value(write_histo, lat);
  pthread_mutex_unlock(&write_histo_lock);
}

static void Worker(TaskContext *task) {

  char key[128];
  char charvalue[100000];
  int objsize;

  rocksdb::Status status;

  uint64_t begin_usec = NowInUsec();

  if (overwrite_all) {
    sem_wait(&task->sem_begin);
    dbg("Task %d: overwrite with %ld objs ...\n",
        task->id, task->init_num_objs);

    for (int i = 0; i < task->init_num_objs; i++) {
      objsize = obj_sizes[i % obj_sizes.size()];

      sprintf(key, "thr-%d-key-%d", task->id, i);

      arc4random_buf(charvalue, objsize);
      EncodeBuffer(charvalue, objsize);

      status = task->db->Put(key, strlen(key), charvalue, objsize);
      if (!status.ok()) {
        err("task %d: failed to write key %s (data size %d): %s\n",
            task->id, key, objsize, status.ToString().c_str());
        task->write_failure++;
      } else {
        task->num_writes++;
        task->write_bytes += objsize;
      }
    }
    sem_post(&task->sem_end);
    dbg("Task %d: finished overwriting\n", task->id);
  }

  uint64_t obj_id = task->init_num_objs;
  uint64_t ops = 0;
  uint64_t t1;

  sem_wait(&task->sem_begin);
  dbg("Task %d: start workload...\n", task->id);
  task->start_usec = NowInUsec();

  while (NowInUsec() - task->start_usec < task->runtime_usec) {
    task->total_ops++;

    if (task->ShouldWrite()) {
      // is a write
      objsize = obj_sizes[obj_id  % obj_sizes.size()];
      sprintf(key, "thr-%d-key-%ld", task->id, obj_id);
      arc4random_buf(charvalue, objsize);
      EncodeBuffer(charvalue, objsize);

      t1 = NowInUsec();
      status = task->db->Put(key, strlen(key), charvalue, objsize);
      if (count_latency) {
        AddWriteLatHisto(NowInUsec() - t1);
      }
      if (status.ok()) {
        task->num_writes++;
        task->write_bytes += objsize;
        obj_id++;
      } else {
        err("task %d: failed to write key %s (data size %d): %s\n",
            task->id, key, objsize, status.ToString().c_str());
        task->write_failure++;
      }
    } else {
      string value;
      uint64_t kid = GetRandom(obj_id);
      sprintf(key, "thr-%d-key-%ld", task->id, kid);
      // is a read
      t1 = NowInUsec();
      status = task->db->Get(key, &value);
      if (count_latency) {
        AddReadLatHisto(NowInUsec() - t1);
      }
      if (status.ok()) {
        task->num_reads++;
        task->read_bytes += value.size();
      } else if (status.IsNotFound()) {
        task->read_miss++;
      } else {
        task->read_failure++;
        err("task %d: failed to read key %s: %s\n",
            task->id, key, status.ToString().c_str());
      }
    }
    task->RateLimit();
  }
  sem_post(&task->sem_end);
  dbg("Task %d: workload finished\n", task->id);

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
  RocksDBEngine rdb;
  assert(rdb.OpenDB(dbpath.c_str(), num_threads, cache_mb) == true);

  char key[128];
  char charvalue[1024];
  int objsize = 1023;

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


void Write(string key, RocksDBEngine *dbIface) {
  int obj_size = 4000;

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
  printf("write key: %s, vlen = %d, value = %s\n",
         rqst.key, rqst.vlen, rqst.value);
}

void Read(string key, RocksDBEngine *dbIface) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.key = key.data();
  rqst.keylen = key.size();

  assert(dbIface->Get(&rqst) == true);
  if (rqst.retcode != SUCCESS) {
    printf("cannot find key %s\n", rqst.key);
  } else {
    printf("key: %s, vlen=%d, value: %s\n",
           rqst.key, rqst.vlen, rqst.value);
    free(rqst.value);
  }
}

static void PrintHistoStats(struct hdr_histogram *histogram,
                            const char *header) {
  int hist_min = hdr_value_at_percentile(histogram, 0);
  int hist_max = (int)hdr_max(histogram);
  int hist_p10 = hdr_value_at_percentile(histogram, 10);
  int hist_p20 = hdr_value_at_percentile(histogram, 20);
  int hist_p50 = hdr_value_at_percentile(histogram, 50);
  int hist_p90 = hdr_value_at_percentile(histogram, 90);
  int hist_p95 = hdr_value_at_percentile(histogram, 95);
  int hist_p99 = hdr_value_at_percentile(histogram, 99);
  int hist_p999 = hdr_value_at_percentile(histogram, 99.9);

  cout << header << endl;
  cout << setw(12) << "min"
       << setw(12) << "10 %"
       << setw(12) << "20 %"
       << setw(12) << "50 %"
       << setw(12) << "90 %"
       << setw(12) << "95 %"
       << setw(12) << "99 %"
       << setw(12) << "99.9 %"
       << setw(12) << "max" << endl;
  cout << setw(12) << hist_min / 1000.0
       << setw(12) << hist_p10 / 1000.0
       << setw(12) << hist_p20 / 1000.0
       << setw(12) << hist_p50 / 1000.0
       << setw(12) << hist_p90 / 1000.0
       << setw(12) << hist_p95 / 1000.0
       << setw(12) << hist_p99 / 1000.0
       << setw(12) << hist_p999 / 1000.0
       << setw(12) << hist_max / 1000.0 << endl;
}


void help() {
  printf("Test RocksDB raw performance, mixed r/w ratio: \n");
  printf("parameters: \n");
  printf("-p <dbpath>          : rocksdb paths. Must provide.\n");
  printf("-s <obj size>        : object size in bytes. Def = 4000\n");
  printf("-n <num of objs>     : Init db with these many objects. Def = 1000000\n");
  printf("-t <num of threads>  : number of worker threads to run. Def = 1\n");
  printf("-i <seconds>         : Run workoad for these many seconds. Default = 30\n");
  printf("-c <DB cache>        : DB cache in MB. Def = 5000\n");
  printf("-q <QPS>             : Aggregated target QPS by all threads. \n"
         "                       Def = 10000 op/sec\n");
  printf("-w <write ratio>     : write ratio. Def = 0\n");
  printf("-m <multiget>        : multi-get these number of keys in one get.\n"
         "                       def = 1 key\n");
  printf("-x <key>             : write this key with random value of given size\n");
  printf("-y <key>             : read this key from DB\n");
  printf("-l                   : count r/w latency. Def not\n");
  printf("-o                   : overwrite entire DB before test. Def not\n");
  printf("-h                   : this message\n");
  //printf("-d <shards>          : number of shards. Def = 8\n");
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
  vector<char*> sizes;

  while ((c = getopt(argc, argv, "p:s:d:n:t:i:c:q:w:m:x:y:ohl")) != EOF) {
    switch(c) {
      case 'h':
        help();
        return 0;
      case 'p':
        db_path = optarg;
        db_paths = SplitString(optarg, ",");
        printf("db path : %s\n", optarg);
        break;
      case 's':
        printf("obj sizes:  %s\n", optarg);
        sizes = SplitString(optarg, ",");
        break;
      case 'd':
        num_shards = atoi(optarg);
        printf("num of shards = %d\n", num_shards);
        break;
      case 'o':
        overwrite_all = true;
        printf("will overwrite all objs upfront.\n");
        break;
      case 'n':
        init_num_objs = atol(optarg);
        printf("init objects count = %ld\n", init_num_objs);
        break;
      case 't':
        num_threads = atoi(optarg);
        printf("will use %d threads\n", num_threads);
        break;
      case 'w':
        write_ratio = atof(optarg);
        printf("write ratio = %f\n", write_ratio);
        break;
      case 'i':
        runtime_sec = atoi(optarg);
        printf("run benchmark for %d second\n", runtime_sec);
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
      case 'c':
        db_cache_mb = atoi(optarg);
        printf("will use %ld MB DB block cache\n", db_cache_mb);
        break;
      case 'q':
        total_target_qps = atol(optarg);
        printf("total read target QPS = %ld\n", total_target_qps);
        break;
      case 'm':
        num_keys_per_read = atoi(optarg);
        printf("Multi-get keys = %d\n", num_keys_per_read);
        break;
      case 'l':
        count_latency = true;
        printf("Will count r/w latency\n");
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

  if (sizes.size() > 0) {
    obj_sizes.clear();
    for (char *s : sizes) {
      obj_sizes.push_back(atoi(s));
    }
  }

  procid = ::getpid();

  cout << "will run " << num_threads << " benchmark threads on DB " << db_path << endl;

  RocksDBShard shard;

  // Single-read  or single-write needs special care.
  if (single_write || single_read) {
    if (!shard.OpenDB(db_path, db_path)) {
      err("failed to open db\n");
      return -1;
    }
  }

  char tmpbuf[10000];
  int tsize = 1000;
  rocksdb::Status status;
  if (single_write) {
    arc4random_buf(tmpbuf, tsize);
    status = shard.Put(single_write_key.c_str(),
                       single_write_key.size(),
                       tmpbuf,
                       tsize);
    printf("write key %s, %s\n",
           single_write_key.c_str(),
           status.ok() ? "success" : "failed");
  }
  if (single_read) {
    string value;
    status = shard.Get(single_read_key, &value);
    printf("read key %s, stat = %s,  return size  %ld\n",
           single_read_key.c_str(),
           status.ToString().c_str(),
           value.size());
  }

  if (single_write || single_read) {
    shard.CloseDB();
    return 0;
  }

  // Init random number.
  std::srand(NowInUsec());

  // Init the histogram.
  int lowest = 1;
  int highest = 100000000;
  int sig_digits = 3;
  hdr_init(lowest, highest, sig_digits, &read_histo);
  hdr_init(lowest, highest, sig_digits, &write_histo);
  printf("memory footprint of hdr-histogram: %ld\n",
         hdr_get_memory_size(read_histo));


  //////////////////
  // Populate db upfront.

  vector<std::thread> workers;
  vector<TaskContext> tasks(num_threads);

  for (int i = 0; i < num_threads; i++) {
    memset(&tasks[i], 0, sizeof(TaskContext));

    tasks[i].id = i;
    tasks[i].init_num_objs = init_num_objs / num_threads;
    tasks[i].target_qps = total_target_qps / num_threads;
    tasks[i].write_ratio = write_ratio;
    tasks[i].runtime_usec = runtime_sec * 1000000L;

    sem_init(&tasks[i].sem_begin, 0, 0);
    sem_init(&tasks[i].sem_end, 0, 0);

    tasks[i].db = &shard;

    workers.push_back(std::thread(Worker, &tasks[i]));
  }


  ////////////////
  // Create timer.
  timer_t timer;
  TimerContext tctx;

  memset(&tctx, 0, sizeof(tctx));
  tctx.tasks = &tasks;
  tctx.ntasks = num_threads;
  tctx.timer = &timer;

  CreateTimer(&timer, timer_cycle_sec * 1000, TimerCallback, &tctx);

  ///////////////////
  // Phase 1: populate the DB with data.
  if (overwrite_all) {
    if (!shard.OpenForBulkLoad(db_path)) {
      err("failed to open DB for overwrite: %s\n", db_path.c_str());
      return -1;
    }
    printf("\nStart overwrite phase...\n");
    for (auto& task : tasks) {
      sem_post(&task.sem_begin);
    }
    for (auto& task : tasks) {
      sem_wait(&task.sem_end);
    }
    printf("\nfinished overwrite\n");
    shard.CloseDB();
  }

  ////////////////
  // Phase 2: run r/w workload.
  if (!shard.OpenDB(db_path, db_path)) {
    err("failed to open db before running workload\n");
    return -1;
  }

  // Now do a compaction.
  printf("before compaction\n");
  shard.CompactRange(nullptr, nullptr);
  printf("after compaction\n");

  printf("\nStart workload ...\n");

  uint64_t tstart = NowInUsec();

  for (auto& task : tasks) {
    sem_post(&task.sem_begin);
  }
  for (auto& task : tasks) {
    sem_wait(&task.sem_end);
  }

  printf("\nFinished workload\n");

  DeleteTimer(&timer);

  ///////////////////////
  // Count stats.
  uint64_t readBytes = 0, writeBytes = 0;
  uint64_t readFail = 0, readMiss = 0;
  uint64_t totalOps = 0, writeFail = 0;
  uint64_t totalReadOps = 0, totalWriteOps = 0;
  for (int i = 0; i < num_threads; i++) {
    if (workers[i].joinable()) {
      workers[i].join();
      printf("joined thread %d\n", i);
    }
    if (overwrite_all) {
      tasks[i].num_writes -= tasks[i].init_num_objs;
    }
    readBytes += tasks[i].read_bytes;
    readMiss += tasks[i].read_miss;
    readFail += tasks[i].read_failure;
    totalReadOps += tasks[i].num_reads;

    writeBytes += tasks[i].write_bytes;
    writeFail += tasks[i].write_failure;
    totalWriteOps += tasks[i].num_writes;

    totalOps += tasks[i].num_reads;
    totalOps += tasks[i].num_writes;
  }

  double totalSecs = (NowInUsec() - tstart) / 1000000.0;

  printf("\nIn total:  %ld ops in %f sec (%ld read, %ld write).\n"
         "Total IOPS = %.f, read IOPS %.f, write IOPS %.f\n"
         "Bandwidth = %.3f MB/s, read bw %.3f MB/s, write bw %.3f MB/s\n"
         "Read miss %ld (%.2f%%), read failure %ld, write failure %ld\n",
         totalOps, totalSecs, totalReadOps, totalWriteOps,
         totalOps / totalSecs,
         totalReadOps / totalSecs, totalWriteOps / totalSecs,
         (readBytes + writeBytes) / totalSecs / 1000000,
         readBytes / totalSecs / 1000000,
         writeBytes / totalSecs / 1000000,
         readMiss,
         (readMiss + 0.0) / totalReadOps * 100,
         readFail,
         writeFail);

  if (count_latency) {
    PrintHistoStats(read_histo, "\n============== Read latency in ms");
    PrintHistoStats(write_histo, "\n============== Write latency in ms");
  }

  ////////////////////
  // clearn up.
  free(read_histo);
  free(write_histo);
  return 0;
}

