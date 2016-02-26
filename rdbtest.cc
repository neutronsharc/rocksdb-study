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


struct TaskContext {
  // worker thread ID
  int id;

  // Init db with these many objs.
  uint64_t init_obj_cnt;

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
  uint64_t read_failures;

  uint64_t num_writes;
  uint64_t write_bytes;
  uint64_t write_failures;

  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;

  rocksdb::DB *db;
  rocksdb::WriteOptions write_options;
  rocksdb::ReadOptions read_options;

  RocksDBEngine* db_iface;

  // A lock to sync output.
  mutex *output_lock;

};



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

static pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;
static struct hdr_histogram *histo_read = NULL;
static struct hdr_histogram *histo_write = NULL;


void TimerCallback(union sigval sv) {
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
    tc->stats.read_failure += tasks[i].read_failures;
    tc->stats.read_miss += tasks[i].read_miss;


    tc->stats.writes += tasks[i].num_writes;
    tc->stats.write_bytes  += tasks[i].write_bytes;
    tc->stats.write_failure  += tasks[i].write_failures;
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
         init_num_objs
         );

}

vector<rocksdb::ColumnFamilyHandle*> family_handles;

uint64_t GetMemoryUsage(RocksDBEngine *dbIface);

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

void Worker(TaskContext *task) {

  char key[128];
  char charvalue[10002];
  int obj_size = 4000;

  assert(obj_size <= 10000);

  rocksdb::Status status;
  unsigned long tBeginUs, tEndUs, t1, t2;

  long elapsed_usec;
  uint64_t begin_usec = NowInUsec();

  while (NowInUsec() - begin_usec < task->runtime_usec) {
    usleep(10000);
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
  RocksDBEngine rdb;
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

uint64_t GetMemoryUsage(RocksDBEngine *dbIface) {
  KVRequest rqst;
  memset(&rqst, 0, sizeof(rqst));
  rqst.type = GET_MEMORY_USAGE;
  dbIface->GetMemoryUsage(&rqst);
  return rqst.vlen;
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

  while ((c = getopt(argc, argv, "p:s:d:n:t:i:c:q:w:m:x:y:oh")) != EOF) {
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
  //TryThreadPool();
  //TryRocksDB(kDBPath, numTasks, cacheMB);
  //TryKVInterface(kDBPath, numTasks, cacheMB);
  //return 0;

  vector<std::thread> workers;
  mutex outputLock;

  cout << "will run " << num_threads << " benchmark threads on DB " << db_path << endl;

  RocksDBShard shard;

  //if (!shard.OpenDB(db_path)) {
  if (!shard.OpenForBulkLoad(db_path)) {
    err("failed to open db\n");
    return -1;
  }

  char tmpbuf[10000];
  int tsize = 1000;
  if (single_write) {
    arc4random_buf(tmpbuf, tsize);
    shard.Put(single_write_key.c_str(),
              single_write_key.size(),
              tmpbuf,
              tsize);
  }
  if (single_read) {
    string value;
    shard.Get(single_read_key, &value);
    printf("read return: \"%s\", size %ld\n",
           value.c_str(), value.size());
  }
  if (single_write || single_read) {
    return 0;
  }

  // Init random number.
  std::srand(NowInUsec());

  // Init the histogram.
  int lowest = 1;
  int highest = 100000000;
  int sig_digits = 3;
  hdr_init(lowest, highest, sig_digits, &histo_read);
  hdr_init(lowest, highest, sig_digits, &histo_write);
  printf("memory footprint of hdr-histogram: %ld\n",
         hdr_get_memory_size(histo_read));

  // Populate db with these many objs.
  vector<TaskContext> tasks(num_threads);

  for (int i = 0; i < num_threads; i++) {
    memset(&tasks[i], 0, sizeof(TaskContext));

    tasks[i].id = i;
    tasks[i].target_qps = total_target_qps / num_threads;
    tasks[i].write_ratio = write_ratio;
    tasks[i].runtime_usec = runtime_sec * 1000000L;

    sem_init(&tasks[i].sem_begin, 0, 0);
    sem_init(&tasks[i].sem_end, 0, 0);

    //tasks[i].db = db;
    //tasks[i].db_iface = &iface;
    //tasks[i].write_options = write_options;
    //tasks[i].read_options = read_options;

    tasks[i].output_lock = &outputLock;

    workers.push_back(std::thread(Worker, &tasks[i]));
  }


  // Create timer.
  timer_t timer;
  TimerContext tctx;

  memset(&tctx, 0, sizeof(tctx));
  tctx.tasks = &tasks;
  tctx.ntasks = num_threads;
  tctx.timer = &timer;

  CreateTimer(&timer, timer_cycle_sec * 1000, TimerCallback, &tctx);


#if 0
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

  // output stats
  if (do_read) {
    sort(read_latency, read_latency + read_obj_count);
    printf("Overall read IOPS = %f\n", read_obj_count / (time_total / 1000000.0));
    PrintStats(read_latency, read_obj_count, "\nOverall read latency in ms");
  }

#endif

  int i = 0;
  for (auto& th : workers) {
    if (th.joinable()) {
      th.join();
      printf("joined thread %d\n", i++);
    }
  }

  free(histo_read);
  free(histo_write);
  return 0;
}

