#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <bsd/stdlib.h>
#include <stdio.h>

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>
#include <queue>
#include <vector>
#include <typeinfo>
#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime>
#include <string>

#include "common/common.hpp"
#include "common/util/network_helper.hpp"
#include "engine/engine.hpp"
#include "engine/rocksdb_engine.hpp"
#include "hdr_histogram.h"
#include "kvhandle.hpp"
#include "slice.hpp"

#include "utils.h"

using namespace ardb;

/////////////////////// variables that control benchmark behavior.
char *dbpath = NULL;
int num_threads = 1;
uint64_t init_num_objs = 1000000;
uint64_t total_target_qps = 10000;
double write_ratio = 0;
int runtime_sec = 30;
int timer_interval_sec = 2;
int cmd_queue_size = 4;
int obj_size = 4096;
bool overwrite_all = false;
bool auto_compaction = false;
int init_load_qps = 10000;
char *config_file = NULL;
int target_batch_size = 1;

///////////////////
static uint64_t GetRandom(uint64_t max_val);

#define MAX_KEY_LEN (30)

struct WorkerTaskContext;

struct WorkItem {
  // 1: is write, 0: is read.
  int write;

  char key[MAX_KEY_LEN];
  int key_size;

  void* buffer;
  // size of above buffer.
  uint64_t buffer_size;

  // Actual data size in the buffer.
  uint64_t data_size;

  WorkItem() {
    buffer_size = 0;
    buffer = NULL;
    data_size = 0;
  }

  void dump() {
    INFO_LOG("key=%s (%d), write=%d, buf size=%ld, data size=%ld\n",
             key, key_size, write, buffer_size, data_size);
  }
};

struct WorkQueue {
  // to synchronize access.
  pthread_cond_t pending_cond;
  pthread_mutex_t pending_lock;

  std::atomic<uint64_t> issued_requests;
  std::atomic<uint64_t> pending_requests;
  std::atomic<uint64_t> completed_requests;

  std::atomic<uint64_t> completed_read_size;
  std::atomic<uint64_t> completed_write_size;

  // work items are stored in this queue.
  std::queue<WorkItem> queue;

  // measure cmd queue size, and batched cmd size.
  struct hdr_histogram *queue_size_histo;
  struct hdr_histogram *batch_size_histo;

  uint64_t item_cnt = 0;

  // If this queue has stopped.
  bool stopped;

  int max_queue_size;

  WorkQueue() {
    WorkQueue(8);
  }

  WorkQueue(int qsize) {
    pthread_cond_init(&pending_cond, NULL);
    pthread_mutex_init(&pending_lock, NULL);
    issued_requests = 0;
    completed_requests = 0;
    completed_read_size = 0;
    completed_write_size = 0;
    stopped = false;
    max_queue_size = qsize;
    int lowest = 1;
    int highest = 100;
    int sig_digits = 3;
    hdr_init(lowest, highest, sig_digits, &queue_size_histo);
    hdr_init(lowest, highest, sig_digits, &batch_size_histo);
    printf("work-queue hdr-histogram memory footprint = %ld, qsize histo %p\n",
           hdr_get_memory_size(queue_size_histo), queue_size_histo);
  }

  ~WorkQueue() {
    printf("work-queue free hdr-histograms\n");
    free(queue_size_histo);
    free(batch_size_histo);
  }

  void Stop() { stopped = true; }

  void RecordQueueSize(int v) {
    if (queue_size_histo) {
      hdr_record_value(queue_size_histo, v);
    }
  }

  void RecordBatchSize(int v) {
    if (batch_size_histo) {
      hdr_record_value(batch_size_histo, v);
    }
  }

  void IncIssued() {
    issued_requests++;
  }

  uint64_t GetIssued() { return issued_requests; }

  void IncCompleted() {
    completed_requests++;
  }

  void IncCompleted(int v) {
    completed_requests += v;
  }

  uint64_t GetCompleted() { return completed_requests; }

  void IncCompletedReadSize(int size) {
    completed_read_size += size;
  }

  void IncCompletedWriteSize(int size) {
    completed_write_size += size;
  }

  uint64_t GetCompletedReadSize() { return completed_read_size.load(); }

  uint64_t GetCompletedWriteSize() { return completed_write_size.load(); }

  void Lock() {
    pthread_mutex_lock(&pending_lock);
  }

  void Unlock() {
    pthread_mutex_unlock(&pending_lock);
  }

  void Enqueue(WorkItem& item) {
    Lock();
    // wait if queue overflows.
    while (queue.size() >= max_queue_size) {
      Unlock();
      usleep(100);
      Lock();
    }
    // enqueu
    queue.push(item);
    // update stats.
    issued_requests++;
    pending_requests++;
    // notify other threads.
    item_cnt++;
    if (item_cnt % 2 == 0) {
      pthread_cond_signal(&pending_cond);
    }
    Unlock();
  }

  bool Dequeue(WorkItem* item) {
    Lock();
    while (1) {
      // Grab head of queue if not empty.
      if (queue.size() > 0) {
        RecordQueueSize(queue.size());
        *item = queue.front();
        queue.pop();
        pending_requests--;
        RecordBatchSize(1);
        Unlock();
        return true;
      }
      // exit if we are done.
      if (stopped) {
        //INFO_LOG("exit dequeue without data");
        Unlock();
        return false;
      }
      // If queue is empty, relinqish lock and wait.
      if (queue.size() == 0) {
        struct timespec ts;
        GetAbsTimeInFuture(&ts, 1000);
        pthread_cond_timedwait(&pending_cond, &pending_lock, &ts);
      }
    }
  }

  // Fetch a bunch of available commands from queue, assign to the given task.
  bool Dequeue(std::vector<WorkItem>& items, int target_batch_size) {
    items.clear();
    int batch_size = 0;
    Lock();

    while (1) {
      int write = 0;
      if (queue.size() > 0) {
        // Grab head of queue if not empty.
        RecordQueueSize(queue.size());
        WorkItem item = queue.front();
        queue.pop();
        pending_requests--;
        write = item.write;
        items.push_back(item);
        batch_size++;
        // Grab subsequent requests of the same type.
        while (queue.size() > 0 && batch_size < target_batch_size) {
          item = queue.front();
          if (item.write != write) break;
          queue.pop();
          pending_requests--;
          items.push_back(item);
          batch_size++;
        }
        RecordBatchSize(batch_size);
        Unlock();
        return true;
      }
      // exit if we are done.
      if (stopped) {
        //INFO_LOG("exit dequeue without data");
        Unlock();
        return false;
      }
      // If queue is empty, relinqish lock and wait.
      if (queue.size() == 0) {
        struct timespec ts;
        GetAbsTimeInFuture(&ts, 1000);
        pthread_cond_timedwait(&pending_cond, &pending_lock, &ts);
      }
    }
  }
};

// generic context for a newly-spawned thread.
struct TaskContext {
  // worker thread ID
  int id;

  bool stopped;

  // This task should run this many usec.
  uint64_t runtime_usec;

  // When the task starts.
  uint64_t start_usec;

  // total # of ops done by this task.
  uint64_t total_ops;

  // Target qps by this worker.
  uint64_t target_qps;

  // qps of initial load.
  uint64_t init_load_qps;

  int object_size;

  KvHandle *db;

  WorkQueue *queue;

  std::mutex mtx;
  // replicated downstream rpc servers.
  std::vector<boost::shared_ptr<rocksdb::replication::ReplicationClient> > repl_peers;
  //std::vector<boost::shared_ptr<apache::thrift::transport::TTransport> > transports;

  /////////////////////

  TaskContext() {
    stopped = false;
    total_ops = 0;
  }

  void Stop() {
    stopped = true;
    INFO_LOG("will stop task %d", id);
  }

  void RateLimit(uint64_t targetqps) {
    uint64_t target_time_usec = total_ops * 1000000 / targetqps;
    uint64_t actual_time_usec = NowInUsec() - start_usec;

    if (actual_time_usec < target_time_usec) {
      usleep(target_time_usec - actual_time_usec);
    }
  }
};

// position in latency array.
enum LatencyPos {
  Min = 0,
  P50,
  P90,
  P99,
  P999,
  Max,
  Last,
};

// context for a worker thread. A worker thread keeps grabbing work items from
// a shared queue and process the request.
struct WorkerTaskContext : public TaskContext {
  // Stats of this worker task.
  uint64_t num_reads = 0;
  uint64_t read_bytes = 0;
  uint64_t read_miss = 0;
  uint64_t read_failure = 0;

  uint64_t num_writes = 0;
  uint64_t write_bytes = 0;
  uint64_t write_failure = 0;


  // Worker thread will upload its latencies to this array every timer interval.
  uint64_t write_latency[2][LatencyPos::Last];
  uint64_t read_latency[2][LatencyPos::Last];
  uint64_t last_report_lat_timestamp;
  int current_lat_slot;

  // interval in seconds.
  int timer_interval_sec;
  // semaphores to sync with main thread.
  sem_t sem_begin;
  sem_t sem_end;

  struct hdr_histogram *read_histo;
  struct hdr_histogram *write_histo;
  // try to grab this many cmds in one batch.
  int target_batch_size;

  void ReportLatency() {
    int i = current_lat_slot % 2;
    write_latency[i][Min] = hdr_value_at_percentile(write_histo, 0);
    write_latency[i][P50] = hdr_value_at_percentile(write_histo, 50);
    write_latency[i][P90] = hdr_value_at_percentile(write_histo, 90);
    write_latency[i][P99] = hdr_value_at_percentile(write_histo, 99);
    write_latency[i][P999] = hdr_value_at_percentile(write_histo, 99.9);
    write_latency[i][Max] = hdr_max(write_histo);
    hdr_reset(write_histo);

    read_latency[i][Min] = hdr_value_at_percentile(read_histo, 0);
    read_latency[i][P50] = hdr_value_at_percentile(read_histo, 50);
    read_latency[i][P90] = hdr_value_at_percentile(read_histo, 90);
    read_latency[i][P99] = hdr_value_at_percentile(read_histo, 99);
    read_latency[i][P999] = hdr_value_at_percentile(read_histo, 99.9);
    read_latency[i][Max] = hdr_max(read_histo);
    hdr_reset(read_histo);

    current_lat_slot++;
  }

  uint64_t RetrieveLatency(LatencyPos pos, int write) {
    // Retrieve from the slot before current update slot.
    int i = (current_lat_slot - 1) % 2;
    uint64_t *lats = write ? write_latency[i] : read_latency[i];
    return lats[pos];
  }

  WorkerTaskContext() : TaskContext() {
    sem_init(&sem_begin, 0, 0);
    sem_init(&sem_end, 0, 0);

    memset(write_latency, 0, sizeof(write_latency));
    memset(read_latency, 0, sizeof(read_latency));

    // send new latency into this slot. Retrieve old latency in previous slot.
    current_lat_slot = 1;

    int lowest = 1;
    int highest = 1000000;
    int sig_digits = 3;
    hdr_init(lowest, highest, sig_digits, &read_histo);
    hdr_init(lowest, highest, sig_digits, &write_histo);
    //INFO_LOG("memory footprint of hdr-histogram: %ld\n",
    //         hdr_get_memory_size(read_histo));
  }

  void RecordWriteLatency(uint64_t lat) {
    hdr_record_value(write_histo, lat);
  }

  void RecordReadLatency(uint64_t lat) {
    hdr_record_value(read_histo, lat);
  }

  ~WorkerTaskContext() {
    free(read_histo);
    free(write_histo);
  }

  void dump() {
    INFO_LOG("worker thread %d: read %ld, read bytes %ld, write %ld, write bytes %ld, write p99=%ld",
             id, num_reads, read_bytes, num_writes, write_bytes,
             hdr_value_at_percentile(write_histo, 99));
    //hdr_reset(write_histo);
  }
};

// load-generator task uses this context to generate work load.
struct LoadGeneratorTaskContext : public TaskContext {
  // Init db with these many objs.
  uint64_t init_num_objs;

  // Obj id to create if next op is write.
  uint64_t next_obj_id;


  // ratio of write ops, 0.0 ~ 1.0. Main thread will set this value.
  double write_ratio;

  // Should we overwrite all data before test?
  bool overwrite_all;

  LoadGeneratorTaskContext() : TaskContext() {
    next_obj_id = 0;
  }

  bool ShouldWrite() {
    if (write_ratio == 0) {
      return false;
    }
    uint64_t maxv = 1000000;
    uint64_t thresh = maxv * write_ratio;
    uint64_t v = GetRandom(maxv);
    return v < thresh;
  }
};

// Operation stats.
struct OpStats {
  uint64_t reads = 0;
  uint64_t read_bytes = 0;
  uint64_t read_miss = 0;
  uint64_t read_failure = 0;

  uint64_t writes = 0;
  uint64_t write_bytes = 0;
  uint64_t write_failure = 0;

  OpStats() {}
};

// Timer context struct.
struct TimerContext {
  timer_t *timer;

  int timer_interval_sec;
  // array of task contexts
  //std::vector<TaskContext>* tasks;

  // number of tasks
  int ntasks;

  // cumulated stats of all tasks.
  OpStats  stats;

  // When timer is triggered last time.
  uint64_t last_timestamp;
};

//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////


static std::vector<std::thread> workers;
static std::vector<std::shared_ptr<WorkerTaskContext> > tasks_ctx;


//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////

static uint64_t GetRandom(uint64_t max_val) {
  return std::rand() % max_val;
}

// Producer thread keeps generating work items (with pre-defined r/w ratio),
// and insert items to shared queue.
static void GenerateLoad(LoadGeneratorTaskContext *ctx) {

  INFO_LOG("generator thread %d started", ctx->id);

  // Overwrite entire db with data.
  if (ctx->overwrite_all) {
    ctx->next_obj_id = 0;
    INFO_LOG("will populate %ld objects", ctx->init_num_objs);
    uint64_t t1 = NowInUsec();
    while (ctx->next_obj_id < ctx->init_num_objs) {
      WorkItem item;
      snprintf(item.key, MAX_KEY_LEN, "%020ld", (ctx->next_obj_id++) * ctx->object_size);
      item.key_size = strlen(item.key);
      item.data_size = ctx->object_size;
      item.write = 1;
      ctx->queue->Enqueue(item);
      ctx->total_ops++;
      ctx->RateLimit(ctx->init_load_qps);
    }
    uint64_t t2 = NowInUsec();
    // wait for outstanding rqsts to finish.
    while (ctx->queue->GetCompleted() < ctx->queue->GetIssued()) {
      usleep(100000);
    }
    uint64_t t3 = NowInUsec();
    INFO_LOG("generator init phase: generate work items %ld usec, "
             "wait for init completion %ld usec\n", t2 - t1, t3 - t2);
  }

  ctx->start_usec = NowInUsec();
  ctx->total_ops = 0;

  // now run workload.
  INFO_LOG("will start to run workload for %ld seconds", ctx->runtime_usec / 1000000);
  while (NowInUsec() - ctx->start_usec < ctx->runtime_usec) {
    WorkItem item;

    item.write = ctx->ShouldWrite() ? 1 : 0;

    uint64_t id = GetRandom(ctx->init_num_objs);
    snprintf(item.key, MAX_KEY_LEN, "%020ld", id * ctx->object_size);
    item.key_size = strlen(item.key);
    item.data_size = ctx->object_size;
    ctx->queue->Enqueue(item);

    ctx->total_ops++;
    ctx->RateLimit(ctx->target_qps);
  }

  // wait for outstanding rqsts to finish.
  while (ctx->queue->GetCompleted() < ctx->queue->GetIssued()) {
    usleep(100000);
  }

  INFO_LOG("will stop workload queue");
  ctx->queue->Stop();

  INFO_LOG("generator thread %d exited", ctx->id);
}

// Worker thread: grab work items from queue and exec.
static void DoWork(WorkerTaskContext *ctx) {
  // Parpare batch requests cmds.
  int bufsize = obj_size * ctx->target_batch_size;
  char buf[bufsize];
  char keybuf[MAX_KEY_LEN * ctx->target_batch_size];

  memset(buf, 0xA5, bufsize);
  const char *keys[ctx->target_batch_size];
  int keys_size[ctx->target_batch_size];
  char *values[ctx->target_batch_size];
  int values_buf_size[ctx->target_batch_size];
  int values_size[ctx->target_batch_size];
  int results[ctx->target_batch_size];

  for (int i = 0; i < ctx->target_batch_size; i++) {
    keys[i] = keybuf + MAX_KEY_LEN * i;
    values[i] = buf + obj_size * i;
    values_buf_size[i] = obj_size;
  }

  INFO_LOG("worker thread %d started", ctx->id);

  uint64_t report_interval_usec = ctx->timer_interval_sec * 1000000L;

  ctx->start_usec = NowInUsec();
  ctx->last_report_lat_timestamp = ctx->start_usec;

  vector<WorkItem> work_items;
  work_items.reserve(ctx->target_batch_size);
  while (!ctx->stopped) {
    //WorkItem item;
    //bool ret = ctx->queue->Dequeue(&item);
    // Grab a batch of commands. All cmds are of the same type (read/write).
    work_items.clear();
    bool ret = ctx->queue->Dequeue(work_items, ctx->target_batch_size);
    if (!ret || ctx->stopped) {
      break;
    }
    int write = work_items[0].write;
    int num_items = work_items.size();
    int total_size = 0;
    for (int i = 0; i < num_items; i++) {
      if (work_items[i].write != write) {
        ERROR_LOG("inconsistent request types in batch cmds");
        assert(0);
      }
      memcpy((char*)keys[i], work_items[i].key, work_items[i].key_size);
      keys_size[i] = work_items[i].key_size;
      values_size[i] = work_items[i].data_size;
      total_size += work_items[i].data_size;
    }

    int rv;
    uint64_t t1 = NowInUsec();
    uint64_t t2;
    if (write) {
      // do batch write.
      //rv = ctx->db->WriteStore(item.key, item.key_size, buf, item.data_size);
      rv = ctx->db->BatchWriteStore(keys, keys_size, num_items, values, values_size);
      t2 = NowInUsec();
      if (rv == 0) {
        ctx->queue->IncCompletedWriteSize(total_size);
        ctx->queue->IncCompleted(num_items);
        ctx->num_writes += num_items;
        ctx->write_bytes += total_size;
        ctx->RecordWriteLatency(t2 - t1);
      } else {
        ctx->write_failure += num_items;
      }
      if (t2 - ctx->last_report_lat_timestamp >= report_interval_usec) {
        ctx->ReportLatency();
        ctx->last_report_lat_timestamp = t2;
      }
    } else {
      // do batch read.
      int data_len = 0;
      //rv = ctx->db->ReadStore(item.key, item.key_size, buf, &data_len);
      ctx->db->BatchReadStore(keys, keys_size, num_items, values, values_buf_size,
                              values_size, results);
      t2 = NowInUsec();
      ctx->queue->IncCompleted(num_items);
      for (int i = 0; i < num_items; i++) {
        if (results[i] == 0) {
          assert(values_size[i] == work_items[i].data_size);
          ctx->queue->IncCompletedReadSize(values_size[i]);
          ctx->num_reads++;
          ctx->read_bytes += values_size[i];
        } else if (results[i] == 1) {
          ctx->read_miss++;
        } else {
          ctx->read_failure++;
        }
      }
      ctx->RecordReadLatency(t2 - t1);
      if (t2 - ctx->last_report_lat_timestamp >= report_interval_usec) {
        ctx->ReportLatency();
        ctx->last_report_lat_timestamp = t2;
      }
    }
    ctx->total_ops += num_items;
  }

  INFO_LOG("worker thread %d exited", ctx->id);

}

static void TimerCallback(union sigval sv) {
  static uint64_t cnt = 0;
  TimerContext *tc = (TimerContext*)sv.sival_ptr;

  //vector<TaskContext>& tasks = *tc->tasks;
  int ntasks = tc->ntasks;

  OpStats last_stats;
  memcpy(&last_stats, &tc->stats, sizeof(OpStats));
  memset(&tc->stats, 0, sizeof(OpStats));

  uint64_t t1 = NowInUsec();
  uint64_t elapsed_usecs = t1 - tc->last_timestamp;
  tc->last_timestamp = t1;

  uint64_t wlat_min = 0;
  uint64_t wlat_p50 = 0;
  uint64_t wlat_p90 = 0;
  uint64_t wlat_p99 = 0;
  uint64_t wlat_p999 = 0;
  uint64_t wlat_max = 0;
  uint64_t rlat_min = 0;
  uint64_t rlat_p50 = 0;
  uint64_t rlat_p90 = 0;
  uint64_t rlat_p99 = 0;
  uint64_t rlat_p999 = 0;
  uint64_t rlat_max = 0;
  for (int i = 0; i < tc->ntasks; i++) {
    tc->stats.reads += tasks_ctx[i]->num_reads;
    tc->stats.read_bytes += tasks_ctx[i]->read_bytes;
    tc->stats.read_failure += tasks_ctx[i]->read_failure;
    tc->stats.read_miss += tasks_ctx[i]->read_miss;

    tc->stats.writes += tasks_ctx[i]->num_writes;
    tc->stats.write_bytes  += tasks_ctx[i]->write_bytes;
    tc->stats.write_failure  += tasks_ctx[i]->write_failure;
    //tasks_ctx[i]->dump();

    int write = 1;
    wlat_min = std::min(wlat_min, tasks_ctx[i]->RetrieveLatency(LatencyPos::Min, write));
    wlat_p50 = std::max(wlat_p50, tasks_ctx[i]->RetrieveLatency(LatencyPos::P50, write));
    wlat_p90 = std::max(wlat_p90, tasks_ctx[i]->RetrieveLatency(LatencyPos::P90, write));
    wlat_p99 = std::max(wlat_p99, tasks_ctx[i]->RetrieveLatency(LatencyPos::P99, write));
    wlat_p999 = std::max(wlat_p999, tasks_ctx[i]->RetrieveLatency(LatencyPos::P999, write));
    wlat_max = std::max(wlat_max, tasks_ctx[i]->RetrieveLatency(LatencyPos::Max, write));
    write = 0;
    rlat_min = std::min(rlat_min, tasks_ctx[i]->RetrieveLatency(LatencyPos::Min, write));
    rlat_p50 = std::max(rlat_p99, tasks_ctx[i]->RetrieveLatency(LatencyPos::P50, write));
    rlat_p90 = std::max(rlat_p99, tasks_ctx[i]->RetrieveLatency(LatencyPos::P90, write));
    rlat_p99 = std::max(rlat_p99, tasks_ctx[i]->RetrieveLatency(LatencyPos::P99, write));
    rlat_p999 = std::max(rlat_p999, tasks_ctx[i]->RetrieveLatency(LatencyPos::P999, write));
    rlat_max = std::max(rlat_max, tasks_ctx[i]->RetrieveLatency(LatencyPos::Max, write));
  }

  uint64_t reads = tc->stats.reads - last_stats.reads;
  uint64_t writes = tc->stats.writes - last_stats.writes;
  uint64_t read_bytes = tc->stats.read_bytes - last_stats.read_bytes;
  uint64_t write_bytes = tc->stats.write_bytes - last_stats.write_bytes;

  WorkQueue* queue = tasks_ctx[0]->queue;
  queue->Lock();
  int qsize_min = hdr_min(queue->queue_size_histo);
  int qsize_p50 = hdr_value_at_percentile(queue->queue_size_histo, 50);
  int qsize_p90 = hdr_value_at_percentile(queue->queue_size_histo, 90);
  int qsize_p99 = hdr_value_at_percentile(queue->queue_size_histo, 99);
  int qsize_p999 = hdr_value_at_percentile(queue->queue_size_histo, 99.9);
  int qsize_max = hdr_max(queue->queue_size_histo);
  hdr_reset(queue->queue_size_histo);
  int batch_min = hdr_min(queue->batch_size_histo);
  int batch_p50 = hdr_value_at_percentile(queue->batch_size_histo, 50);
  int batch_p90 = hdr_value_at_percentile(queue->batch_size_histo, 90);
  int batch_p99 = hdr_value_at_percentile(queue->batch_size_histo, 99);
  int batch_p999 = hdr_value_at_percentile(queue->batch_size_histo, 99.9);
  int batch_max = hdr_max(queue->batch_size_histo);
  hdr_reset(queue->batch_size_histo);
  queue->Unlock();

  INFO_LOG("in past %d seconds: %ld reads (%ld failure, %ld miss), %ld writes (%ld failure)\n"
           "      read IOPS %ld, read bw %.3f MB/s, write IOPS %ld, write bw %.3f MB/s\n"
           "      read lat(usec):  min %ld, p50 %ld, p90 %ld, p99 %ld, p999 %ld, max %ld\n"
           "      write lat(usec): min %ld, p50 %ld, p90 %ld, p99 %ld, p999 %ld, max %ld\n"
           "      qsize:      min %d, p50 %d, p90 %d, p99 %d, p999 %d, max %d\n"
           "      batch size: min %d, p50 %d, p90 %d, p99 %d, p999 %d, max %d\n",
           tc->timer_interval_sec,
           reads,
           tc->stats.read_failure - last_stats.read_failure,
           tc->stats.read_miss - last_stats.read_miss,
           writes,
           tc->stats.write_failure - last_stats.write_failure,
           (uint64_t)(reads / (elapsed_usecs / 1000000.0)),
           read_bytes / (elapsed_usecs + 0.0),
           (uint64_t)(writes / (elapsed_usecs / 1000000.0)),
           write_bytes / (elapsed_usecs + 0.0),
           rlat_min, rlat_p50, rlat_p90, rlat_p99, rlat_p999, rlat_max,
           wlat_min, wlat_p50, wlat_p90, wlat_p99, wlat_p999, wlat_max,
           qsize_min, qsize_p50, qsize_p90, qsize_p99, qsize_p999, qsize_max,
           batch_min, batch_p50, batch_p90, batch_p99, batch_p999, batch_max);
}


// Show current glb config.
void ShowConfig() {
  INFO_LOG("===========  Config::");
  INFO_LOG("will open db at dir %s", dbpath);
  //INFO_LOG("db rpc server address %s:%d", rpcip.c_str(), rpcport);
  INFO_LOG("%s do auto compaction", auto_compaction ? "will" : "will NOT");
  INFO_LOG("will run %d worker threads ", num_threads);
  INFO_LOG("will run workload for %d seconds", runtime_sec);
  INFO_LOG("object size %d", obj_size);
  INFO_LOG("cmd queue size %d", cmd_queue_size);
  INFO_LOG("init overwrite = %s", overwrite_all ? "true" : "false");
  INFO_LOG("init load %ld objects", init_num_objs);
  INFO_LOG("init load qps %d", init_load_qps);
  INFO_LOG("target workload qps %d", total_target_qps);
  INFO_LOG("write ratio = %f", write_ratio);
  INFO_LOG("will use config file %s", config_file);
  INFO_LOG("target cmd batch size %d", target_batch_size);
  INFO_LOG("===========\n");
}

void help() {
  printf("Test libardb raw performance, mixed r/w ratio: \n");
  printf("parameters: \n");
  printf("-f <config file>     : config file. Def no config file\n");
  printf("-p <dbpath>          : rocksdb paths. Must provide.\n");
  printf("-t <num of threads>  : number of worker threads to run. Def %d\n", num_threads);
  printf("-s <obj size>        : object size in bytes. Def %d\n", obj_size);
  printf("-n <num of objs>     : Init db with these many objects. Def %ld\n", init_num_objs);
  printf("-i <seconds>         : Run workoad for these many seconds. Default %d\n", runtime_sec);
  printf("-Q <cmd queue size>  : Cmd queue size. Default %d\n", cmd_queue_size);
  printf("-S <QPS>             : init load qps. Def %d\n", init_load_qps);
  printf("-q <QPS>             : Aggregated workload target QPS by all threads. Def %ld\n",
         total_target_qps);
  printf("-w <write ratio>     : write ratio. Def %f\n", write_ratio);
  printf("-b <cmd batch size>  : try to coalesce this many cmds in one batch. Def %d\n",
         target_batch_size);
  printf("-a                   : run auto-compaction, default %s\n",
         overwrite_all ? "true" : "false");
  printf("-o                   : overwrite entire DB before test. Def %s\n\n\n",
         overwrite_all ? "true" : "false");

  printf("******** below not used yet ************\n");

  printf("-c <DB cache>        : DB cache in MB. Def = 5000\n");
  printf("-m <multiget>        : multi-get these number of keys in one get.\n"
         "                       def = 1 key\n");
  printf("-x <key>             : write this key with random value of given size\n");
  printf("-y <key>             : read this key from DB\n");
  printf("-U <address:port>    : upstream RPC address:port\n");
  printf("-D <address:port>    : downstream RPC address:port\n");
  printf("-C <address:port>    : local RPC address:port\n");
  printf("-E <address:port,address:port> : ',' separated list of replicaiton downstream peers\n"
         "                       We will read back from these peers to verify replication success.\n");
  printf("-A                   : download a snapshot from given upstream to given path\n");
  printf("-L <rate limit>      : download snapshot rate limit in MB, default to 50MB/s\n");
  printf("-M                   : after write, ready from remote peers to verify data. Def not.\n");
  printf("-k                   : the path in -p is a checkpoint. Def not\n");
  printf("-l                   : count r/w latency. Def not\n");
  printf("-B                   : when populating data, use bulk load mode (disable WAL). Def not\n");
  printf("-h                   : this message\n");
  printf("-X                   : destroy a checkpoint / db at path\n");
  //printf("-d <shards>          : number of shards. Def = 8\n");
}


int main(int argc, char** argv) {
  if (argc == 1) {
    help();
    return 0;
  }

  int c;

  // Init random number.
  std::srand(NowInUsec());

  while ((c = getopt(argc, argv, "f:p:t:s:n:i:Q:S:q:w:b:aoh")) != EOF) {
    switch(c) {
      case 'h':
        help();
        return 0;
      case 'p':
        dbpath = optarg;
        break;
      case 't':
        num_threads = atoi(optarg);
        break;
      case 's':
        obj_size = atoi(optarg);
        break;
      case 'i':
        runtime_sec = atoi(optarg);
        break;
      case 'n':
        init_num_objs = atoi(optarg);
        break;
      case 'Q':
        cmd_queue_size = atoi(optarg);
        break;
      case 'q':
        total_target_qps = atoi(optarg);
        break;
      case 'S':
        init_load_qps = atoi(optarg);
        break;
      case 'w':
        write_ratio = atof(optarg);
        break;
      case 'a':
        auto_compaction = true;
        break;
      case 'o':
        overwrite_all = true;
        break;
      case 'f':
        config_file = optarg;
        break;
      case 'b':
        target_batch_size = atoi(optarg);
        break;
      default:
        break;
    }
  }

  if (optind < argc) {
    help();
    return 0;
  }


  std::string rpcip;
  int rpcport = 0;
  get_maxspeed_host_ipv4(rpcip);

  // Show config.
  ShowConfig();

  KvHandle handle;
  PrepareDBEnv();
  std::string data_dir(dbpath);
  if (config_file) {
    string cfile(config_file);
    handle.OpenWithConfigFile(data_dir, rpcip, rpcport, cfile);
  } else {
    handle.Open(data_dir, rpcip, rpcport, auto_compaction);
  }

  //WorkQueue queue(cmd_queue_size);
  WorkQueue wqueue(cmd_queue_size);;

  /////////////////////////////
  // Start worker threads.
  for (int i = 0; i < num_threads; i++) {
    std::shared_ptr<WorkerTaskContext> ctx(new WorkerTaskContext());
    ctx->id = i;
    ctx->queue = &wqueue;
    ctx->db = &handle;
    ctx->target_batch_size = target_batch_size;
    ctx->timer_interval_sec = timer_interval_sec;
    tasks_ctx.push_back(ctx);
    workers.push_back(std::thread(DoWork, ctx.get()));
  }

  // start load generator.
  LoadGeneratorTaskContext genload_ctx;
  genload_ctx.init_num_objs = init_num_objs;
  genload_ctx.target_qps = total_target_qps;
  genload_ctx.init_load_qps = init_load_qps;
  genload_ctx.write_ratio = write_ratio;
  genload_ctx.runtime_usec = runtime_sec * 1000000L;
  genload_ctx.queue = &wqueue;
  genload_ctx.db = &handle;
  genload_ctx.object_size = obj_size;
  genload_ctx.id = 0;
  genload_ctx.overwrite_all = overwrite_all;
  std::thread generator(GenerateLoad, &genload_ctx);

  sleep(1);

  /////////////////////////////
  // Create timer.
  timer_t timer;
  TimerContext tctx;
  memset(&tctx, 0, sizeof(tctx));
  //tctx.tasks = &tasks;
  tctx.ntasks = num_threads;
  tctx.timer = &timer;
  tctx.timer_interval_sec = timer_interval_sec;
  tctx.last_timestamp = NowInUsec();
  CreateTimer(&timer, timer_interval_sec * 1000, TimerCallback, &tctx);


  INFO_LOG("wait for generator / worker threads to finish");


  // wait for generator to finish.
  generator.join();
  INFO_LOG("generator completed");

  //////////////////////////////////
  // then, tell workers to finish
  for (int i = 0; i < num_threads; i++) {

    tasks_ctx[i]->Stop();

    if (workers[i].joinable()) {
      workers[i].join();
      INFO_LOG("joined worker thread %d", i);
    }
  }

  ////////////////////////////////
  // stop timer
  INFO_LOG("stop timer");
  DeleteTimer(&timer);

  ////////////////////////////////
  // close db
  INFO_LOG("close db now...");
  handle.Close();

  return 0;
}

