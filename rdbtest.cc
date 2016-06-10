#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <unistd.h>
#include <bsd/stdlib.h>

#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <typeinfo>
#include <iostream>
#include <fstream>
#include <chrono>
#include <ctime>

#include <boost/archive/text_oarchive.hpp>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TTransport.h>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#include "threadpool.h"
#include "kvinterface.h"
#include "kvimpl_rocks.h"
#include "utils.h"
#include "hdr_histogram.h"
#include "replication/repl_types.h"
#include "replication/repl_util.h"
#include "replication/gen-cpp/replication_types.h"
#include "replication/gen-cpp/Replication.h"

using namespace std;
using namespace std::chrono;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
//using namespace ::apache::thrift::server;

struct TaskContext;

static int ReadbackCompare(char* key, TaskContext* ctx);

static bool ConnectToRPC(const string& addr,
                         int port,
                         boost::shared_ptr<rocksdb::replication::ReplicationClient>& rpccli);

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

  std::mutex mtx_;
  // replicated downstream rpc servers.
  std::vector<boost::shared_ptr<rocksdb::replication::ReplicationClient> > repl_peers;
  //std::vector<boost::shared_ptr<apache::thrift::transport::TTransport> > transports;

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
static bool compact_before_workload = false;

static pthread_mutex_t read_histo_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t write_histo_lock = PTHREAD_MUTEX_INITIALIZER;
static struct hdr_histogram *read_histo = NULL;
static struct hdr_histogram *write_histo = NULL;

static std::string rpc_addr;
static int rpc_port;

static std::string upstream_addr;
static int upstream_port;

static std::string downstream_addr;
static int downstream_port;

static bool use_bulk_load = false;

// After a write completes, should we read from replication peer to verify
// data is replicated?
static bool check_remote = false;

static RocksDBShard shard;

static vector<string> repl_ds_addresses;
static vector<int> repl_ds_ports;

// default download rate limit = 50 MB/s
static uint64_t dl_rate_limit = 1024L * 1024 * 50;

class TestReplWatcher : public rocksdb::replication::ReplWatcher {
 public:
  TestReplWatcher(vector<TaskContext>& tasks) : tasks_(tasks) {}

  virtual void OnDownstreamConnection(const std::string& addr, int port) {
    dbg("downstream %s:%d connection request...\n", addr.c_str(), port);
    for (int i = 0; i < (int)tasks_.size(); i++) {
      boost::shared_ptr<rocksdb::replication::ReplicationClient> rep;
      bool ret = ConnectToRPC(addr, port, rep);
      if (ret) {
        dbg("rdbtest worker %d has connected to remote RPC %s:%d\n", i, addr.c_str(), port);
        std::unique_lock<std::mutex> lk(tasks_[i].mtx_);
        tasks_[i].repl_peers.push_back(rep);
        dbg("rdbtest worker %d has added downstream peer %s:%d\n", i, addr.c_str(), port);
      }
    }
    repl_ds_addresses.push_back(addr);
    repl_ds_ports.push_back(port);
  }

  virtual void OnDownstreamTimeout(const std::string& addr, int port) {
    printf("downstream %s:%d timeout\n", addr.c_str(), port);
  }


 private:
  std::vector<TaskContext>& tasks_;
};


static void TimerCallback(union sigval sv) {
  static uint64_t cnt = 0;
  TimerContext *tc = (TimerContext*)sv.sival_ptr;

  vector<TaskContext>& tasks = *tc->tasks;
  int ntasks = tc->ntasks;

  /////////////////////////
  /// Create a checkpoint
  if (++cnt > 10 ) {
    //string ckpt_path = db_path;
    //ckpt_path.append("/ckpt-").append(std::to_string(cnt));
    //dbg("will create ckpt %ld at %s\n", cnt, ckpt_path.c_str());
    //tasks[0].db->CreateCheckpoint(ckpt_path);
  }

  ///////////////////////
  OpStats last_stats;
  memcpy(&last_stats, &tc->stats, sizeof(OpStats));
  memset(&tc->stats, 0, sizeof(OpStats));

  if (!shard.db_) return;

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

  uint64_t read_min, read_p50, read_p90, read_p99, read_max;
  uint64_t write_min, write_p50, write_p90, write_p99, write_max;
  read_min = hdr_value_at_percentile(read_histo, 0);
  read_p50 = hdr_value_at_percentile(read_histo, 50);
  read_p90 = hdr_value_at_percentile(read_histo, 90);
  read_p99 = hdr_value_at_percentile(read_histo, 99);
  read_max  = hdr_max(read_histo);
  write_min = hdr_value_at_percentile(write_histo, 0);
  write_p50 = hdr_value_at_percentile(write_histo, 50);
  write_p90 = hdr_value_at_percentile(write_histo, 90);
  write_p99 = hdr_value_at_percentile(write_histo, 99);
  write_max = hdr_max(write_histo);
  hdr_reset(read_histo);
  hdr_reset(write_histo);

  printf("%s: proc %d in past %d seconds:  %ld reads (%ld failure, %ld miss), "
         "%ld writes (%ld failure), 99%% read %.2f ms, 99%% write %.2f ms, "
         "latest write # %ld, latest db seq# %ld\n",
         TimestampString().c_str(),
         procid,
         timer_cycle_sec,
         tc->stats.reads - last_stats.reads,
         tc->stats.read_failure - last_stats.read_failure,
         tc->stats.read_miss - last_stats.read_miss,
         tc->stats.writes - last_stats.writes,
         tc->stats.write_failure - last_stats.write_failure,
         read_p99 / 1000.0,
         write_p99 / 1000.0,
         last_obj_id,
         shard.LatestSequenceNumber()
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
        //err("task %d: failed to write key %s (data size %d): %s\n",
        //    task->id, key, objsize, status.ToString().c_str());
        task->write_failure++;
        // don't forgive write failure during init load.
        //assert(0);
      } else {
        task->num_writes++;
        if (check_remote) {
          ReadbackCompare(key, task);
        }
      }
    }
    sem_post(&task->sem_end);
    dbg("Task %d: finished overwriting\n", task->id);
  }

  // obj_id is the next obj key to write.
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
        if (check_remote) {
          ReadbackCompare(key, task);
        }
        task->num_writes++;
        task->write_bytes += objsize;
        obj_id++;
      } else {
        //err("task %d: failed to write key %s (data size %d): %s\n",
        //    task->id, key, objsize, status.ToString().c_str());
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


// Read from a checkpoint.
static void ReadCheckpoint(string ckpt_path, string key_base, uint64_t num_objs) {
  RocksDBShard sd;
  assert(sd.OpenDB(ckpt_path));
  uint64_t sequence = sd.LatestSequenceNumber();

  printf("checkpoint %s, latest seq # %ld\n", ckpt_path.c_str(), sequence);

  uint64_t misses = 0, hits = 0, errors = 0;
  for (int i = 0; i < num_objs; i++) {
    string key = key_base;
    key.append(std::to_string(i));
    string value;
    rocksdb::Status status = sd.Get(key, &value);
    if (status.ok()) {
      hits++;
    } else {
      err("miss/error at key: %s\n", key.c_str());
      misses++;
    }
  }

  printf("checkpoint %s, read %ld objs, hits %ld, misses %ld\n\n",
         ckpt_path.c_str(), num_objs, hits, misses);

}

static bool ConnectToRPC(const string& addr,
                         int port,
                         boost::shared_ptr<rocksdb::replication::ReplicationClient>& rpccli) {
  try {
    boost::shared_ptr<TTransport> socket(new TSocket(addr, port));
    boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
    boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    rpccli.reset(new rocksdb::replication::ReplicationClient(protocol));
    transport->open();
    return true;

  } catch (const apache::thrift::transport::TTransportException& e) {
    printf("****  remote RPC %s:%d: transport failure: %s\n", addr.c_str(), port, e.what());
  } catch (const std::exception& e) {
    printf("****  remote RPC %s:%d: generic failure: %s\n", addr.c_str(), port, e.what());
  }
  return false;
}


static int ReadFromRemote(boost::shared_ptr<rocksdb::replication::ReplicationClient>& rpccli,
                          char* key,
                          string& value) {
  rocksdb::replication::RocksdbOpResponse response;
  string skey(key);
  int rv = -1;
  try {
    rpccli->Get(response, skey);
    if (response.status.code == rocksdb::Status::kOk) {
      value = response.data;
    }
    rv = response.status.code;
  } catch (const apache::thrift::transport::TTransportException& e) {
    dbg("****  Read key %s from remote RPC: transport failure: %s\n", key, e.what());
  } catch (const std::exception& e) {
    dbg("****  Read key %s from remote RPC: generic failure: %s\n", key, e.what());
  }

  return rv;
}


// Read the key back from local db and remote replication peers, compare the results.
//
// Return:
//   0 if at least one peer matches with local data, false otherwise.
//   1 if local read fails
//   2 if no remote peers have the data
//   3 if remote peers data mismtach with local data
static int ReadbackCompare(char* key, TaskContext* ctx) {
  if (ctx->repl_peers.size() == 0) return 0;

  // read from replication peers.
  int rv;
  int remote_mismatch = 0;
  int remote_miss = 0;
  vector<string> remote_data;
  vector<int> remote_result;

  std::unique_lock<std::mutex> lk(ctx->mtx_);
  for (int i = 0; i < ctx->repl_peers.size(); i++) {
    boost::shared_ptr<rocksdb::replication::ReplicationClient>& rpccli = ctx->repl_peers[i];
    string rd;
    rv = ReadFromRemote(rpccli, key, rd);
    remote_result.push_back(rv);
    if (rv == rocksdb::Status::kOk) {
      remote_data.push_back(rd);
    } else {
      //printf("key %s: remote peer %d:  data miss\n", key, i);
      remote_data.push_back("");
    }
  }

  // local read
  string local_data;
  rocksdb::Status status = ctx->db->Get(key, &local_data);
  if (!status.ok()) {
    dbg("key %s: local read failed\n", key);
    return 1;
  }

  // Compare local read and remote read.
  for (int i = 0; i < ctx->repl_peers.size(); i++) {
    if (remote_result[i] == rocksdb::Status::kOk) {
      if (local_data.compare(remote_data[i]) == 0) {
        //printf("key %s: remote peer %d:  data match\n", key, i);
        return 0;
      } else {
        remote_mismatch++;
      }
    } else {
      remote_miss++;
    }
  }

  dbg("key %s: remote peer has no data: miss = %d, mismatch = %d\n",
      key, remote_miss, remote_mismatch);
  if (remote_miss && !remote_mismatch) {
    // remote don't have the data
    return 2;
  } else {
    // remote data diffs
    return 3;
  }
}


// Download a snapshot of remote DB from given addr:port to a local dir.
static void DownloadCheckpoint(const string& addr, int port, const string& local_dir) {

  boost::shared_ptr<rocksdb::replication::ReplicationClient> rpccli;
  bool ret = ConnectToRPC(addr, port, rpccli);
  if (!ret) {
    printf("failed to connect to remote rpc %s:%d\n", addr.c_str(), port);
    return;
  }
  printf("has created remote rpc %s:%d\n", addr.c_str(), port);

  rocksdb::replication::CkptResult ckpt_result;
  rocksdb::replication::HttpServerInfo http_srv;

  string ckpt_name = "ckptxyz";

  try {
    // Create a checkpoint at remote.
    rpccli->CreateCheckpoint(ckpt_result, ckpt_name);
    if (ckpt_result.code != rocksdb::replication::ErrorCode::SUCCESS) {
      err("failed to created remote ckpt %s\n", ckpt_name.c_str());
      return;
    }

    // Tell remote to start http server to serve the checkpoint data.
    rpccli->StartHttpServer(http_srv, ckpt_result.dirname);
    if (http_srv.code != rocksdb::replication::ErrorCode::SUCCESS) {
      err("failed to start remote http server for ckpt %s \n", ckpt_name.c_str());
      return;
    }
    printf("have started remote http server %s:%d to server %s\n",
           http_srv.address.c_str(), http_srv.port, http_srv.root_dir.c_str());

    // Download the ckpt files.
    rocksdb::replication::HttpDownloader dl;

    string remote_url = http_srv.address;
    remote_url.append(":").append(std::to_string(http_srv.port));
    uint64_t rate_limit = dl_rate_limit;

    for (const string& s : ckpt_result.filenames) {
      if (s == "." || s == "..") continue;
      string remote_file = remote_url;
      uint64_t t1 = NowInUsec();
      remote_file.append("/").append(s);
      string local_fname = local_dir;
      local_fname.append("/").append(s);
      printf("will download remote %s to local dir: %s with rate limit %ld\n",
             remote_file.c_str(), local_dir.c_str(), rate_limit);
      bool rv = dl.DownloadFile(remote_file, local_fname, rate_limit);
      uint64_t t2 = NowInUsec() - t1;
      if (rv) {
        std::ifstream is(local_fname, std::ifstream::binary);
        uint64_t fsize = 0;
        if (is) {
          is.seekg(0, is.end);
          fsize = is.tellg();
          is.close();
        }
        printf("\thave downloaded file %s to local %s (%.3f MB in %ld msec), %.3f MB/s \n",
               remote_file.c_str(), local_fname.c_str(),
               fsize / 1000000.0, t2 / 1000, (fsize + 0.0) / t2);
      } else {
        err("failed to download remote file %s\n", remote_file.c_str());
      }
    }

    rpccli->StopHttpServer(ckpt_result.dirname);

  } catch (const apache::thrift::transport::TTransportException& e) {
    dbg("****  remote RPC: transport failure: %s\n", e.what());
  } catch (const std::exception& e) {
    dbg("****  remote RPC: generic failure: %s\n", e.what());
  }

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
  printf("-a                   : run compaction before workload starts. Def not\n");
  printf("-B                   : when populating data, use bulk load mode (disable WAL). Def not\n");
  printf("-o                   : overwrite entire DB before test. Def not\n");
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
  vector<char*> db_paths;
  bool single_read = false, single_write = false;
  string single_read_key, single_write_key;
  vector<char*> sizes;
  std::vector<std::string> ss;
  bool checkpoint = false;
  bool destroy_db = false;
  bool download_test = false;

  while ((c = getopt(argc, argv, "p:s:d:n:t:i:c:q:w:m:x:y:U:D:C:E:L:ohlakXBMA")) != EOF) {
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
      case 'L':
        dl_rate_limit = atoi(optarg) * 1024L * 1024;
        printf("download at rate limit %d MB/s\n", atoi(optarg));
        break;
      case 'a':
        compact_before_workload = true;
        printf("will run major compaction before workload.\n");
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
      case 'k':
        checkpoint = true;
        printf("test with checkpoint.\n");
      case 'A':
        download_test = true;
        printf("will run download test.\n");
        break;
      case 'U':
        SplitString(std::string(optarg), ':', ss);
        upstream_addr = ss[0];
        upstream_port = atoi(ss[1].c_str());
        printf("will connect to upstream %s:%d\n",
               upstream_addr.c_str(), upstream_port);
        break;
      case 'E':
        SplitString(std::string(optarg), ',', ss);
        for (auto& s : ss) {
          printf("\treplication downstream peers: %s\n", s.c_str());
          vector<string> tss;
          SplitString(s, ':', tss);
          assert(tss.size() == 2);
          repl_ds_addresses.push_back(tss[0]);
          repl_ds_ports.push_back(atoi(tss[1].c_str()));
        }
        break;

      case 'C':
        SplitString(std::string(optarg), ':', ss);
        rpc_addr = ss[0];
        rpc_port = atoi(ss[1].c_str());
        printf("will run RPC server at: %s:%d\n", rpc_addr.c_str(), rpc_port);
        break;
      case 'D':
        SplitString(std::string(optarg), ':', ss);
        downstream_addr = ss[0];
        downstream_port = atoi(ss[1].c_str());
        printf("will connect to downstream %s:%d\n",
               downstream_addr.c_str(), downstream_port);
        break;
      case 'B':
        use_bulk_load = true;
        printf("will use bulk-load mode when populating data\n");
        break;
      case 'M':
        check_remote = true;
        printf("will read remote data after write.\n");
        break;
      case 'X':
        destroy_db = true;
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

  if (checkpoint) {
    ReadCheckpoint(db_path, "thr-0-key-", init_num_objs);
    return 0;
  }

  if (download_test) {
    if (db_path.size() == 0 || upstream_addr.size() == 0) {
      err("must provide upstream and dbpath\n");
      return -1;
    }
    DownloadCheckpoint(upstream_addr, upstream_port, db_path);
    printf("have downloaded remote db to local %s\n", db_path.c_str());
  }

  if (destroy_db) {
    printf("will destroy db %s\n", db_path.c_str());
    RocksDBShard::DestroyDB(db_path);
    return 0;
  }


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
    sprintf(tmpbuf, "value1");
    tsize = strlen(tmpbuf);
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
    uint64_t t1 = NowInUsec();
    bool ret;
    if (use_bulk_load) {
      ret = shard.OpenForBulkLoad(db_path);
    } else {
      ret = shard.OpenDB(db_path, db_path);
    }
    if (!ret) {
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

    uint64_t t2 = NowInUsec() - t1;
    double data_mb = init_num_objs * obj_sizes[0] / 1000000.0;
    printf("\nFinished overwrite, has written %.3f MB in %.3f sec, "
           "bw = %.3f MB/s, latest sequence %ld\n\n",
           data_mb,
           t2 / 1000000.0,
           data_mb * 1000000 / t2,
           shard.LatestSequenceNumber());

    /*
    string ckpt_path = db_path;
    ckpt_path.append("/ckpt-").append(std::to_string(0));
    dbg("will create ckpt at %s\n", ckpt_path.c_str());
    shard.CreateCheckpoint(ckpt_path);
    */

    printf("%s: will run compaction after bulkload...\n",
           TimestampString().c_str());
    shard.Compact();
    printf("%s: compaction finished.\n",
           TimestampString().c_str());

    shard.CloseDB();
  }

  ////////////////
  // Phase 2: run r/w workload.
  dbg("open db for workload...\n");
  if (!shard.OpenDB(db_path, db_path)) {
    err("failed to open db before running workload\n");
    return -1;
  }
  printf("db path %s, latest sequence %ld\n", db_path.c_str(), shard.LatestSequenceNumber());

  // Now do a compaction.
  if (compact_before_workload) {
    printf("%s: will run compaction before workload...\n",
           TimestampString().c_str());
    shard.Compact();
    printf("%s: finished compaction before workload\n",
           TimestampString().c_str());
  }

  if (rpc_addr.size() > 0) {
    std::shared_ptr<rocksdb::replication::ReplWatcher> w(new TestReplWatcher(tasks));
    status = shard.InitReplicator(rpc_addr, rpc_port, w);
    dbg("start RPC server, ret = %s\n", status.ToString().c_str());
  }

  if (upstream_addr.size() > 0) {
    status = shard.ConnectUpstream(upstream_addr, upstream_port);
    dbg("connect to upstream, ret = %s\n", status.ToString().c_str());
    //sleep(2);
    //status = shard.ConnectUpstream(upstream_addr, upstream_port);
    //dbg("connect to upstream again, ret = %s\n", status.ToString().c_str());
  }
  if (downstream_addr.size() > 0) {
    status = shard.ConnectDownstream(downstream_addr, downstream_port);
    dbg("connect to downstream, ret = %s\n", status.ToString().c_str());
  }

  printf("\nWill start workload ...\n");
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
      printf("will join thread %d\n", i);
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
  //free(read_histo);
  //free(write_histo);
  printf("db %s sequence = %ld\n", db_path.c_str(), shard.LatestSequenceNumber());
  shard.CloseDB();
  return 0;
}

