
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include <atomic>
#include <map>
#include <unordered_map>
#include <string>
#include <thread>
#include <sys/time.h>
#include <mutex>
#include <queue>
#include <condition_variable>

//#include "boost/lockfree/spsc_queue.hpp"
#include "readerwriterqueue.h"

using namespace std;


/*
template<typename T>
class LFQueue : public boost::lockfree::spsc_queue<T> {

 public:
  LFQueue(int size) :
    boost::lockfree::spsc_queue<T, boost::lockfree::capacity<size> > {
  }

}; */

// A piece of data to be replicated to peers.
class Data {
 public:
  explicit Data(int id) {
    printf("--   create data %d\n", id);
    id_ = id;
  }

  ~Data() {
    printf("~ destruct data %d\n", id_);
  }

  int id_;
};

// A helper thread that works as RPC client to handle replication-related tasks.
//
// Each repl-thread handles one upstream or one downstream peer.
class Worker {
 public:
  explicit Worker(int id, bool lockfree) {
    id_ = id;
    pthread_mutex_init(&lock_, NULL);
    pthread_cond_init(&cv_, NULL);
    running_ = true;

    //lfqueue_.reset(new boost::lockfree::spsc_queue<std::shared_ptr<Data> >(1000));

    use_lockfree_ = lockfree;
    lfqueue_.reset(new moodycamel::ReaderWriterQueue<std::shared_ptr<Data> >(1000));
    std::atomic_store(&elem_count_, 0);

    if (use_lockfree_) {
      StartLockfree();
    } else {
      Start();
    }
    printf("worker %d started...\n", id_);
  }

  virtual ~Worker() {
    Stop();
    printf("Worker %d dtor\n", id_);
  }

  virtual void Lock() {
    pthread_mutex_lock(&lock_);
  }

  virtual void Unlock() {
    pthread_mutex_unlock(&lock_);
  }

  void Work() {
    while (running_) {
      Lock();
      while (queue_.size() == 0 && running_) {
        pthread_cond_wait(&cv_, &lock_);
      }

      if (!running_) break;

      int rtry = 0;
      while (rtry < 5) {
        rtry++;
        std::shared_ptr<Data>& d = queue_.front();
        Unlock();
        printf("worker %d: processing data %d try %d\n", id_, d->id_, rtry);
        usleep(500);
        Lock();
      }
      queue_.pop();
      Unlock();
    }
  }

  void Start() {
    printf("worker %d: use lock-based\n", id_);
    thread_ = std::thread(&Worker::Work, this);
  }

  void StartLockfree() {
    printf("worker %d: use lock-free\n", id_);
    thread_ = std::thread(&Worker::WorkLockfree, this);
  }

  void Stop() {
    if (!running_) return;

    if (!use_lockfree_) {
      while (queue_.size() > 0) {
        usleep(10000);
      }
    }
    //Lock();
    running_ = false;
    pthread_cond_signal(&cv_);
    //Unlock();
    thread_.join();
  }

  void AddData(std::shared_ptr<Data>& d) {
    Lock();
    queue_.push(d);
    pthread_cond_signal(&cv_);
    Unlock();
  }


  void WorkLockfree() {
    //bool has_data = false;
    struct timespec ts;
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    while (running_) {
      std::shared_ptr<Data>* d = lfqueue_->peek();
      if (d) {
        // process data.
        printf("worker %d: process data %d\n", id_, (*d)->id_);
        lfqueue_->pop();
        elem_count_.fetch_sub(1);
      } else {
        Lock();
        pthread_cond_timedwait(&cv_, &lock_, &ts);
        Unlock();
      }

      if (!running_) break;

    }
  }

  void AddDataLockfree(std::shared_ptr<Data>& d) {
    int existing = std::atomic_fetch_add(&elem_count_, 1);
    bool ret = lfqueue_->enqueue(d);
    if (existing  == 0) {
      pthread_cond_signal(&cv_);
    }
    printf("worker %d: add data lockfree %s\n",
           id_, ret ? "success" : "fail");
  }

 //protected:

  int running_;
  int id_;
  pthread_mutex_t lock_;
  pthread_cond_t cv_;
  std::thread thread_;

  bool use_lockfree_;

  // Producer will put data to be replicated into this queue.
  // This thread will pop data from the queue and replicate it to downstream.
  std::queue<std::shared_ptr<Data> > queue_;

  //boost::lockfree::spsc_queue<std::shared_ptr<Data>, boost::lockfree::capacity<5> > lfqueue_;
  //std::unique_ptr<boost::lockfree::spsc_queue<std::shared_ptr<Data> > > lfqueue_;
  std::unique_ptr<moodycamel::ReaderWriterQueue<std::shared_ptr<Data> > > lfqueue_;

  std::atomic<int> elem_count_;

};

Worker w1(1, true);
Worker w2(2, true);

void test(int did) {
  std::shared_ptr<Data> d(new Data(did));
  //w1.AddData(d);
  //w2.AddData(d);
  w1.AddDataLockfree(d);
  w2.AddDataLockfree(d);
}

std::unordered_map<int, std::shared_ptr<Worker> > wmap;

void Test2() {
  // test ref to unique_ptr
  {
    std::shared_ptr<Worker> w1(new Worker(3, true));;

    std::shared_ptr<Worker>& ref_w1 = w1;

    printf("1: w1 id %d\n", w1->id_);
    printf("2: w1 ref id %d\n", ref_w1->id_);
    //wmap[3] = std::move(w1);
    wmap[3] = w1;
    printf("3: w1 ref id %d\n", ref_w1->id_);
    printf("4: w1 id %d\n", w1->id_);
  }


  {
    auto it = wmap.find(3);
    if (it != wmap.end()) {
      std::shared_ptr<Worker> tw = std::move(it->second);
      wmap.erase(it);
    }
  }

  sleep(1);
  printf("test 2 returned...\n");

}

// to build: g++ -std=c++11 -pthread ./shared.cc  -g -lboost_system
int main(int argc, char** argv) {
  Test2();
  printf("after teset 2\n");
  sleep(1);
  printf("exit now\n");
  return 0;
  test(11);
  test(12);
  test(13);
  test(14);
  test(15);
  test(16);
  test(17);
  test(18);
  test(19);

  return 0;
}
