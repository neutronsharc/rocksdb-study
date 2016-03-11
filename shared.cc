
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>

#include <atomic>
#include <map>
#include <string>
#include <thread>
#include <sys/time.h>
#include <mutex>
#include <queue>
#include <condition_variable>

using namespace std;


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
  explicit Worker(int id) {
    id_ = id;
    pthread_mutex_init(&lock_, NULL);
    pthread_cond_init(&cv_, NULL);
    running_ = true;
    Start();
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

      std::shared_ptr<Data> d = queue_.front();
      queue_.pop();
      Unlock();
      printf("worker %d: processing data %d\n", id_, d->id_);
    }
  }

  void Start() {
    thread_ = std::thread(&Worker::Work, this);
  }

  void Stop() {
    if (!running_) return;

    while (queue_.size() > 0) {
      usleep(10000);
    }
    Lock();
    running_ = false;
    pthread_cond_signal(&cv_);
    Unlock();
    thread_.join();
  }

  void AddData(std::shared_ptr<Data>& d) {
    Lock();
    queue_.push(d);
    pthread_cond_signal(&cv_);
    Unlock();
  }

 protected:

  int running_;
  int id_;
  pthread_mutex_t lock_;
  pthread_cond_t cv_;
  std::thread thread_;

  // Producer will put data to be replicated into this queue.
  // This thread will pop data from the queue and replicate it to downstream.
  std::queue<std::shared_ptr<Data> > queue_;
};

Worker w1(1);
Worker w2(2);

void test(int did) {
  std::shared_ptr<Data> d(new Data(did));
  w1.AddData(d);
  w2.AddData(d);
}

// to build: g++ -std=c++11 -pthread ./shared.cc  -g
int main(int argc, char** argv) {
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
