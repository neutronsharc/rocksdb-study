#ifndef __THREAD_POOL__
#define __THREAD_POOL__

#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <malloc.h>
#include <stdlib.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include "kvinterface.h"
#include "kvstore.h"

using namespace std;

class KVStore;

// Function that handles an item from the work queue.
typedef void (*DataProcessor)(void*);

// Base class for Tasks
// run() should be overloaded and expensive calculations done there.
// showTask() is for debugging and can be deleted if not used
class Task {
 public:
  Task() {}
  virtual ~Task() {}
  virtual void run()=0;
  virtual void showTask()=0;
};


// Wrapper around std::queue with some mutex protection
class WorkQueue {
 public:
  WorkQueue() : finished_(false) {
    pthread_mutex_init(&mutex_, 0);
    pthread_cond_init(&cond_, 0);
  }

  ~WorkQueue() {
    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&cond_);
  }

  // Add work to the queue.
  void AddWork(void *data) {
     pthread_mutex_lock(&mutex_);
     if (!finished_) {
       tasks_.push(data);
       pthread_cond_signal(&cond_);
     }
     pthread_mutex_unlock(&mutex_);
  }

  // Get the next task in the queue.
  // Block until there is a task avail.
  // Return:
  //   the next task,
  //   or NULL only if the queue is torn down, and no more data avail.
  void* GetNext() {
    void *data = NULL;

     pthread_mutex_lock(&mutex_);

     while (!finished_ || tasks_.size() > 0) {
       if (tasks_.size() > 0) {
         data = tasks_.front();
         tasks_.pop();
         break;
       } else {
         pthread_cond_wait(&cond_, &mutex_);
       }
     }

     pthread_mutex_unlock(&mutex_);
     return data;
  }

  // Mark the queue finished
  void Finish() {
    pthread_mutex_lock(&mutex_);
    finished_ = true;
    // Signal all waiting threads.
    pthread_cond_broadcast(&cond_);
    pthread_mutex_unlock(&mutex_);
  }

  bool HasWork() {
    return (tasks_.size() > 0);
  }

 private:
  std::queue<void*> tasks_;
  bool finished_;
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
};

static void GetWork(void *p, int id, KVStore* kvstore) {
  void *data;
  WorkQueue *wq = (WorkQueue*)p;
  printf("thread %d started, kvstore = %p...\n", id, kvstore);
  while (data = wq->GetNext()) {
    if (kvstore) {
      kvstore->ProcessRequest((KVRequest*)data);
    }
  }
  printf("thread %d stopped...\n", id);
}


class ThreadPool {
 public:
  ThreadPool(int n, KVStore* kvstore) : numberThreads_(n), kvstore_(kvstore) {
    threads_ = new std::thread[n];
    for (int i = 0; i < n; i++) {
      threads_[i] = std::thread(GetWork, &workQueue_, i, kvstore);
      printf("created thread %d\n", i);
    }
  }

  ~ThreadPool() {
    workQueue_.Finish();
    for (int i = 0; i < numberThreads_; i++) {
      if (threads_[i].joinable()) {
        threads_[i].join();
      }
    }
    delete [] threads_;
  }


  void AddWork(void *data) {
    workQueue_.AddWork(data);
  }

  // Tell the tasks to finish and return
  void Finish() {
    workQueue_.Finish();
  }

  // Checks if there is work to do
  bool HasWork() {
    return workQueue_.HasWork();
  }

  void SetKVStore(KVStore* kvstore) {
    kvstore_ = kvstore;
  }

 private:
  std::thread *threads_;
  int numberThreads_;
  WorkQueue workQueue_;
  // TODO: define a function to process each piece of data.
  KVStore *kvstore_;

  DataProcessor processor_;


};

#endif  // __THREAD_POOL__
