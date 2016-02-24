#ifndef __MULTI_COMPLETION_H__
#define __MULTI_COMPLETION_H__

#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>

#include "debug.h"

class MultiCompletion {
 public:
  MultiCompletion(int todo) {
    todo_ = todo;;
    finished_ = 0;
    pthread_mutex_init(&mutex_, 0);
    pthread_cond_init(&cond_, 0);
  }

  ~MultiCompletion() {}

  bool Finished() {
    return finished_ == todo_;
  }

  void WaitForCompletion() {
    pthread_mutex_lock(&mutex_);
    while (finished_ < todo_) {
      dbg("finished = %d, todo = %d\n", finished_, todo_);
      pthread_cond_wait(&cond_, &mutex_);
    }
    pthread_mutex_unlock(&mutex_);
  }

  void AddFinish() {
    dbg("add finished\n");
    pthread_mutex_lock(&mutex_);
    finished_++;
    if (finished_ == todo_) {
      pthread_cond_broadcast(&cond_);
    }
    pthread_mutex_unlock(&mutex_);
  }

 private:
  int todo_;
  int finished_;
  pthread_mutex_t mutex_;
  pthread_cond_t cond_;
};

#endif  // __MULTI_COMPLETION_H__
