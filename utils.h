#ifndef __UTILS_H__
#define __UTILS_H__

#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include <vector>
#include <string>

using namespace std;

// A class to do inter-task sync.
class TaskSync {
 public:
  TaskSync(pthread_t id, void *data) {
    pid = id;
    userdata = data;
    done = false;
    pthread_mutex_init(&lock, NULL);
    pthread_cond_init(&cond, NULL);
  }

  ~TaskSync() {}

  bool IsDone() {
    return done;
  }

  void WaitForCompletion() {
    pthread_mutex_lock(&lock);
    while (!done) {
      pthread_cond_wait(&cond, &lock);
    }
    pthread_mutex_unlock(&lock);
  }

  void Finish() {
    pthread_mutex_lock(&lock);
    done = true;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&lock);
  }

  bool done;
  pthread_mutex_t lock;
  pthread_cond_t  cond;
  pthread_t pid;
  void* userdata;
};

vector<char*> SplitString(char *input, const char *delimiters);
vector<string> SplitString(string input, const char *delimiter);

unsigned long NowInUsec();

unsigned long NowInSec();

// Get current time in seconds since Epoch time.
unsigned long NowInSecSinceEpoch();

uint64_t ExpireTimeToEpochTime(uint64_t expire_time);

uint64_t EpochTimeToExpireTime(uint64_t esec);

// Mark start time of the program.
void MarkStartTime();

uint64_t GetStartTime();

// Get current time in seconds since program starts.
uint64_t NowInSecSinceStart();

size_t KMGToValue(char *strValue);

// Check if "path" is a dir.
bool IsDir(char *path);

// Create a dir if "path" not already exists.
bool CreateDir(char *path);

// Delete a dir. The dir must be empty.
bool DeleteDir(char* path);

// Check if "path" is a regular file.
bool IsFile(char* path);

// Remove a file.
// Return: true on success, false otherwise.
bool DeleteFile(char* path);

// Create file with given "path" as file name, in r/w mode.
// This file with "path" must not exist before calling this method.
//
// Return:
//   file handle if the file doesn't exist before, and is
//   created successfully.
//   -1 on failure.
int CreateFile(char* path);

int64_t GetFileSize(char* path);

void BindThread2CPU(pthread_t tid,
                    vector<int> &allowed_cores,
                    vector<int> &disallowed_cores);

void SetRealTimePriority(pthread_t tid);


int CreateTimer(timer_t *tm,
                int millisec,
                void (*callback)(union sigval),
                void *data);

void DeleteTimer(timer_t *tm);

#endif  // __UTILS_H__
