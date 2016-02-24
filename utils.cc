#include <assert.h>
#include <dirent.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <fcntl.h>

#include "utils.h"

using namespace std;

// Start time in seconds, since epoch.
static uint64_t start_time;

// Program start time in seconds, since epoch.
uint64_t start_time_epoch;

void MarkStartTime() {
  start_time_epoch = NowInSecSinceEpoch();
  start_time = NowInSec();
  //start_time = time(0);
}

uint64_t GetStartTime() {
  return start_time;
}

// Convert a TTL time (in seconds, relative to start_time)
// to seconds since epoch.
uint64_t ExpireTimeToEpochTime(uint64_t expire_time) {
  // 0 means no expire time.
  if (expire_time == 0UL) {
    return 0UL;
  }
  return expire_time + start_time_epoch;
}

// Convert a TTL time (in seconds, relative to start_time)
// to seconds since epoch.
uint64_t EpochTimeToExpireTime(uint64_t esec) {
  // 0 means no expire time.
  if (esec == 0UL) {
    return 0UL;
  }
  if (esec <= start_time_epoch) {
    return 1UL;
  } else {
    return esec - start_time_epoch;
  }
}

// Get current time in seconds since program starts.
uint64_t NowInSecSinceStart() {
  uint64_t t = NowInSec();
  return (unsigned int)(t - start_time);
}

// Get current time in seconds since Epoch time.
unsigned long NowInSecSinceEpoch() {
  struct timespec t;
  clock_gettime(CLOCK_REALTIME, &t);
  return t.tv_sec;
}

unsigned long NowInUsec() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

// Get current time in seconds, since some unspecified starting point in the
// past.
unsigned long NowInSec() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec;
}

// Split a string into tokens, separated by chars in the given "delimiters".
vector<char*> SplitString(char *input, const char *delimiters) {
  vector<char*> ss;
  char *pch;

  pch = strtok(input, delimiters);
  while (pch) {
    ss.push_back(pch);
    pch = strtok(NULL, delimiters);
  }
  return ss;
}

// Split a string into tokens, separated by delimiter.
vector<string> SplitString(string input, const char *delimiter) {
  vector<string> ss;
  string token;
  size_t pos = 0;
  size_t begin = 0;

  while ((pos = input.find(delimiter, pos)) != string::npos) {
    token = input.substr(begin, pos - begin);
    ss.push_back(token);
    pos += strlen(delimiter);
    begin = pos;
  }
  ss.push_back(input.substr(begin, input.size() - begin));
  return ss;
}


// Convert a string in form of 123K/M/G to its decimal value.
size_t KMGToValue(char *strValue) {
  char *rptr = NULL;
  size_t len = strlen(strValue);
  size_t val = strtoul(strValue, &rptr, 10);
  if (*rptr == 0) {
    // the input string is like "1345".
  } else if (*(rptr + 1) == 0) {
    // the input string is like "1345K".
    switch (*rptr) {
      case 'k':
      case 'K':
        val *= 1024L;
        break;
      case 'm':
      case 'M':
        val *= (1024L * 1024);
        break;
      case 'g':
      case 'G':
        val *= (1024L * 1024 * 1024);
        break;
      default:
        fprintf(stderr, "invalid input format: %s\n", strValue);
        return 0;
    }
  } else {
    fprintf(stderr, "invalid input format: %s\n", strValue);
    return 0;
  }

  return val;
}

bool IsDir(char *path) {
  if (path == NULL) {
    return false;
  }
  struct stat st;
  if(stat(path, &st) == 0) {
    if(st.st_mode & S_IFDIR) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

bool CreateDir(char *path) {
  int mode = 0775;

  if (!IsDir(path)) {
    if (mkdir(path, mode) == 0) {
      return true;
    } else {
      fprintf(stderr, "failed to mkdir %s\n", path);
    }
  } else {
    fprintf(stderr, "path already exists: %s\n", path);
  }
}

bool DeleteDir(char* path) {
  return rmdir(path) == 0;
}

bool IsFile(char* path) {
  if (path == NULL) {
    return false;
  }
  struct stat st;
  if(stat(path, &st) == 0) {
    if(st.st_mode & S_IFREG) {
      return true;
    } else {
      return false;
    }
  } else {
    return false;
  }
}

int64_t GetFileSize(char* path) {
  if (!IsFile(path)) {
    return -1L;
  }

  struct stat st;
  if(stat(path, &st) == 0) {
    return st.st_size;
  } else {
    return -1;
  }
}


int CreateFile(char* path) {
  int mode = 0666;
  int oflag = O_CREAT | O_RDWR | O_EXCL;
  int fd = open(path, oflag, mode);
  if (fd < 0) {
    fprintf(stderr, "failed to create file %s\n", path);
    perror("failed to create file:");
  }
  return fd;
}

bool DeleteFile(char* path) {
  if (unlink(path) != 0) {
    fprintf(stderr, "failed to unlink file %s\n", path);
    perror("failed to unlink:");
    return false;
  }
  return true;
}

void SetRealTimePriority(pthread_t tid) {
  int policy = SCHED_FIFO;
  struct sched_param params;
  params.sched_priority = sched_get_priority_max(SCHED_FIFO) - 25;

  printf("pthread %ld: will set sched policy %d, priority %d\n",
         tid, SCHED_FIFO, params.sched_priority);

  if (pthread_setschedparam(tid, policy, &params) != 0) {
    perror("fail to set sched_fifo");
    return;
  }
  usleep(1000);

  int ret_policy;
  struct sched_param ret_param;
  if (pthread_getschedparam(tid, &ret_policy, &ret_param) != 0) {
    perror("fail to get sched param");
    return;
  }
  printf("get sched:  policy %d, priority %d\n",
         ret_policy, ret_param.sched_priority);
}

void PrintCPUSet(cpu_set_t &cpuset) {
  for (int i = 0; i < CPU_SETSIZE; i++) {
    if (CPU_ISSET(i, &cpuset)) {
      printf("%d ", i);
    }
  }
}

void BindThread2CPU(pthread_t tid,
                    vector<int> &allowed_cores,
                    vector<int> &disallowed_cores) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);

  int ret;
  ret = pthread_getaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
  if (ret == 0) {
    printf("pid %lu: old cpuset: ", tid);
    PrintCPUSet(cpuset);
    printf("\n");
  }

  if (allowed_cores.size() > 0) {
    // set allowed cores.
    CPU_ZERO(&cpuset);
    for (int i : allowed_cores) {
      if (!CPU_ISSET(i, &cpuset)) {
        CPU_SET(i, &cpuset);
      }
    }
  }
  // disable disallowed cores.
  for (int i : disallowed_cores) {
    if (CPU_ISSET(i, &cpuset)) {
      CPU_CLR(i, &cpuset);
    }
  }
  if (pthread_setaffinity_np(tid, sizeof(cpu_set_t), &cpuset) != 0) {
    perror("failed to set cpu affinity");
  }

  CPU_ZERO(&cpuset);
  ret = pthread_getaffinity_np(tid, sizeof(cpu_set_t), &cpuset);
  if (ret == 0) {
    printf("pid %lu: new cpuset: ", tid);
    PrintCPUSet(cpuset);
    printf("\n");
  }
}

// Create a timer.
int CreateTimer(timer_t *tm,
                int millisec,
                void (*callback)(union sigval),
                void *data) {
  long sec = millisec / 1000;
  long nsec = (millisec % 1000 ) * 1000000;

  struct itimerspec ts;
  struct sigevent se;

  se.sigev_notify = SIGEV_THREAD;
  se.sigev_value.sival_ptr = data;
  se.sigev_notify_function = callback;
  se.sigev_notify_attributes = NULL;

  if (timer_create(CLOCK_REALTIME, &se, tm) < 0) {
    perror("failed to create timer: ");
    return -1;
  }

  ts.it_value.tv_sec = sec;
  ts.it_value.tv_nsec = nsec;
  ts.it_interval.tv_sec = sec;
  ts.it_interval.tv_nsec = nsec;

  if (timer_settime(*tm, 0, &ts, NULL) < 0) {
    perror("failed to set timer: ");
    timer_delete(*tm);
    return -1;
  }

  return 0;
}

void DeleteTimer(timer_t *tm) {
    timer_delete(*tm);
}
