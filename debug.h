// SSD-Assisted Hybrid Memory.
// Author: Xiangyong Ouyang (neutronsharc@gmail.com)
// Created on: 2011-11-11

#ifndef MYDEBUG_H_
#define MYDEBUG_H_

#include <assert.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

#define MYDBG 1

#define DateTimeString(tmpbuf, buflen)                                \
  do {                                                                \
    time_t rawtime = time(NULL);                                      \
    struct tm *tm = localtime(&rawtime);                              \
    struct timeval tv;                                                \
    gettimeofday(&tv, NULL);                                          \
    size_t len = strftime(tmpbuf, buflen , "%Y", tm);                 \
    sprintf(tmpbuf + len, "-%02d-%02d %02d:%02d:%02d,%03ld",          \
            tm->tm_mon, tm->tm_mday,                                  \
            tm->tm_hour, tm->tm_min, tm->tm_sec,                      \
            tv.tv_usec / 1000);                                       \
  } while (0)

#if MYDBG

#define dbg(fmt, ...)                                                 \
  do {                                                                \
    char timebuf[200];                                                \
    DateTimeString(timebuf, 200);                                     \
    fprintf(stderr, "[%s %s : %s(%d)] DBG: " fmt, timebuf,            \
            __FILE__, __func__, __LINE__, ##__VA_ARGS__);             \
    fflush(stderr);                                                   \
  } while (0)

#define warn(fmt, ...)                                                \
  do {                                                                \
    char timebuf[200];                                                \
    DateTimeString(timebuf, 200);                                     \
    fprintf(stderr, "[%s %s : %s(%d)] WARN: " fmt, timebuf,           \
            __FILE__, __func__, __LINE__, ##__VA_ARGS__);             \
    fflush(stderr);                                                   \
  } while (0)

#else

#define dbg(fmt, ...)
#define warn(fmt, ...)

#endif


#define error(fmt, ...)                                               \
  do {                                                                \
    char timebuf[200];                                                \
    DateTimeString(timebuf, 200);                                     \
    fprintf(stderr, "[%s %s : %s(%d)] ERROR: " fmt, timebuf,          \
            __FILE__, __func__, __LINE__, ##__VA_ARGS__);             \
    fflush(stderr);                                                   \
  } while (0)

#define err(fmt, ...) error(fmt, ##__VA_ARGS__)

#define err_exit(fmt, ...)     \
  do {                         \
    error(fmt, ##__VA_ARGS__)  \
    assert(0);                 \
  } while (0)

#endif  // MYDEBUG_H_
