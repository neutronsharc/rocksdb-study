// SSD-Assisted Hybrid Memory.
// Author: Xiangyong Ouyang (neutronsharc@gmail.com)
// Created on: 2011-11-11

#ifndef MYDEBUG_H_
#define MYDEBUG_H_

#include <stdio.h>
#include <assert.h>

#define MYDBG 0

#if MYDBG

#define dbg(fmt, ...)                                                 \
  do {                                                                \
    fprintf(stderr, "%s (%d) " fmt, __func__, __LINE__, ##__VA_ARGS__); \
    fflush(stderr);                                                   \
  } while (0)

#else

#define dbg(fmt, ...)

#endif

#define error(fmt, ...)                        \
  do {                                         \
    fprintf(stderr, "%s (%d) : Error!!  " fmt, \
        __func__, __LINE__, ##__VA_ARGS__);      \
    fflush(stderr);                            \
  } while (0)

#define err(fmt, ...) error(fmt, ##__VA_ARGS__)

#define err_exit(fmt, ...)     \
  do {                         \
    error(fmt, ##__VA_ARGS__);   \
    assert(0);                 \
  } while (0)

#endif  // MYDEBUG_H_
