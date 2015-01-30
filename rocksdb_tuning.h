#ifndef __ROCKSDB_TUNNING_H__
#define __ROCKSDB_TUNNING_H__

#include "stdio.h"
#include "stddef.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void TuneUniversalStyleCompaction(rocksdb::Options *options,
                                         int blkCacheMB);

extern void TuneLevelStyleCompaction(rocksdb::Options *options,
                                     int blkCacheMB);


#ifdef __cplusplus
}
#endif

#endif  // __ROCKSDB_TUNNING_H__
