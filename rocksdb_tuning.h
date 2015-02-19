#ifndef __ROCKSDB_TUNNING_H__
#define __ROCKSDB_TUNNING_H__

#include "stdio.h"
#include "stddef.h"

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

extern void TuneUniversalStyleCompaction(rocksdb::Options *options,
                                         int blkCacheMB);

extern void TuneLevelStyleCompaction(rocksdb::Options *options,
                                     int blkCacheMB);


#endif  // __ROCKSDB_TUNNING_H__
