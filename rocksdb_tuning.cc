#include <time.h>
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <mutex>
#include <string>
#include <thread>

#include "debug.h"
#include "rocksdb_tuning.h"


void TuneUniversalStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  // Set num of threads in Low/High thread pools.
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(16, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);

  options->IncreaseParallelism();

  options->OptimizeUniversalStyleCompaction();

  // Want point query.
  options->OptimizeForPointLookup(blkCacheMB);

  // create the DB if it's not already present
  options->create_if_missing = true;
  options->max_open_files = 4096;
  options->allow_os_buffer = true;
  options->write_buffer_size = 1024L * 1024 * 128;
  options->max_write_buffer_number = 16;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 8;

  //options.compression = rocksdb::kNoCompression;
  options->compression = rocksdb::kSnappyCompression;

  //options.disable_auto_compactions = true;
  //options.max_background_compactions = 16;
  options->max_background_flushes = 2;

  options->env = env;
}

void TuneLevelStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  // Set num of threads in Low/High thread pools.
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(16, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);

  options->IncreaseParallelism();

  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options->OptimizeLevelStyleCompaction();
  //options->compression = rocksdb::kNoCompression;

  // Want point query.
  options->OptimizeForPointLookup(blkCacheMB);

  // create the DB if it's not already present
  options->create_if_missing = true;
  options->max_open_files = 4096;
  options->allow_os_buffer = true;
  options->write_buffer_size = 1024L * 1024 * 128;
  options->max_write_buffer_number = 16;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 4;

  //options.max_background_compactions = 16;
  options->max_background_flushes = 2;

  options->env = env;
}
