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
  printf("Use universal-style compaction\n");

  //options->IncreaseParallelism();

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

  //options->compression = rocksdb::kSnappyCompression;
  options->compaction_options_universal.max_size_amplification_percent = 20;
  options->compression = rocksdb::kNoCompression;

  //options.disable_auto_compactions = true;

  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(16, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);
  options->max_background_compactions = 16;
  options->max_background_flushes = 2;

  // Set num of threads in Low/High thread pools.
  options->env = env;
}

void TuneLevelStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  printf("Use level-style compaction\n");

  //options->IncreaseParallelism();

  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options->OptimizeLevelStyleCompaction();

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

  options->num_levels = 2;
  options->compression = rocksdb::kNoCompression;

  // Set num of threads in Low/High thread pools.
  rocksdb::Env *env = rocksdb::Env::Default();
  env->SetBackgroundThreads(16, rocksdb::Env::Priority::LOW);
  env->SetBackgroundThreads(2, rocksdb::Env::Priority::HIGH);
  options->max_background_compactions = 16;
  options->max_background_flushes = 2;

  options->env = env;
}
