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
  if (blkCacheMB < 128) {
    blkCacheMB = 128;
  }
  options->OptimizeForPointLookup(blkCacheMB);


  // create the DB if it's not already present
  options->create_if_missing = true;

  options->max_open_files = 2000;
  options->allow_os_buffer = true;
  options->write_buffer_size = 1024L * 1024 * 32;
  options->max_write_buffer_number = 8;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 4;

  options->compaction_options_universal.max_size_amplification_percent = 100;
  options->compression = rocksdb::kNoCompression;
  //options->compression = rocksdb::kSnappyCompression;

  //options.disable_auto_compactions = true;
  options->max_background_compactions = 8;
  options->max_background_flushes = 2;

}

void TuneLevelStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  printf("Use level-style compaction\n");

  //options->IncreaseParallelism();

  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options->OptimizeLevelStyleCompaction();

  // Want point query.
  if (blkCacheMB < 128) {
    blkCacheMB = 128;
  }
  options->OptimizeForPointLookup(blkCacheMB);

  // create the DB if it's not already present
  options->create_if_missing = true;
  options->max_open_files = 2000;
  options->allow_os_buffer = true;
  options->write_buffer_size = 1024L * 1024 * 32;
  options->max_write_buffer_number = 8;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 4;

  options->num_levels = 5;
  options->compression = rocksdb::kNoCompression;

  options->max_background_compactions = 8;
  options->max_background_flushes = 2;
}
