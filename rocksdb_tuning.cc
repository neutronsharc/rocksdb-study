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

#include "rocksdb/table.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"

#include "debug.h"
#include "rocksdb_tuning.h"


// memory budget for each DB shard.
static size_t write_buffer_size = 16L * 1024 * 1024;
static int max_write_buffer_number = 4;
static size_t min_block_cache_size_MB = 160;
// Whether to put idx/filter blocks in block cache. This is to contain memory
// consumption.
static bool cache_index_and_filter_blocks = true;

static void TunePointLookup(rocksdb::Options *options, int blkCacheMB) {
  options->prefix_extractor.reset(rocksdb::NewNoopTransform());
  rocksdb::BlockBasedTableOptions block_based_options;

  block_based_options.index_type = rocksdb::BlockBasedTableOptions::kHashSearch;

  block_based_options.cache_index_and_filter_blocks = false;

  block_based_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));

  if (blkCacheMB < min_block_cache_size_MB) {
    blkCacheMB = min_block_cache_size_MB;
  }
  block_based_options.block_cache =
      rocksdb::NewLRUCache(static_cast<size_t>(blkCacheMB * 1024 * 1024));

  options->table_factory.reset(rocksdb::NewBlockBasedTableFactory(block_based_options));

  options->memtable_factory.reset(rocksdb::NewHashLinkListRepFactory());
}

void TuneUniversalStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  printf("Use universal-style compaction\n");

  //options->IncreaseParallelism();
  options->OptimizeUniversalStyleCompaction();

  options->OptimizeForPointLookup(blkCacheMB);
  // Fine tune some parameters for point query.
  TunePointLookup(options, blkCacheMB);

  // create the DB if it's not already present
  options->create_if_missing = true;

  options->max_open_files = 2000;
  options->allow_os_buffer = true;
  options->write_buffer_size = write_buffer_size;
  options->max_write_buffer_number = max_write_buffer_number;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 4;

  options->compaction_options_universal.max_size_amplification_percent = 100;
  options->compression = rocksdb::kNoCompression;
  //options->compression = rocksdb::kSnappyCompression;

  //options.disable_auto_compactions = true;
  options->max_background_compactions = 8;
  options->max_background_flushes = 1;
}

void TuneLevelStyleCompaction(rocksdb::Options *options, int blkCacheMB) {
  printf("Use level-style compaction\n");

  //options->IncreaseParallelism();

  // optimize level compaction: also set up per-level compression: 0,0,1,1,1,1,1
  //   def to 512 MB memtable
  options->OptimizeLevelStyleCompaction();

  options->OptimizeForPointLookup(blkCacheMB);
  // Fine tune some parameters for point query.
  TunePointLookup(options, blkCacheMB);

  // create the DB if it's not already present
  options->create_if_missing = true;

  options->max_open_files = 2000;
  options->allow_os_buffer = true;
  options->write_buffer_size = write_buffer_size;
  options->max_write_buffer_number = max_write_buffer_number;

  options->min_write_buffer_number_to_merge = 2;
  options->level0_file_num_compaction_trigger = 4;

  options->num_levels = 5;
  options->compression = rocksdb::kNoCompression;

  options->max_background_compactions = 8;
  options->max_background_flushes = 1;
}
