#include <stdio.h>
#include <string>

#include "engine/engine.hpp"
#include "engine/rocksdb_engine.hpp"
#include "slice.hpp"

using namespace ardb;

int main(int argc, char** argv) {

  ardb::Properties props;
  std::string data_dir = "/tmp/ddd1";
  conf_set(props,"data-dir", data_dir, true);
  INFO_LOG("will open db at dir %s", data_dir.c_str());
  ardb::RocksDBEngineFactory engine_factory(props);
  ardb::RocksDBEngine* rdb =
    (ardb::RocksDBEngine*)engine_factory.CreateDB("", rocksdb::BytewiseComparator());
  INFO_LOG("open db at dir %s return %p\n", data_dir.c_str(), rdb);

  if (rdb == nullptr) {
    return -1;
  }

  ardb::Options options;
  ardb::Slice key1("key1");
  ardb::Slice value1("value 111");
  int ret = rdb->Put(key1, value1, options);
  INFO_LOG("put ret %d\n", ret);

  ardb::Slice key2("key2");
  ardb::Slice value2("value of key 2");
  ret = rdb->Put(key2, value2, options);
  INFO_LOG("put ret %d\n", ret);


  std::string rvalue;
  ret = rdb->Get(key1, &rvalue, options);
  INFO_LOG("get ret: %d, value: \"%s\"\n", ret, rvalue.c_str());

  ardb::Slice key3("key3");

  int num_keys = 3;
  const char* keys[3] = {key1.data(), key2.data(), key3.data()};
  int key_sizes[3] = {(int)key1.size(), (int)key2.size(), (int)key3.size()};
  char* value_bufs[3];
  value_bufs[0] = new char[1000];
  value_bufs[1] = new char[1000];
  value_bufs[2] = new char[1000];
  int value_buf_sizes[3] = {1000, 1000, 1000};
  int ret_data_sizes[3];
  int results[3];

  rdb->MultiGet(keys, key_sizes, num_keys, value_bufs,
                value_buf_sizes, ret_data_sizes, results, options);

  for (int i = 0; i < num_keys; i++) {
    if (results[i] == 0) {
      INFO_LOG("key %s ret value \"%s\" (%d)", keys[i], value_bufs[i], ret_data_sizes[i]);
    } else if (results[i] == 1) {
      INFO_LOG("key %s is a miss", keys[i]);
    } else {
      ERROR_LOG("failed to read key %s", keys[i]);
    }
  }

  delete value_bufs[0];
  delete value_bufs[1];
  delete value_bufs[2];

  delete rdb;
  return 0;
}

