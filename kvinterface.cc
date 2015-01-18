#include "stdio.h"
#include "time.h"
#include "unistd.h"

#include "kvinterface.h"
#include "kvimpl_rocks.h"

const char* KVCmdName[] = {
  "GET",
  "PUT",
  "DELETE"
};
/*unsigned long time_microsec() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000 + t.tv_nsec / 1000;
}*/

void DumpKVRequest(KVRequest* p) {
  if (p->type <= DELETE) {
    printf("KV cmd type = %s\n", KVCmdName[p->type]);
  } else {
    printf("Unknown KV cmd type = %d\n", p->type);
  }
}

void* OpenDB(char* dbPath, int pathLen) {
  RocksDBInterface *rdb = new RocksDBInterface();
  rdb->OpenDB(dbPath, pathLen);
  return (void*)rdb;
}

// Open the DB
void CloseDB(void* dbHandler) {
  delete (RocksDBInterface*)dbHandler;
}

// Run the requests, block until the rqsts finished,
int KVRunCommand(void* dbHandler, KVRequest* request, int numRequest) {
  RocksDBInterface *rdb = (RocksDBInterface*)dbHandler;
  return numRequest;
}
