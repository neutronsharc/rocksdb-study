#include "stdio.h"
#include "time.h"
#include "unistd.h"

#include "kvinterface.h"
#include "kvimpl_rocks.h"

#include "debug.h"

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
    printf("KV cmd type = %s, key \"%s\", keylen %d\n",
           KVCmdName[p->type], p->key, p->keylen);
  } else {
    printf("Unknown KV cmd type = %d\n", p->type);
  }
}

void* OpenDB(const char* dbPath, int numIOThreads, int cacheMB) {
  RocksDBInterface *rdb = new RocksDBInterface();
  rdb->OpenDB(dbPath, numIOThreads, cacheMB);
  return (void*)rdb;
}

// Open the DB
void CloseDB(void* dbHandler) {
  delete (RocksDBInterface*)dbHandler;
}

// Run the requests, block until the rqsts finished,
int KVRunCommand(void* dbHandler, KVRequest* request, int numRequests) {
  if (numRequests <= 0) {
    err("number of requests = %d\n", numRequests);
    return 0;
  }

  RocksDBInterface *rdb = (RocksDBInterface*)dbHandler;
  if (numRequests == 1) {
    request->reserved = NULL;
    rdb->ProcessRequest(request);
  } else {
    // Check if this is a multi-get.
    bool allGet = true;
    for (int i = 0; i < numRequests; i++) {
      KVRequest *p = request + i;
      p->reserved = NULL;
      if (p->type != GET) {
        allGet = false;
        break;
      }
    }
    if (allGet) {
      rdb->MultiGet(request, numRequests);
    } else {
      MultiCompletion comp(numRequests);
      for (int i = 0; i < numRequests; i++) {
        KVRequest *p = request + i;
        p->reserved = (void*)&comp;
        rdb->PostRequest(p);
        dbg("posted rqst %d\n", i);
      }
      // TODO: wait for these requests to complete.
      comp.WaitForCompletion();
    }
  }
  return numRequests;
}
