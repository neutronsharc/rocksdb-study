#include "stdio.h"
#include "time.h"
#include "unistd.h"

#include "kvinterface.h"
#include "kvimpl_rocks.h"

#include "debug.h"
#include "utils.h"

const char* KVCmdName[] = {
  "GET",
  "PUT",
  "DELETE"
};

void DumpKVRequest(KVRequest* p) {
  if (p->type <= DELETE) {
    printf("KV cmd type = %s, key \"%s\", keylen %d\n",
           KVCmdName[p->type], p->key, p->keylen);
  } else {
    printf("Unknown KV cmd type = %d\n", p->type);
  }
}

void* OpenDB(const char* dbPath, int numShards, int cacheMB) {
  // The dbPath is a "," separated list of dirs where DB is stored,
  char *origPath = new char[strlen(dbPath) + 1];
  strcpy(origPath, dbPath);
  vector<char*> paths = SplitString(origPath, ",");

  printf("Will open DB in %d shards at %d locations\n", numShards, paths.size());
  for (int i = 0; i < paths.size(); i++) {
    printf("\t%s\n", paths[i]);
  }

  int numIOThreads = numShards;
  RocksDBInterface *rdb = new RocksDBInterface();

  // Use Universal-compaction by default.
  rdb->Open((const char**)&paths[0], paths.size(), numShards, numIOThreads, cacheMB);
  delete origPath;
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
    QueuedTask task;
    task.type = SINGLE_REQUEST;
    task.task.request = request;
    rdb->ProcessRequest(&task);
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
      QueuedTask tasks[numRequests];
      for (int i = 0; i < numRequests; i++) {
        KVRequest *p = request + i;
        p->reserved = (void*)&comp;

        tasks[i].type = SINGLE_REQUEST;
        tasks[i].task.request = p;

        rdb->PostRequest(tasks + i);
        dbg("posted rqst %d\n", i);
      }
      // TODO: wait for these requests to complete.
      comp.WaitForCompletion();
    }
  }
  return numRequests;
}

void ReleaseMemory(void* p) {
  free(p);
}
