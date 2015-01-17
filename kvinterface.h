#ifndef __KVINTERFACE__
#define __KVINTERFACE__

#include "stdio.h"
#include "stddef.h"

enum {
  GET = 0,
  PUT = 1,
  DELETE = 2,
};

static const char* CmdName[] = {
  "GET",
  "PUT",
  "DELETE"
};

typedef struct KVRequest KVRequest;

struct KVRequest {
  int type;

  // key and length, provided by caller
  char *key;
  int keylen;

  // Value and length. For "get" these are set by DB.
  char *value;
  size_t vlen;

  // Some special purpose data passed by the caller.
  void* userdata;

};

// Open the DB
void* OpenDB(char* dbPath, int pathLen);

// Open the DB
void CloseDB(void* dbHandler);

// Run the requests, block until the rqsts finished,
int KVRunCommand(void* dbHandler, KVRequest* request, int numRequest);

void DumpKVRequest(KVRequest* p) {
  if (p->type <= DELETE) {
    printf("KV cmd type = %s\n", CmdName[p->type]);
  } else {
    printf("Unknown KV cmd type = %d\n", p->type);
  }
}

#endif  // __KVINTERFACE__
