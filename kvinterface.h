#ifndef __KVINTERFACE__
#define __KVINTERFACE__

#include "stdio.h"
#include "stddef.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  GET = 0,
  PUT = 1,
  DELETE = 2,
};

extern const char* KVCmdName[];

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
extern void* OpenDB(char* dbPath, int pathLen);

// Open the DB
extern void CloseDB(void* dbHandler);

// Run the requests, block until the rqsts finished,
extern int KVRunCommand(void* dbHandler, KVRequest* request, int numRequest);

extern void DumpKVRequest(KVRequest* p);

//unsigned long time_microsec();

#ifdef __cplusplus
}
#endif

#endif  // __KVINTERFACE__
