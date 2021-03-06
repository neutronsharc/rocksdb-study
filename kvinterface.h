#ifndef __KVINTERFACE_H__
#define __KVINTERFACE_H__

#include "stdio.h"
#include "stddef.h"

#ifdef __cplusplus
extern "C" {
#endif

enum {
  GET = 0,
  PUT = 1,
  DELETE = 2,
  GET_NUMBER_RECORDS = 3,
  GET_DATA_SIZE = 4,
  GET_MEMORY_USAGE = 5,
};

// KV request return code
enum {
  SUCCESS = 0,
  FAILURE = 1,
  NOT_EXIST = 2,
  NO_MEM = 3,
};

extern const char* KVCmdName[];

typedef struct KVRequest KVRequest;

struct KVRequest {
  int type;

  // key and length, provided by caller
  const char *key;
  int keylen;

  // Value and length. For "get" these are set by DB.
  char *value;
  int vlen;

  int retcode;

  // Some special purpose data passed by the caller.
  void* reserved;

};

// Open the DB
extern void* OpenDB(const char* dbPath, int numShards, int cacheMB);

// Close the DB
extern void CloseDB(void* dbHandler);

// Run the requests, block until the rqsts finished,
extern int KVRunCommand(void* dbHandler, KVRequest* request, int numRequest);

extern void DumpKVRequest(KVRequest* p);

extern void ReleaseMemory(void* p);

//unsigned long time_microsec();

#ifdef __cplusplus
}
#endif

#endif  // __KVINTERFACE_H__
