#include "stdio.h"
#include "time.h"
#include "unistd.h"

#include "kvinterface.h"

static const char* KVCmdName[] = {
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

