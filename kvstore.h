#ifndef __KVSTORE_H__
#define __KVSTORE_H__

#include "kvinterface.h"

// Base class that define
class KVStore {
 public:
   KVStore() {}

   ~KVStore() {}

   virtual bool ProcessRequest(KVRequest* p) = 0;
};

#endif  // __KVSTORE_H__
