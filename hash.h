#ifndef HASH_H
#define HASH_H

#include <stddef.h>
#include <stdint.h>

#ifdef    __cplusplus
extern "C" {
#endif

#define ENDIAN_LITTLE 1

uint32_t bobhash(const void *key, size_t length, const uint32_t initval);

#ifdef    __cplusplus
}
#endif

#endif    // HASH_H

