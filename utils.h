#ifndef __UTILS_H__
#define __UTILS_H__

#include <string.h>
#include <time.h>
#include <unistd.h>
#include <vector>

using namespace std;

vector<char*> SplitString(char *input, const char *delimiters);

unsigned long NowInUsec();


#endif  // __UTILS_H__
