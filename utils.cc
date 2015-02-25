
#include "utils.h"

unsigned long NowInUsec() {
  struct timespec t;
  clock_gettime(CLOCK_MONOTONIC, &t);
  return t.tv_sec * 1000000 + t.tv_nsec / 1000;
}

// Split a string into tokens, separated by chars in the given "delimiters".
vector<char*> SplitString(char *input, const char *delimiters) {
  vector<char*> ss;
  char *pch;

  pch = strtok(input, delimiters);
  while (pch) {
    ss.push_back(pch);
    pch = strtok(NULL, delimiters);
  }
  return ss;
}

