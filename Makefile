CC = gcc
CXX = g++ -std=c++11
ROCKSDB = /home/shawn/code/rocksdb
CFLAGS = -g -I${ROCKSDB}/include
CXXFLAGS = -g -I${ROCKSDB}/include
LDFLAGS = -lpthread -lrt -lsnappy -lz -lbz2 -lbsd

.PHONY: clean

all: rdbtest kvlib.a

kvlib.a : kvinterface.o kvimpl_rocks.o rocksdb_tuning.o
	ar crvs $@ $^
#$(ROCKSDB)/librocksdb.a

rdbtest : rdbtest.cc kvinterface.cc kvimpl_rocks.cc rocksdb_tuning.cc threadpool.h
	g++ -std=c++11 -g $^ -o$@ -I$(ROCKSDB)/include $(ROCKSDB)/librocksdb.a -lpthread -lrt -lsnappy -lz -lbz2 -lbsd

#.cpp.o:
%.o : %.cpp
	$(CXX) -std=c++11 -g $(CXXFLAGS) -c $< -o $@

#.c.o:
#	$(CC) -g $(CFLAGS) -c $< -o $@ -I../include

clean:
	rm -rf $(all) *.o
