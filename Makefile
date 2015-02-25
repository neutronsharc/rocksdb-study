CC = gcc -g
CXX = g++ -g -std=c++11
ROCKSDB = /home/shawn/code/rocksdb
CFLAGS = -g -I${ROCKSDB}/include
CXXFLAGS = -g -I${ROCKSDB}/include -gdwarf-3
LDFLAGS = -lpthread -lrt -lsnappy -lz -lbz2 -lbsd

.PHONY: clean

all: rdbtest kvlib.a

kvlib.a : kvinterface.o kvimpl_rocks.o rocksdb_tuning.o hash.o utils.o
	ar crvs $@ $^
#$(ROCKSDB)/librocksdb.a

rdbtest : rdbtest.o kvinterface.o kvimpl_rocks.o rocksdb_tuning.o hash.o utils.o
	g++ -std=c++11 -g $^ -o$@ -I$(ROCKSDB)/include $(ROCKSDB)/librocksdb.a -lpthread -lrt -lsnappy -lz -lbz2 -lbsd

#.cpp.o:
%.o : %.cpp
	$(CXX) -std=c++11 $(CXXFLAGS) -c $< -o $@

%.o : %.c
	$(CC) -std=gnu99 $(CFLAGS) -c $< -o $@

#.c.o:
#	$(CC) -g $(CFLAGS) -c $< -o $@ -I../include

clean:
	rm -rf $(all) *.o
