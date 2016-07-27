CC = gcc -g
CXX = g++ -g -std=c++11
#ROCKSDB = ../rocksdb-with-replication
ROCKSDB = ../ardb/deps/rocksdb-with-replication
#ROCKSDB = /home/ceph/code/rocksdb-with-replication
INCLUDES = -I. -I../ardb/src -I../ardb/src/common -I$(ROCKSDB) -I$(ROCKSDB)/include -I./hdr_histogram

LIBS = ../ardb/src/libardb.a -L../ardb/deps/rocksdb-with-replication -lrocksdb

CFLAGS = -g $(INCLUDES)

CXXFLAGS = -g $(INCLUDES) -gdwarf-3

LDFLAGS = $(LIBS) -lpthread -lrt -lsnappy -lz -lbz2 -lbsd -lcrypto
LDFLAGS += -lthrift -lboost_system -lboost_filesystem -lcurl

.PHONY: clean

objs = kvinterface.o kvimpl_rocks.o rocksdb_tuning.o hash.o utils.o

subdirs = hdr_histogram

all: testlibardb
#	rdbtest
#kvlib.a

testlibardb : testlibardb.o utils.o hash.o
	g++ -g $^ hdr_histogram/lib_hdr_histogram.a -o$@ $(LDFLAGS)

kvlib.a : $(objs) libhdrhistogram
	ar crvs $@ $(objs)  hdr_histogram/lib_hdr_histogram.a

rdbtest : rdbtest.o kvlib.a libhdrhistogram
	g++ -std=c++11 -g rdbtest.o kvlib.a hdr_histogram/lib_hdr_histogram.a -o$@ $(LDFLAGS)

libhdrhistogram : force_look
	cd hdr_histogram; $(MAKE) $(MFLAGS)

%.o : %.cpp
	$(CXX) -std=c++11 $(CXXFLAGS) -c $< -o $@

%.o : %.c
	$(CC) -std=gnu99 $(CFLAGS) -c $< -o $@

force_look :
	true

clean:
	rm -rf $(all) *.o
	-for d in $(subdirs); do (cd $$d; $(MAKE) clean); done
