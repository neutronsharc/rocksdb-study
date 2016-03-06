CC = gcc -g
CXX = g++ -g -std=c++11
ROCKSDB = /home/shawn/code/rocksdb-with-replication
CFLAGS = -g -I${ROCKSDB}/include -I./hdr_histogram
CXXFLAGS = -g -I${ROCKSDB}/include -gdwarf-3 -I./hdr_histogram
LDFLAGS = -L$(ROCKSDB) -lrocksdb_debug -lpthread -lrt -lsnappy -lz -lbz2 -lbsd -lcrypto

.PHONY: clean

objs = kvinterface.o kvimpl_rocks.o rocksdb_tuning.o hash.o utils.o

subdirs = hdr_histogram

all: rdbtest kvlib.a

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
