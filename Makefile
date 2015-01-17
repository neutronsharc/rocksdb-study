
ROCKSDB = /home/shawn/code/rocksdb

.PHONY: clean

all: rdbtest

rdbtest : rdbtest.cc threadpool.h kvinterface.h  kvinterface.cc
	g++ -std=c++11 -g $^ -o$@ -I$(ROCKSDB)/include $(ROCKSDB)/librocksdb.a -lpthread -lrt -lsnappy -lz -lbz2

.c.o:
	$(CC) -g $(CFLAGS) -c $< -o $@ -I../include

clean:
	rm -rf $(all) *.o
