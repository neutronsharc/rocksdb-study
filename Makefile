.PHONY: clean

all: rdbtest

rdbtest : rdbtest.cc
	g++ -std=c++11 -g $(CXXFLAGS) $@.cc -o$@ ../rocksdb/librocksdb.a -I../rocksdb/include

.c.o:
	$(CC) -g $(CFLAGS) -c $< -o $@ -I../include

clean:
	rm -rf $(all) *.o
