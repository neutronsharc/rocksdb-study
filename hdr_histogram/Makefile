
GCC = gcc
GXX = g++ -std=c++11

CFLAGS := -g -I. -I/usr/include
CXXFLAGS := -g -I. -I/usr/include
LDFLAGS := -lrt -lpthread

objs = hdr_encoding.o hdr_histogram.o hdr_histogram_log.o \
       hdr_interval_recorder.o hdr_thread.o hdr_time.o    \
       hdr_writer_reader_phaser.o

HistogramLib = lib_hdr_histogram.a

.PHONY : all clean

all : $(HistogramLib)

$(HistogramLib) : $(objs)
	ar rcs $@ $^

%.o : %.c
	$(GCC) $(CFLAGS) -c $< -o $@

clean:
	$(RM) *.o $(objs) $(HistogramLib)
