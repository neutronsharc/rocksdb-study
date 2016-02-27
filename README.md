# rocksdb-study
study rocks db performance


# RocksDB performance

The workflow is:  open db => bulk load => close => open db => trigger a compaction
=> run workload.

Test setup:

8 threads to issue r/w requests in parallel. 64GB memory, 2 X Intel CPU, 24
cores, 1 SSD.

## 100% read (data in memory)

```
./rdbtest -p /data/nvme0/rdbtest/ -n 5000000 -t 8 -q 1000000 -l -i 30 -s 4000


In total:  26416312 ops in 30.003713 sec (26416312 read, 0 write).
Total IOPS = 880435, read IOPS 880435, write IOPS 0
Bandwidth = 3788.373 MB/s, read bw 3521.739 MB/s, write bw 266.634 MB/s
Read miss 0 (0.00%), read failure 0, write failure 0

============== Read latency in ms
min    10 %     20 %    50 %     90 %        95 %    99 %      99.9 %         max
0.004  0.005   0.005    0.006   0.01 k     0.01       0.011       0.018       2.843
```

## 99% read, 1% write (data in memory)

```
./rdbtest -p /data/nvme0/rdbtest/ -n 2000000 -t 8 -q 800000 -l  -w 0.01  -i 200 -s 4000


In total:  108359949 ops in 200.000845 sec (107277341 read, 1082608 write).
Total IOPS = 541797, read IOPS 536384, write IOPS 5413
Bandwidth = 2167.190 MB/s, read bw 2145.538 MB/s, write bw 21.652 MB/s
Read miss 0 (0.00%), read failure 0, write failure 0

============== Read latency in ms
min   10 %   20 %    50 %        90 %    95 %      99 %      99.9 %         max
0.001  0.008   0.008   0.011    0.017   0.019     0.024       0.033     231.423

============== Write latency in ms
min   10 %   20 %    50 %    90 %    95 %   99 %      99.9 %         max
0.005  0.015 0.016  0.021  0.027    0.03   0.04       0.514         231.167

```

## 99% read, 1% write (most data in disk)


```
./rdbtest -p /data/nvme0/rdbtest/ -n 30000000 -t 8 -q 800000 -l -w 0.01 -i 300 -s 4000


In total:  7993632 ops in 300.007355 sec (7913382 read, 80250 write).
Total IOPS = 26645, read IOPS 26377, write IOPS 267
Bandwidth = 106.579 MB/s, read bw 105.509 MB/s, write bw 1.070 MB/s

============== Read latency in ms
         min        10 %        20 %        50 %        90 %        95 %        99 %      99.9 %         max
       0.002       0.008       0.008       0.012       0.456         0.6       1.171       3.921     138.495

============== Write latency in ms
         min        10 %        20 %        50 %        90 %        95 %        99 %      99.9 %         max
       0.006       0.015       0.016       0.022        0.04       0.053       0.122      10.671      90.687

```

## 90% read, 10% write (most data in disk)


```
./rdbtest -p /data/nvme0/rdbtest/ -n 30000000 -t 8 -q 800000 -l -w 0.1 -i 300 -s 4000


In total:  6798175 ops in 300.005525 sec (6117971 read, 680204 write).
Total IOPS = 22660, read IOPS 20393, write IOPS 2267
Bandwidth = 90.641 MB/s, read bw 81.571 MB/s, write bw 9.069 MB/s
Read miss 0 (0.00%), read failure 0, write failure 0

============== Read latency in ms
      min        10 %        20 %        50 %        90 %        95 %        99 %      99.9 %         max
     0.002       0.016        0.02       0.185       0.576       0.818       2.961      16.639     620.031

============== Write latency in ms
      min        10 %        20 %        50 %        90 %        95 %        99 %      99.9 %         max
     0.005       0.016       0.019       0.024       0.046       0.058       0.114       9.615     120.127

```

