# rocksdb-study
study rocks db performance


# RocksDB performance

The workflow is:  open db => bulk load => close => open db => trigger a compaction
=> run workload.

Test setup:

8 threads to issue r/w requests in parallel.

## 100% read (data in memory)

command to run:
`./rdbtest -p /data/nvme0/rdbtest/ -n 5000000 -t 8 -q 1000000 -l -i 30`

Results:
```
In total:  26416312 ops in 30.003713 sec (26416312 read, 0 write).
Total IOPS = 880435, read IOPS 880435, write IOPS 0
Bandwidth = 3788.373 MB/s, read bw 3521.739 MB/s, write bw 266.634 MB/s
Read miss 0 (0.00%), read failure 0, write failure 0

============== Read latency in ms
min    10 %     20 %    50 %     90 %        95 %    99 %      99.9 %         max
0.004  0.005   0.005    0.006   0.01 k     0.01       0.011       0.018       2.843
```

## 99% read, 1% write (data in memory)
`./rdbtest -p /data/nvme0/rdbtest/ -n 2000000 -t 8 -q 800000 -l  -w 0.01  -i 200`

```
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
