# kvstore-bench
Go kv store benchmark

## Tested kvstore

* [BadgerDB](https://github.com/dgraph-io/badger) (with default options)
* [BoltDB](https://github.com/boltdb/bolt) (with default options)
* [BuntDB](https://github.com/tidwall/buntdb) (with default options)
* [LevelDB](https://github.com/syndtr/goleveldb) (with default options)
* [NutsDB](https://github.com/xujiajun/nutsdb) (with default options or custom options)

## Benchmark System:

* Go Version : go1.11.4 darwin/amd64
* OS: Mac OS X 10.13.6
* Architecture: x86_64
* 16 GB 2133 MHz LPDDR3
* CPU: 3.1 GHz Intel Core i7

## To run benchmark:

> go test -bench=.

##  Benchmark results:

```
BenchmarkBadgerDBPutValue64B-8    	   10000	    135431 ns/op	    2375 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue128B-8   	   10000	    119450 ns/op	    2503 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue256B-8   	   10000	    142451 ns/op	    2759 B/op	      74 allocs/op
BenchmarkBadgerDBPutValue512B-8   	   10000	    109066 ns/op	    3270 B/op	      74 allocs/op
BenchmarkBadgerDBGet-8            	 1000000	      1679 ns/op	     416 B/op	       9 allocs/op
BenchmarkBoltDBPutValue64B-8      	    5000	    200487 ns/op	   20005 B/op	      59 allocs/op
BenchmarkBoltDBPutValue128B-8     	    5000	    230297 ns/op	   13703 B/op	      64 allocs/op
BenchmarkBoltDBPutValue256B-8     	    5000	    207220 ns/op	   16708 B/op	      64 allocs/op
BenchmarkBoltDBPutValue512B-8     	    5000	    262358 ns/op	   17768 B/op	      64 allocs/op
BenchmarkBoltDBGet-8              	 1000000	      1163 ns/op	     592 B/op	      10 allocs/op
BenchmarkBoltDBRangeScans-8       	 1000000	      1226 ns/op	     584 B/op	       9 allocs/op
BenchmarkBoltDBPrefixScans-8      	 1000000	      1275 ns/op	     584 B/op	       9 allocs/op
BenchmarkBuntDBPutValue64B-8      	  200000	      8930 ns/op	     927 B/op	      14 allocs/op
BenchmarkBuntDBPutValue128B-8     	  200000	      8892 ns/op	    1015 B/op	      15 allocs/op
BenchmarkBuntDBPutValue256B-8     	  200000	     11282 ns/op	    1274 B/op	      16 allocs/op
BenchmarkBuntDBPutValue512B-8     	  200000	     12323 ns/op	    1794 B/op	      16 allocs/op
BenchmarkBuntDBGet-8              	 2000000	       675 ns/op	     104 B/op	       4 allocs/op
BenchmarkLevelDBPutValue64B-8     	  100000	     11909 ns/op	     476 B/op	       7 allocs/op
BenchmarkLevelDBPutValue128B-8    	  200000	     10838 ns/op	     254 B/op	       7 allocs/op
BenchmarkLevelDBPutValue256B-8    	  100000	     11510 ns/op	     445 B/op	       7 allocs/op
BenchmarkLevelDBPutValue512B-8    	  100000	     12661 ns/op	     799 B/op	       8 allocs/op
BenchmarkLevelDBGet-8             	 1000000	      1371 ns/op	     184 B/op	       5 allocs/op
BenchmarkNutsDBPutValue64B-8      	 1000000	      2472 ns/op	     670 B/op	      14 allocs/op
BenchmarkNutsDBPutValue128B-8     	 1000000	      2182 ns/op	     664 B/op	      13 allocs/op
BenchmarkNutsDBPutValue256B-8     	 1000000	      2579 ns/op	     920 B/op	      13 allocs/op
BenchmarkNutsDBPutValue512B-8     	 1000000	      3640 ns/op	    1432 B/op	      13 allocs/op
BenchmarkNutsDBGet-8              	 2000000	       781 ns/op	      88 B/op	       3 allocs/op
BenchmarkNutsDBGetByMemoryMap-8   	   50000	     40734 ns/op	     888 B/op	      17 allocs/op
BenchmarkNutsDBPrefixScan-8       	 1000000	      1293 ns/op	     656 B/op	       9 allocs/op
BenchmarkNutsDBRangeScan-8        	 1000000	      2250 ns/op	     752 B/op	      12 allocs/op
```

## Conclusions:

### Put(write) Performance: 

NutsDB、BuntDB、LevelDB are fast. And NutsDB is fastest. It is 5-10x faster than LevelDB, 5x faster than BuntDB.

### Get(read) Performance: 

All are fast. And NutsDB and BuntDB with default options are 2x faster than others.
And NutsDB reads with MemoryMap option is slower 40x than its default option way. 
