# KeyDB

Simple key-value store based on a log-structured merge-tree architecture.

## Todo

- [x] Build basic building blocks (MemTable, SparseIndex, Segment)
- [x] Implement final logic in KeyDB to get MVP
- [x] Convert to gradle multi project
- [x] Don't reopen file handle on each `put`
- [x] Switch underlying key and value to support any types
- [ ] Bloom-filter for efficiently handling missing keys
- [x] Write benchmarking module (to compare above etc.)
- [ ] MULTITHREADING??!
- [ ] Basic compaction
- [ ] More advanced compaction (leveled?)
- [ ] Some basic logging
- [ ] More interesting metrics tracking
- [ ] Build a server (gRPC)


## Benchmarks

#### 6380eea46d44977998fe7a4db2d0a399ed44fadd
Fixed bug to get actual 2MB segments (was 1MB previously)

    Benchmark                                       Mode  Cnt   Score   Error  Units
    WriteBenchmarks.write1MegaByte                 thrpt    5  17.101 ± 0.317  ops/s
    ReadBenchmarks.getHundredThousandExistingKeys   avgt    5   4.767 ± 0.210   s/op
    ReadBenchmarks.getHundredThousandMissingKeys    avgt    5   9.186 ± 1.325   s/op

#### 9eb85a6820063c293b32857ed657b997a3ca3be6

    Benchmark                        Mode  Cnt   Score   Error  Units
    WriteBenchmarks.write1MegaByte  thrpt    5  20.683 ± 0.184  ops/s
