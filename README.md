# KeyDB

Simple key-value store based on a log-structured merge-tree architecture.

## Todo

- [x] Build basic building blocks (MemTable, SparseIndex, Segment)
- [x] Implement final logic in KeyDB to get MVP
- [x] Convert to gradle multi project
- [ ] Don't reopen file handle on each `set`
- [ ] Switch underlying key and value types to be arbitrary byte arrays?
- [ ] Basic compaction
- [ ] Look into more efficient way of serializing the data (protobuf/Avro?)
- [ ] Bloom-filter for efficiently handling missing keys
- [x] Write benchmarking module (to compare above etc.)
- [ ] MULTITHREADING??!
- [ ] More advanced compaction (leveled?)
- [ ] Some basic logging
- [ ] More interesting metrics tracking
- [ ] Build a server (gRPC)
