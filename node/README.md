# node/

Driver loop (one goroutine per node that owns the Raft core) and the request tracker that maps proposal IDs and read contexts to waiting gRPC calls.

- **Spec:** §8, §4
- **Owner:** Claude writes, Gaurav reviews and must be able to explain the §8 order (SPEC §20.2).
- **Must not:** contain Raft decisions; hold locks across apply; send before persist + fsync (I6).
