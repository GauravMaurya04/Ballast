# storage/mem/

In-memory implementation of `raft.Storage` for unit tests and the simulation, including a synced/unsynced split so crashes can drop unsynced writes.

- **Spec:** §4, §17.2
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** interpret entries.
