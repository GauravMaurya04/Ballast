# storage/wal/

Segmented write-ahead log: record format with CRC32C, fsync, segment roll, replay, torn-tail truncation, compaction.

- **Spec:** §9.1, §9.2
- **Owner:** **Gaurav writes.** Agents review and explain only; they never edit files here (SPEC §20.2, CLAUDE.md).
- **Must not:** interpret entries.
