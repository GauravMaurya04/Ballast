# storage/snap/

Snapshot files: write to temp + fsync + rename, file format with magic and CRC, loading the newest valid snapshot.

- **Spec:** §9.3
- **Owner:** **Gaurav writes.** Agents review and explain only; they never edit files here (SPEC §20.2, CLAUDE.md).
- **Must not:** interpret snapshot data.
