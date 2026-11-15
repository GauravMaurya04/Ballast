# kv/

State machine: MVCC store, revisions, apply, client sessions, compare logic. Subpackages: `lease/`, `watch/`, `kvpb/`.

- **Spec:** §10
- **Owner (SPEC §20.2):**
  - `apply.go`, `compare.go`, `session.go`: **Gaurav writes.** Agents review and explain only; they never edit these files (CLAUDE.md).
  - `store.go`: Claude may write it, as the B-tree wrapper only (the ordered `key → KeyValue` map of §10.2). No apply, compare or session logic in it. Gaurav reviews.
  - `lease/`, `watch/`, `kvpb/`: Claude writes, Gaurav reviews.
  - Any other file in this directory: ask Gaurav first.
- **Must not:** read wall-clock time or randomness while applying (K1).
