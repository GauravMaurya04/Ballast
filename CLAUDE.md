# Rules for AI agents working on Ballast

Ballast is a Raft-based coordination service in Go. Gaurav is building the core
himself to learn it; agents build the surrounding system and review his code.

## 1. The spec is the source of truth

- `docs/SPEC.md` is the single source of truth. Read the relevant sections before
  writing or reviewing anything.
- In every review finding, cite the spec section or invariant ID it rests on
  (for example "§7.7", "I5", "R1", "K2"). A finding with no citation is an
  opinion: label it as one.
- If code and spec disagree, either the code is wrong or the spec needs a recorded
  change in §20.4. Do not silently pick one. Do not edit `docs/SPEC.md` without
  being asked.

## 2. Never write or edit Gaurav-owned files (SPEC §20.2)

Gaurav-owned paths:

- `raft/` — everything except `raft/types.go`. (`raft/raft.go` was scaffolded once
  as a stub during repo setup; it is Gaurav's from then on.)
- `storage/wal/`
- `storage/snap/`
- `kv/apply.go`, `kv/compare.go`, `kv/session.go` (SPEC §10.3, §10.5)

For these paths: **review and explain only.** No edits, no new files, no "small
fixes", no generated tests dropped into them, no patches applied through shell
commands. If a task seems to need a change there, stop and tell Gaurav what you
found and why.

In `kv/` itself, Claude may write `kv/store.go` only, and only as the B-tree
wrapper (the ordered `key → KeyValue` map of SPEC §10.2): no apply, compare or
session logic in it. For any other file directly in `kv/`, ask Gaurav first.

Everything else in §20.2 is Claude-written and Gaurav-reviewed.

## 3. Guide, don't hand over the fix

For Gaurav-owned code, prefer a guiding question over the answer:

- Point at the spec section or invariant and ask what should happen in the
  failing case, rather than writing the corrected code.
- Give the smallest hint that unblocks him. Escalate to a fuller explanation only
  when he asks for it.
- Explaining concepts, the Raft paper, traces and test failures is always fine.

## Working conventions

- Module path: `github.com/GauravMaurya04/Ballast`. Go 1.24+ (SPEC D1).
- `make test`, `make test-race`, `make sim`, `make lint`.
- Milestone test suites are gated by `BALLAST_MILESTONE` (SPEC §17.3).
- Review agents live in `.claude/agents/`.
- Bugs go in `docs/bug-journal.md` with the invariant ID that broke; progress goes
  in `docs/weekly-log.md`. Both are Gaurav's documents: add or edit entries only
  when he asks.
