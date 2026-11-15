# Bug journal

One entry per real bug: anything that broke an invariant (SPEC §12), failed a
milestone test (§17.3), or made a simulation seed fail. Newest first.

The point is the lesson, not the fix. Write the entry after the root cause is
understood, in your own words.

## Index

| # | Date | Invariant | Area | One-line summary |
|---|---|---|---|---|
| | | | | |

---

## Template

Copy this block for each bug.

```markdown
## BUG-000: short title

- **Date found / fixed:** YYYY-MM-DD / YYYY-MM-DD
- **Milestone:** M1 | M2 | M3 | M3b | M4 | M5
- **Invariant broken:** ID and wording (e.g. I5 — the leader counts replicas only for current-term entries)
- **Spec section:** §x.y
- **Found by:** unit test | simulation | Porcupine | chaos | review (which agent) | by hand
- **Reproduce:** `go test ./sim -run <Test> -seed <seed> -v` (trace: `sim/failures/<seed>.trace`)
- **Fix:** commit hash

### Symptom

What was observed: the checker message, the failing assertion, the wrong output.

### Timeline

The few trace lines that matter, in order (`tick | node | role | term | event | details`),
with the first wrong event marked.

### Root cause

What the code did, what the spec requires, and why the difference stayed hidden
until this sequence of events.

### Fix

What changed and why that is sufficient. Which test now covers it.

### Lesson

The rule I had wrong or had not understood. What I would check first next time.
Any neighbouring bugs this suggests.
```
