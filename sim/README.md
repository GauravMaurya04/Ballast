# sim/

Deterministic simulation harness, invariant checkers and milestone simulation tests. Failure traces are written to `sim/failures/<seed>.trace` (git-ignored).

- **Spec:** §17.2, §17.3
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** use goroutines per node, real time, or unseeded randomness (I11).
