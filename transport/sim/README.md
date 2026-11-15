# transport/sim/

Simulated network with the same `Transport` interface: delivery queue ordered by tick, with drop, duplicate, delay, reorder and partitions.

- **Spec:** §14, §17.2
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** use real time or unseeded randomness.
