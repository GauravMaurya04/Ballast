# kv/watch/

Watch hub: fans applied events out to watchers, with catch-up from history and slow-watcher handling.

- **Spec:** §10.8, W1–W3
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** block apply.
