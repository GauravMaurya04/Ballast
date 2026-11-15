# kv/lease/

Lease manager and the leader-only expiry timer, which proposes `LeaseRevoke` through the log.

- **Spec:** §10.6, K7–K9
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** delete keys directly; expire leases on a follower.
