# server/

gRPC services (KV, Lease, Watch, Maintenance): validation, leader checks, `NotLeader` redirects with a leader hint.

- **Spec:** §13, D9, D10
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** bypass Raft for writes.
