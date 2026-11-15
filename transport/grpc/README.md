# transport/grpc/

Peer transport over gRPC: one long-lived connection and a 4,096-message send queue per peer, dropping the newest message when full.

- **Spec:** §14
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** guarantee delivery; block the driver loop.
