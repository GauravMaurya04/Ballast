# client/

Go client library: finds the leader, follows redirects, retries with the same sequence number, keeps leases alive, resumes watches.

- **Spec:** §4, §10.5, D10, D11
- **Owner:** Claude writes, Gaurav reviews (SPEC §20.2).
- **Must not:** retry with a new sequence number.
