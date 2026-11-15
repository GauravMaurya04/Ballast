package raft

import "errors"

// ErrNotImplemented is returned by New until the core exists.
var ErrNotImplemented = errors.New("raft: not implemented")

// New builds a Core from cfg (contract: docs/SPEC.md §6).
//
// Stub only. Owner: Gaurav (SPEC §20.2). The behaviour to implement is SPEC §7,
// on top of Figure 2 of the Raft paper:
//
//	§7.1  Roles and transitions (Follower, PreCandidate, Candidate, Leader)
//	§7.2  Timers (randomised election timeout in [E, 2E) from cfg.Rand; heartbeats)
//	§7.3  Pre-vote
//	§7.4  Election (candidate)
//	§7.5  Becoming leader (next/match reset, no-op entry in the new term)
//	§7.6  Replication (MsgApp, fast backtracking with RejectHint, flow control)
//	§7.7  Commit rule (quorum match AND current-term entry)
//	§7.8  ReadIndex
//	§7.9  CheckQuorum
//	§7.10 Term handling (every message)
//	§7.11 Snapshots in the core (MsgSnap)
//
// Invariants the core must hold: R1-R5 (§12.1) and I1-I5, I7-I11 (§12.2).
func New(cfg Config) (Core, error) {
	return nil, ErrNotImplemented
}
