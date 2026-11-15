// Package raft is Ballast's Raft core: a deterministic state machine with no I/O,
// driven by the driver loop in package node.
//
// This file is the API contract from docs/SPEC.md §6, verbatim. Change it only
// together with a recorded spec change (§20.4).
package raft

import (
	"errors"
	"math/rand"
)

type NodeID uint64

type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

type EntryType uint8

const (
	EntryNormal EntryType = iota // Data = encoded kv.Command
	EntryNoOp                    // appended by a new leader at the start of its term
)

type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState is persisted before any message that depends on it is sent.
type HardState struct {
	Term   uint64
	Vote   NodeID // 0 = none
	Commit uint64
}

type SnapshotMeta struct {
	Index uint64   // last log index the snapshot includes
	Term  uint64   // term of that entry
	Peers []NodeID // cluster members (fixed in v1)
}

type Snapshot struct {
	Meta SnapshotMeta
	Data []byte // opaque to raft: encoded kv state (§10.7)
}

type MsgType uint8

const (
	MsgPreVote MsgType = iota
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp // AppendEntries
	MsgAppResp
	MsgHeartbeat
	MsgHeartbeatResp
	MsgSnap // InstallSnapshot (whole snapshot in one message in v1)
)

type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64 // sender's term; PreVote carries term+1 without the sender changing its term

	// Vote / PreVote: last log index and term of the candidate.
	// App: prevLogIndex and prevLogTerm.
	LogIndex uint64
	LogTerm  uint64

	Entries []Entry
	Commit  uint64 // leader's commit index (App, Heartbeat)

	Reject     bool
	RejectHint uint64 // AppResp reject: follower's last index, for fast backtracking

	Index uint64 // AppResp accept: highest index the follower now matches

	Context  []byte    // ReadIndex context, echoed in Heartbeat / HeartbeatResp
	Snapshot *Snapshot // MsgSnap only
}

type SoftState struct { // not persisted
	Lead NodeID
	Role Role
}

type ReadState struct {
	Index      uint64 // commit index confirmed by a quorum for this read
	RequestCtx []byte
}

// Ready is everything the driver must do, in the order of §8.
type Ready struct {
	SoftState        *SoftState // nil if unchanged
	HardState        *HardState // nil if unchanged
	Snapshot         *Snapshot  // received from the leader: persist, then apply
	Entries          []Entry    // persist BEFORE sending Messages
	Messages         []Message
	CommittedEntries []Entry // already persisted, safe to apply
	ReadStates       []ReadState
}

type Status struct {
	ID        NodeID
	Role      Role
	Term      uint64
	Lead      NodeID
	Commit    uint64
	Applied   uint64
	LastIndex uint64
	Progress  map[NodeID]Progress // leader only
}

type Progress struct {
	Match, Next  uint64
	RecentActive bool
}

// Storage is read-only from the core's point of view.
type Storage interface {
	InitialState() (HardState, SnapshotMeta, error)
	Entries(lo, hi uint64) ([]Entry, error) // [lo, hi); ErrCompacted, ErrUnavailable
	Term(i uint64) (uint64, error)
	FirstIndex() (uint64, error) // first index still in the log (snapshot.Index+1)
	LastIndex() (uint64, error)
	Snapshot() (Snapshot, error)
}

type Config struct {
	ID              NodeID
	Peers           []NodeID // includes ID
	ElectionTicks   int      // E: timeout is random in [E, 2E)
	HeartbeatTicks  int      // must be < ElectionTicks
	Storage         Storage
	Applied         uint64     // last applied index after restart
	PreVote         bool       // true in v1
	CheckQuorum     bool       // true in v1
	MaxSizePerMsg   uint64     // bytes of entries per MsgApp
	MaxInflightMsgs int        // unacknowledged MsgApp per follower
	Rand            *rand.Rand // seeded; the only source of randomness
}

type Core interface {
	Tick()
	Step(m Message) error      // incoming peer message
	Propose(data []byte) error // ErrNotLeader, ErrProposalDropped
	ReadIndex(ctx []byte)      // result arrives as Ready.ReadStates
	HasReady() bool
	Ready() Ready
	Advance(rd Ready) // called after the driver has carried out rd
	Status() Status
}

var (
	ErrNotLeader       = errors.New("raft: not leader")
	ErrProposalDropped = errors.New("raft: proposal dropped")
	ErrCompacted       = errors.New("raft: requested index is compacted")
	ErrUnavailable     = errors.New("raft: requested entry not available")
)
