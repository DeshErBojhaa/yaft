package yaft

type RaftState uint8

const (
	// Follower indicates current node in follower state.
	// From follower, it can become candidate and ask for vote.
	Follower RaftState = iota

	// Candidate is when a follower has not received heartbeat from
	// the leader for some time, so it decides to step up.
	Candidate

	Leader
)
var _ = Follower
var _ = Candidate
var _ = Leader

type Raft struct {
	// Configuration
	conf *Config

	// Current state
	state RaftState

	// stable is a ConfigStore implementation for durable state
	stable ConfigStore

	// logs is a LogStore implementation to keep our logs
	logs LogStore

	// Highest committed log entry
	commitIndex uint64

	// Last applied log to the FSM
	lastApplied uint64

	// If we are the leader, we have extra state
	leader *LeaderState
}

