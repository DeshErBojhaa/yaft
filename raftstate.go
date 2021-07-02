package yaft

import (
	"sync/atomic"
)

type RaftState uint32

const (
	Follower RaftState = iota
	Candidate
	Leader
)

// raftState is used to maintain various state variables
// and provides an interface to set/get the variables in a
// thread safe manner
type raftState struct {
	// The current state
	state RaftState

	// The current term, cache of StableStore
	currentTerm uint64

	// Cache the latest log from LogStore
	lastLogIndex uint64

	// Highest committed log entry
	commitIndex uint64

	// Last applied log to the FSM
	lastApplied uint64
}

func (r *raftState) getState() RaftState {
	stateAddr := (*uint32)(&r.state)
	return RaftState(atomic.LoadUint32(stateAddr))
}

func (r *raftState) setState(s RaftState) {
	stateAddr := (*uint32)(&r.state)
	atomic.StoreUint32(stateAddr, uint32(s))
}

func (r *raftState) getCurrentTerm() uint64 {
	return atomic.LoadUint64(&r.currentTerm)
}

func (r *raftState) setCurrentTerm(term uint64) {
	atomic.StoreUint64(&r.currentTerm, term)
}

func (r *raftState) getLastLogIndex() uint64 {
	return atomic.LoadUint64(&r.lastLogIndex)
}

func (r *raftState) setLastLogIndex(term uint64) {
	atomic.StoreUint64(&r.lastLogIndex, term)
}

func (r *raftState) getCommitIndex() uint64 {
	return atomic.LoadUint64(&r.commitIndex)
}

func (r *raftState) setCommitIndex(term uint64) {
	atomic.StoreUint64(&r.commitIndex, term)
}

func (r *raftState) getLastApplied() uint64 {
	return atomic.LoadUint64(&r.lastApplied)
}

func (r *raftState) setLastApplied(term uint64) {
	atomic.StoreUint64(&r.lastApplied, term)
}
