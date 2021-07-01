package yaft

import (
	"math/rand"
	"sync"
	"time"
)

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

	// FSM is a finite state machine handler for logs
	fsm FSM

	// shutdownCh is to signal the systemwide shutdown
	shutdownCh chan struct{}

	// shutdownLock is mutex for shutdown operations
	shutdownLock sync.Mutex

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via pendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	pendingConfIndex uint64

	// an estimate of the size of the uncommitted tail of the Raft log. Used to
	// prevent unbounded log growth. Only maintained by the leader. Reset on
	// term changes.
	uncommittedSize uint64

	// TODO: transport? TCP?
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, store ConfigStore, logs LogStore, fsm FSM) (*Raft, error) {
	r := &Raft{
		conf:        conf,
		state:       Follower,
		stable:      store,
		logs:        logs,
		commitIndex: 0,
		lastApplied: 0,
		fsm:         fsm,
		shutdownCh:  make(chan struct{}),
	}

	go r.run()
	return r, nil
}

// run is a long running goroutine that runs the Raft FSM
func (r *Raft) run() {
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// runFollower runs the FSM for a follower
func (r *Raft) runFollower() {
	for {
		select {
		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate() {
	for {
		select {
		case <-r.shutdownCh:
			return
		}
	}
}

// runLeader runs the FSM for a leader
func (r *Raft) runLeader() {
	for {
		select {
		case <-r.shutdownCh:
			return
		}
	}
}

// Shutdown is used to stop the Raft background routines.
// This is not a graceful operation.
func (r *Raft) Shutdown() {
	r.shutdownLock.Lock()
	defer r.shutdownLock.Lock()

	if r.shutdownCh != nil {
		close(r.shutdownCh)
		r.shutdownCh = nil
	}
}

// randomTimeout returns a value that is between the minVal and maxVal
func randomTimeout(minVal, maxVal time.Duration) <-chan time.Time {
	extra := time.Duration(rand.Int63()) % maxVal
	return time.After((minVal + extra) % maxVal)
}
