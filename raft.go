package yaft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
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

	// Leader handles all write request. Leader is not expected
	// to change very often.
	Leader
)

type Raft struct {
	// Configuration
	conf *Config

	// Current state
	state RaftState

	// stable is a Store implementation for durable state
	stable Store

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

	// Transport layer. Most probably TCP.
	trans Transport

	// Cache the current term, write through to StableStore
	currentTerm uint64

	// Cache the latest log index, though we can get from LogStore
	lastLogIndex uint64

	// Log warnings
	logW *log.Logger
	// log errors
	logE *log.Logger
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, store Store, logs LogStore, fsm FSM) (*Raft, error) {
	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	r := &Raft{
		conf:         conf,
		state:        Follower,
		stable:       store,
		logs:         logs,
		commitIndex:  0,
		lastApplied:  0,
		fsm:          fsm,
		shutdownCh:   make(chan struct{}),
		lastLogIndex: lastLog,
		logE: log.New(os.Stdout, "[ERROR]", log.LstdFlags),
		logW: log.New(os.Stdout, "[WARN]", log.LstdFlags),
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

// followerAppendEntries is invoked when we are in the follower state and
// get an append entries RPC call
func (r *Raft) followerAppendEntries(rpc RPC, a *AppendEntriesRequest) {
	// Setup a default response
	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore any previous term
	if a.Term < r.currentTerm {
		err = errors.New("obsolete entry")
		return
	}

	// Increase the term if we see a newer one
	if a.Term > r.currentTerm {
		r.currentTerm = a.Term
		resp.Term = a.Term

		// TODO: Ensure transition to follower
	}

	// Verify the last log entry
	var prevLog Log
	if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
		r.logW.Printf("failed to get previous log: %d %v",
			a.PrevLogEntry, err)
		return
	}
	if a.PrevLogTerm != prevLog.Term {
		r.logW.Printf("previous log term mis-match: ours: %d remote: %d",
			prevLog.Term, a.PrevLogTerm)
		return
	}

	// Add all the entries
	for _, entry := range a.Entries {
		// Delete any conflicting entries
		if entry.Index <= r.lastLogIndex {
			r.logW.Printf("clearing log suffix from %d to %d", entry.Index, r.lastLogIndex)
			if err := r.logs.DeleteRange(entry.Index, r.lastLogIndex); err != nil {
				r.logE.Printf("failed to clear log suffix: %v", err)
				return
			}
		}

		// Append the entry
		if err := r.logs.StoreLog(entry); err != nil {
			r.logE.Printf("failed to append to log: %v", err)
			return
		}

		// Update the lastLogIndex
		r.lastLogIndex = entry.Index
	}

	// Update the commit index
	if a.LeaderCommitIndex > r.commitIndex {
		r.commitIndex = min(a.LeaderCommitIndex, r.lastLogIndex)

		// TODO: Trigger applying logs locally!
	}

	// Everything went well, set success
	resp.Success = true
}

// followerRequestVote is invoked when we are in the follwer state and
// get an request vote RPC call
func (r *Raft) followerRequestVote(rpc RPC, req *RequestVoteRequest) {
	// TODO
}

// runFollower runs the FSM for a follower
func (r *Raft) runFollower() {
	ch := r.trans.Consume()
	for {
		select {
		case rpc := <-ch:
			// Handle the command
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.followerAppendEntries(rpc, cmd)
			case *RequestVoteRequest:
				r.followerRequestVote(rpc, cmd)
			default:
				log.Printf("[ERR] in follower state, got unexpected command: %#v", rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}

		case <-randomTimeout(r.conf.HeartbeatTimeout, r.conf.ElectionTimeout):
			// Heartbeat failed! Go to the candidate state
			r.state = Candidate
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate() {
	ch := r.trans.Consume()
	for {
		select {
		case rpc := <-ch:
			// Handle the command
			switch rpc.Command.(type) {
			default:
				log.Printf("[ERR] Candidate state, got unexpected command: %#v", rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}

		case <-randomTimeout(r.conf.ElectionTimeout, r.conf.ElectionTimeout * 2):
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			return

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

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}
