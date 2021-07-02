package yaft

import (
	"bytes"
	cr "crypto/rand"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
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

var (
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
	keyCandidateId  = []byte("CandidateId")
	keyCurrentTerm  = []byte("CurrentTerm")

	// ErrNotFound is used in persistence layer
	ErrNotFound = errors.New("not found")
	// ErrNotLeader is used when a
	ErrNotLeader = fmt.Errorf("node is not the leader")
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

	// FSM is a finite state machine handler for logs
	fsm FSM

	// shutdownCh is to signal the system wide shutdown
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
	// log debug + info
	logD *log.Logger

	// peers ...
	peers []net.Addr

	// commitCh is used to provide the newest commit index
	// so that changes can be applied to the FSM. This is used
	// so the main goroutine can use commitIndex without locking.
	commitCh chan commitTuple

	// applyCh is used to manage commands to be applied
	applyCh chan *DeferLog
}

// commitTupel is used to send an index that was committed,
// with an optional associated deferLog that should be invoked
type commitTuple struct {
	index    uint64
	deferLog *DeferLog
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, store Store, logs LogStore, fsm FSM) (*Raft, error) {
	// Try to restore the current term
	currentTerm, err := store.GetUint64(keyCurrentTerm)
	if err != nil && errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

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
		currentTerm:  currentTerm,
		logE:         log.New(os.Stdout, "[ERROR]", log.LstdFlags),
		logW:         log.New(os.Stdout, "[WARN]", log.LstdFlags),
		logD:         log.New(os.Stdout, "[DEBUG]", log.LstdFlags),
		commitCh:     make(chan commitTuple, 128),
		applyCh:      make(chan *DeferLog),
	}

	go r.run()
	go r.runFSM()
	return r, nil
}

// run is a long running goroutine that runs the Raft FSM
func (r *Raft) run() {
	ch := r.trans.Consume()
	for {
		// Check if we are doing a shutdown
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower(ch)
		case Candidate:
			r.runCandidate(ch)
		case Leader:
			r.runLeader(ch)
		}
	}
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a defer that can be used to wait on the application.
// An optional timeout can be provided to limit the amount of time we wait
// for the command to be started.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyDefer {
	if timeout <= 0 {
		return &DeferError{err: fmt.Errorf("invalid timeout %v", timeout)}
	}
	var timer = time.After(timeout)

	// Create a log deferLog, no index or term yet
	logFuture := &DeferLog{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
	}
	logFuture.init()

	select {
	case <-timer:
		return &DeferError{err: fmt.Errorf("timed out enqueuing operation")}
	case r.applyCh <- logFuture:
		return logFuture
	}
}

// appendEntries is invoked when we get an append entries RPC call
// Returns true if we transition to a Follower
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) (transition bool) {
	// Setup a response
	resp := &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if a.Term < r.currentTerm {
		err = errors.New("obsolete term")
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.currentTerm || r.state != Follower {
		r.currentTerm = a.Term
		resp.Term = a.Term

		// Ensure transition to follower
		transition = true
		r.state = Follower
	}

	// Verify the last log entry
	var prevLog Log
	if err := r.logs.GetLog(a.PrevLogEntry, &prevLog); err != nil {
		r.logW.Printf("Failed to get previous log: %d %v",
			a.PrevLogEntry, err)
		return
	}
	if a.PrevLogTerm != prevLog.Term {
		r.logW.Printf("Previous log term mis-match: ours: %d remote: %d",
			prevLog.Term, a.PrevLogTerm)
		return
	}

	// Add all the entries
	for _, entry := range a.Entries {
		// Delete any conflicting entries
		if entry.Index <= r.lastLogIndex {
			r.logW.Printf("Clearing log suffix from %d to %d",
				entry.Index, r.lastLogIndex)
			if err := r.logs.DeleteRange(entry.Index, r.lastLogIndex); err != nil {
				r.logE.Printf("Failed to clear log suffix: %v", err)
				return
			}
		}

		// Append the entry
		if err := r.logs.StoreLog(entry); err != nil {
			r.logE.Printf("Failed to append to log: %v", err)
			return
		}

		// Update the lastLog
		r.lastLogIndex = entry.Index
	}

	// Update the commit index
	if a.LeaderCommitIndex > r.commitIndex {
		r.commitIndex = min(a.LeaderCommitIndex, r.lastLogIndex)
		// Trigger applying logs locally
		r.commitCh <- commitTuple{r.commitIndex, nil}
	}

	// Set success
	resp.Success = true
	return
}

// requestVote is called when node is in the follower state and
// get an request vote for candidate.
func (r *Raft) requestVote(rpc RPC, req *RequestVoteRequest) (transition bool) {
	// Setup a response
	resp := &RequestVoteResponse{
		Term:    r.currentTerm,
		Granted: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if req.Term < r.currentTerm {
		err = errors.New("obsolete term")
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.currentTerm {
		// Ensure transition to follower
		r.state = Follower
		r.currentTerm = req.Term
		resp.Term = req.Term
	}

	// Check if we have voted yet
	lastVoteTerm, err := r.stable.GetUint64(keyLastVoteTerm)
	if err != nil && err.Error() != "not found" {
		r.logE.Printf("raft: Failed to get last vote term: %v", err)
		return
	}
	lastVoteCandyBytes, err := r.stable.Get(keyLastVoteCand)
	if err != nil && err.Error() != "not found" {
		r.logE.Printf("raft: Failed to get last vote candidate: %v", err)
		return
	}

	// Check if we've voted in this election before
	if lastVoteTerm == req.Term && lastVoteCandyBytes != nil {
		r.logW.Printf("raft: Duplicate RequestVote for same term: %d", req.Term)
		if bytes.Compare(lastVoteCandyBytes, req.Candidate) == 0 {
			r.logW.Printf("raft: Duplicate RequestVote from candidate: %s", req.Candidate)
			resp.Granted = true
		}
		return
	}

	// Reject if their term is older
	lastIdx, lastTerm := r.lastLogIndex, r.currentTerm
	if lastTerm > req.LastLogTerm {
		r.logW.Printf("raft: Rejecting vote from %v since our last term is greater (%d, %d)",
			req.Candidate, lastTerm, req.LastLogTerm)
		return
	}

	if lastIdx > req.LastLogIndex {
		r.logW.Printf("raft: Rejecting vote from %v since our last index is greater (%d, %d)",
			req.Candidate, lastIdx, req.LastLogIndex)
		return
	}

	// Persist a vote for safety
	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logE.Printf("raft: Failed to persist vote: %v", err)
		return
	}

	resp.Granted = true
	return
}

// electSelf is used to send a RequestVote RPC to all peers,
// and vote for itself. This has the side affecting of incrementing
// the current term. The response channel returned is used to wait
// for all the responses (including a vote for ourself).
func (r *Raft) electSelf() <-chan *RequestVoteResponse {
	// Create a response channel
	respCh := make(chan *RequestVoteResponse, len(r.peers)+1)

	// Get the last log
	var lastLog Log
	if r.lastLogIndex > 0 {
		if err := r.logs.GetLog(r.lastLogIndex, &lastLog); err != nil {
			r.logE.Printf("Failed to get last log: %d %v",
				r.lastLogIndex, err)
			return nil
		}
	}

	// Increment the term
	if err := r.setCurrentTerm(r.currentTerm + 1); err != nil {
		r.logE.Printf("Failed to update current term: %v", err)
		return nil
	}

	// Construct the request
	req := &RequestVoteRequest{
		Term:         r.currentTerm,
		Candidate:    r.CandidateId(),
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}

	// request peer for a vote
	reqPeer := func(peer net.Addr) {
		resp := &RequestVoteResponse{
			Granted: false,
		}
		err := r.trans.RequestVote(peer, req, resp)
		if err != nil {
			r.logE.Printf("Failed to make RequestVote RPC to %v: %v",
				peer, err)
			resp.Term = req.Term
			resp.Granted = false
		}
		respCh <- resp
	}

	// For each peer, request a vote
	for _, peer := range r.peers {
		go reqPeer(peer)
	}

	// Persist a vote for ourselves
	if err := r.persistVote(req.Term, req.Candidate); err != nil {
		r.logE.Printf("Failed to persist vote : %v", err)
		return nil
	}

	// Include our own vote
	respCh <- &RequestVoteResponse{Term: req.Term, Granted: true}
	return respCh
}

// runFollower runs the FSM for a follower
func (r *Raft) runFollower(ch <-chan RPC) {
	for {
		select {
		case rpc := <-ch:
			// Handle the command
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				r.requestVote(rpc, cmd)
			default:
				r.logE.Printf("In follower state, got unexpected command: %#v", rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.response = ErrNotLeader
			a.Response()
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
func (r *Raft) runCandidate(ch <-chan RPC) {
	// Start vote for us, and set a timeout
	voteCh := r.electSelf()
	electionTimeout := randomTimeout(r.conf.ElectionTimeout, 2*r.conf.ElectionTimeout)

	// Tally the votes, need a simple majority
	grantedVotes := 0
	quorum := r.quorumSize()
	r.logD.Printf("Cluster size: %d, votes needed: %d", len(r.peers) + 1, quorum)

	transition := false
	for !transition {
		select {
		case rpc := <-ch:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				r.logE.Printf("Candidate state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}

		// Got response from peers on voting request
		case vote := <-voteCh:
			// Check if the term is greater than ours, bail
			if vote.Term > r.currentTerm {
				r.logD.Printf("Newer term discovered")
				r.state = Follower
				if err := r.setCurrentTerm(vote.Term); err != nil {
					r.logE.Printf("Failed to update current term: %v", err)
				}
				return
			}

			// Check if the vote is granted
			if vote.Granted {
				grantedVotes++
				r.logD.Printf("Vote granted. Tally: %d", grantedVotes)
			}

			// Check if we've become the leader
			if grantedVotes >= quorum {
				r.logD.Printf("Election won. Tally: %d", grantedVotes)
				r.state = Leader
				return
			}
		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.response = ErrNotLeader // TODO: Fix
			a.Response()

		case <-electionTimeout:
			// Election failed! Restart the election. We simply return,
			// which will kick us back into runCandidate
			r.logW.Printf("Election timeout reached, restarting election")
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runLeader runs the FSM for a leader
func (r *Raft) runLeader(ch <-chan RPC) {
	// Make a channel to processes commits, defer cancellation
	// of all inflight processes when we step down
	commitCh := make(chan *DeferLog)
	inflight := NewInflight(commitCh)
	defer inflight.Cancel(ErrNotLeader)

	// signal all peers that current node is stepping down.
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create the trigger channels
	triggers := make([]chan struct{}, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		triggers = append(triggers, make(chan struct{}))
	}

	// Start a replication routine for each peer
	for i, peer := range r.peers {
		go r.replicate(triggers[i], stopCh, peer)
	}

	transition := false
	for !transition {
		select {
		case applyLog := <-r.applyCh:
			// Prepare log
			applyLog.log.Index = r.lastLogIndex + 1
			applyLog.log.Term = r.currentTerm

			// Write the log entry locally
			if err := r.logs.StoreLog(&applyLog.log); err != nil {
				r.logE.Printf("Failed to commit log: %v", err)
				applyLog.response = err
				applyLog.Response()
				r.state = Follower
				return
			}
			r.lastLogIndex++

			// Add this to the inflight logs
			inflight.Start(applyLog, r.quorumSize())

			// Notify the replicators of the new log
			asyncNotify(triggers)

		case commitLog := <-commitCh:
			// Increment the commit index
			r.commitIndex = commitLog.log.Index

			// Trigger applying logs locally
			r.commitCh <- commitTuple{commitLog.log.Index, commitLog}

		case rpc := <-ch:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				log.Printf("[ERR] Leaderstate, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case <-r.shutdownCh:
			return
		}
	}
}

// runFSM is a long running goroutine responsible for the management
// of the local FSM.
func (r *Raft) runFSM() {
	for {
		select {
		case commitTuple := <-r.commitCh:
			// Get the log, either from the deferLog or from our log store
			var l *Log
			if commitTuple.deferLog != nil {
				l = &commitTuple.deferLog.log
			} else {
				l = new(Log)
				if err := r.logs.GetLog(commitTuple.index, l); err != nil {
					r.logE.Printf("Failed to get log: %v", err)
					panic(err)
				}
			}

			// Only apply commands, ignore other logs
			if l.Type == LogCommand {
				r.fsm.Apply(l.Data)
			}

			// Invoke the deferLog if given
			if commitTuple.deferLog != nil {
				commitTuple.deferLog.Response()
			}

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
	extra := time.Duration(rand.Int()) % maxVal
	return time.After((minVal + extra) % maxVal)
}

// min returns the minimum.
func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

// persistVote is used to persist our vote for safety
func (r *Raft) persistVote(term uint64, candidate []byte) error {
	if err := r.stable.SetUint64(keyLastVoteTerm, term); err != nil {
		return err
	}
	if err := r.stable.Set(keyLastVoteCand, candidate); err != nil {
		return err
	}
	return nil
}

// CandidateId is used to return a stable and unique candidate ID
func (r *Raft) CandidateId() []byte {
	// Get the persistent id
	raw, err := r.stable.Get(keyCandidateId)
	if err == nil {
		return raw
	}

	// Generate a UUID on the first call
	if errors.Is(err, ErrNotFound) {
		id := generateUUID()
		if err := r.stable.Set(keyCandidateId, []byte(id)); err != nil {
			panic(fmt.Errorf("failed to write CandidateId: %v", err))
		}
		return []byte(id)
	}
	panic(fmt.Errorf("failed to read CandidateId: %v", err))
}

// setCurrentTerm is used to set the current term in a durable manner
func (r *Raft) setCurrentTerm(t uint64) error {
	// Make persistence
	if err := r.stable.SetUint64(keyCurrentTerm, t); err != nil {
		r.logE.Printf("Failed to save current term: %v", err)
		return err
	}
	r.currentTerm = t
	return nil
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to our followers
func (r *Raft) replicate(triggerCh, stopCh chan struct{}, peer net.Addr) {
	// Initialize timer to fire immediately since
	// we just established leadership.
	timeout := time.After(time.Microsecond)
	for {
		select {

		case <-timeout:
			timeout = randomTimeout(r.conf.CommitTimeout, 2*r.conf.CommitTimeout)
			r.heartbeat(peer)

		case <-stopCh:
			return
		case <-triggerCh:
		}
	}
}

func (r *Raft) heartbeat(peer net.Addr) {
	// TODO: Cache prevLogEntry, prevLogTerm!
	var prevLogEntry, prevLogTerm uint64
	prevLogEntry = 0
	prevLogTerm = 0
	req := AppendEntriesRequest{
		Term:              r.currentTerm,
		Leader:            r.CandidateId(),
		PrevLogEntry:      prevLogEntry,
		PrevLogTerm:       prevLogTerm,
		LeaderCommitIndex: r.commitIndex,
	}
	var resp AppendEntriesResponse
	if err := r.trans.AppendEntries(peer, &req, &resp); err != nil {
		r.logE.Printf("Failed to heartbeat with %v: %v", peer, err)
	}
}

func (r *Raft) quorumSize() int {
	clusterSize := len(r.peers) + 1
	votesNeeded := (clusterSize / 2) + 1
	return votesNeeded
}

// generateUUID is used to generate a random UUID
func generateUUID() string {
	buf := make([]byte, 16)
	if _, err := cr.Read(buf); err != nil {
		panic(fmt.Errorf("failed to read random bytes: %v", err))
	}

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%12x",
		buf[0:4],
		buf[4:6],
		buf[6:8],
		buf[8:10],
		buf[10:16])
}

// asyncNotify is used to do an async channel send to
// a list of channels. This will not block.
func asyncNotify(chs []chan struct{}) {
	for _, ch := range chs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
