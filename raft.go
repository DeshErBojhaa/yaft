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

var (
	keyLastVoteTerm = []byte("LastVoteTerm")
	keyLastVoteCand = []byte("LastVoteCand")
	keyCandidateId  = []byte("CandidateId")
	keyCurrentTerm  = []byte("CurrentTerm")

	// ErrNotFound is used in persistence layer
	ErrNotFound = errors.New("not found")
	// ErrNotLeader is used when a
	ErrNotLeader = fmt.Errorf("node is not the leader")
	// ErrLeadershipLost ...
	ErrLeadershipLost = fmt.Errorf("leadership lost while committing log")
)

type Raft struct {
	raftState
	// Configuration
	conf *Config

	// stable is a Store implementation for durable state
	stable Store

	// logs is a LogStore implementation to keep our logs
	logs LogStore

	// FSM is a finite state machine handler for logs
	fsm FSM

	shutdown bool
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

	// rpcCh for transport layer
	rpcCh <-chan RPC

	// Stores our local addr
	localAddr net.Addr

}

// commitTupel is used to send an index that was committed,
// with an optional associated deferLog that should be invoked
type commitTuple struct {
	index    uint64
	deferLog *DeferLog
}

// NewRaft is used to construct a new Raft node
func NewRaft(conf *Config, store Store, logs LogStore, peers []net.Addr, fsm FSM, trans Transport) (*Raft, error) {
	// Try to restore the current term
	currentTerm, err := store.GetUint64(keyCurrentTerm)
	if err != nil && errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("failed to load current term: %v", err)
	}

	lastLog, err := logs.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("failed to find last log: %v", err)
	}

	// Construct the list of peers that excludes us
	localAddr := trans.LocalAddr()
	otherPeers := make([]net.Addr, 0, len(peers))
	for _, p := range peers {
		if p.String() != localAddr.String() {
			otherPeers = append(otherPeers, p)
		}
	}

	r := &Raft{
		conf:       conf,
		stable:     store,
		logs:       logs,
		fsm:        fsm,
		shutdownCh: make(chan struct{}),
		logE:       log.New(os.Stdout, "[ERROR]", log.LstdFlags|log.Lshortfile),
		logW:       log.New(os.Stdout, "[WARN]", log.LstdFlags|log.Lshortfile),
		logD:       log.New(os.Stdout, "[DEBUG]", log.LstdFlags|log.Lshortfile),
		commitCh:   make(chan commitTuple, 128),
		applyCh:    make(chan *DeferLog),
		rpcCh:      trans.Consume(),
		peers:      otherPeers,
		trans:      trans,
		localAddr: localAddr,
	}
	// Initialize as a follower
	r.setState(Follower)

	// Restore the current term and the last log
	_ = r.setCurrentTerm(currentTerm)
	r.setLastLogIndex(lastLog)

	go r.run()
	go r.runFSM()
	return r, nil
}

func (r *Raft) String() string {
	return fmt.Sprintf("Node %s at %s", r.CandidateId(), r.localAddr.String())
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

		// Enter into a sub-FSM
		switch r.getState() {
		case Follower:
			r.runFollower()
		case Candidate:
			r.runCandidate()
		case Leader:
			r.runLeader()
		}
	}
}

// State is used to return the state of current node
func (r *Raft) State() RaftState {
	return r.getState()
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a defer that can be used to wait on the application.
// An timeout should be provided to limit the amount of time we wait
// for the command to be started.
func (r *Raft) Apply(cmd []byte, timeout time.Duration) ApplyDefer {
	if timeout <= 0 {
		return &DeferError{err: fmt.Errorf("invalid timeout %v", timeout)}
	}
	var timer = time.After(timeout)

	// Create a log deferLog, no index or term yet
	deferLog := &DeferLog{
		log: Log{
			Type: LogCommand,
			Data: cmd,
		},
	}
	deferLog.init()

	select {
	case <-timer:
		return &DeferError{err: fmt.Errorf("timed out enqueuing operation")}
	case r.applyCh <- deferLog:
		return deferLog
	}
}

// appendEntries is invoked when we get an append entries RPC call
// Returns true if we transition to a Follower
func (r *Raft) appendEntries(rpc RPC, a *AppendEntriesRequest) (transition bool) {
	// Setup a response
	resp := &AppendEntriesResponse{
		Term:    r.getCurrentTerm(),
		LastLog: r.getLastLogIndex(),
		Success: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if a.Term < r.getCurrentTerm() {
		err = errors.New("obsolete term")
		return
	}

	// Increase the term if we see a newer one, also transition to follower
	// if we ever get an appendEntries call
	if a.Term > r.getCurrentTerm() || r.getState() != Follower {
		r.currentTerm = a.Term
		resp.Term = a.Term

		// Ensure transition to follower
		transition = true
		r.setState(Follower)
	}

	// Verify the last log entry
	var prevLog Log
	if a.PrevLogEntry > 0 {
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
	}

	// Add all the entries
	for _, entry := range a.Entries {
		// Delete any conflicting entries
		if entry.Index <= r.getLastLogIndex() {
			r.logW.Printf("Clearing log suffix from %d to %d",
				entry.Index, r.getLastLogIndex())
			if err := r.logs.DeleteRange(entry.Index, r.getLastLogIndex()); err != nil {
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
		r.setLastLogIndex(entry.Index)
	}

	// Update the commit index
	if a.LeaderCommitIndex > r.getCommitIndex() {
		idx := min(a.LeaderCommitIndex, r.getLastLogIndex())
		r.setCommitIndex(idx)

		// Trigger applying logs locally
		r.commitCh <- commitTuple{idx, nil}
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
		Term:    r.getCurrentTerm(),
		Granted: false,
	}
	var err error
	defer rpc.Respond(resp, err)

	// Ignore an older term
	if req.Term < r.getCurrentTerm() {
		err = errors.New("obsolete term")
		return
	}

	// Increase the term if we see a newer one
	if req.Term > r.getCurrentTerm() {
		if err := r.setCurrentTerm(req.Term); err != nil {
			r.logE.Printf("Failed to update current term: %v", err)
			return
		}
		resp.Term = req.Term

		// Ensure transition to follower
		transition = true
		r.setState(Follower)
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
	if r.getLastLogIndex() > 0 {
		var lastLog Log
		if err := r.logs.GetLog(r.getLastLogIndex(), &lastLog); err != nil {
			r.logE.Printf("Failed to get last log: %d %v",
				r.getLastLogIndex(), err)
			return
		}
		if lastLog.Term > req.LastLogTerm {
			r.logW.Printf("Rejecting vote since our last term is greater")
			return
		}

		if lastLog.Index > req.LastLogIndex {
			r.logW.Printf("Rejecting vote since our last index is greater")
			return
		}
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
	if r.getLastLogIndex() > 0 {
		if err := r.logs.GetLog(r.lastLogIndex, &lastLog); err != nil {
			r.logE.Printf("Failed to get last log: %d %v",
				r.getLastLogIndex(), err)
			return nil
		}
	}

	// Increment the term
	if err := r.setCurrentTerm(r.getCurrentTerm() + 1); err != nil {
		r.logE.Printf("Failed to update current term: %v", err)
		return nil
	}

	// Construct the request
	req := &RequestVoteRequest{
		Term:         r.getCurrentTerm(),
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
func (r *Raft) runFollower() {
	for {
		select {
		case rpc := <-r.rpcCh:
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
			r.logW.Printf("Heartbeat timeout, start election process")
			r.setState(Candidate)
			return

		case <-r.shutdownCh:
			return
		}
	}
}

// runCandidate runs the FSM for a candidate
func (r *Raft) runCandidate() {
	// Start vote for us, and set a timeout
	voteCh := r.electSelf()
	electionTimeout := randomTimeout(r.conf.ElectionTimeout, 2*r.conf.ElectionTimeout)

	// Tally the votes, need a simple majority
	grantedVotes := 0
	quorum := r.quorumSize()
	r.logD.Printf("Cluster size: %d, votes needed: %d", len(r.peers)+1, quorum)

	transition := false
	for !transition {
		select {
		case rpc := <-r.rpcCh:
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
			if vote.Term > r.getCurrentTerm() {
				r.logD.Printf("Newer term discovered")
				r.setState(Follower)
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
				r.setState(Leader)
				return
			}
		case a := <-r.applyCh:
			// Reject any operations since we are not the leader
			a.response = ErrNotLeader
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
func (r *Raft) runLeader() {
	// Make a channel to processes commits, defer cancellation
	// of all inflight processes when we step down
	commitCh := make(chan *DeferLog, 128)
	inflight := NewInflight(commitCh)
	defer inflight.Cancel(ErrLeadershipLost)

	// signal all peers that current node is stepping down.
	stopCh := make(chan struct{})
	defer close(stopCh)

	// Create the trigger channels
	triggers := make([]chan struct{}, 0, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		triggers = append(triggers, make(chan struct{}, 1))
	}

	// Start a replication routine for each peer
	for i, peer := range r.peers {
		go r.replicate(inflight, triggers[i], stopCh, peer)
	}
	// seal leadership
	go r.leaderNoop()

	transition := false
	for !transition {
		select {
		case applyLog := <-r.applyCh:
			// Prepare log
			applyLog.log.Index = r.getLastLogIndex() + 1
			applyLog.log.Term = r.getCurrentTerm()
			// Write the log entry locally
			if err := r.logs.StoreLog(&applyLog.log); err != nil {
				r.logE.Printf("Failed to commit log: %v", err)
				applyLog.response = err
				applyLog.Response()
				r.setState(Follower)
				return
			}

			// Add this to the inflight logs
			inflight.Start(applyLog, r.quorumSize())
			inflight.Commit(applyLog.log.Index)
			// Update the last log since it's on disk now
			r.setLastLogIndex(applyLog.log.Index)

			// Notify the replicators of the new log
			asyncNotify(triggers)

		case commitLog := <-commitCh:
			// Increment the commit index
			idx := commitLog.log.Index
			r.setCommitIndex(idx)
			// Trigger applying logs locally
			r.commitCh <- commitTuple{idx, commitLog}

		case rpc := <-r.rpcCh:
			switch cmd := rpc.Command.(type) {
			case *AppendEntriesRequest:
				transition = r.appendEntries(rpc, cmd)
			case *RequestVoteRequest:
				transition = r.requestVote(rpc, cmd)
			default:
				r.logE.Printf("Leader state, got unexpected command: %#v",
					rpc.Command)
				rpc.Respond(nil, fmt.Errorf("unexpected command"))
			}
		case <-r.shutdownCh:
			return
		}
	}
}

// leaderNoop is a blocking command that appends a no-op log
// entry. It is used to seal leadership.
func (r *Raft) leaderNoop() {
	logFuture := &DeferLog{
		log: Log{
			Type: LogNoop,
		},
	}
	r.applyCh <- logFuture
}

// runFSM is a long running goroutine responsible for the management
// of the local FSM.
func (r *Raft) runFSM() {
	for {
		select {
		case commitTuple := <-r.commitCh:
			// obsolete logs
			if commitTuple.index <= r.getLastApplied() {
				r.logW.Printf("Skipping application of old log: %d",
					commitTuple.index)
				continue
			}

			// Apply all the preceding logs
			for idx := r.getLastApplied() + 1; idx <= commitTuple.index; idx++ {
				// Get the log, either from the future or from our log store
				var l *Log
				if commitTuple.deferLog != nil && commitTuple.deferLog.log.Index == idx {
					l = &commitTuple.deferLog.log
				} else {
					l = new(Log)
					if err := r.logs.GetLog(idx, l); err != nil {
						r.logE.Printf("Failed to get log at %d: %v", idx, err)
						panic(err)
					}
				}

				// Only apply commands, ignore other logs
				if l.Type == LogCommand {
					r.fsm.Apply(l.Data)
				}

				// Update the lastApplied
				r.setLastApplied(l.Index)
			}

			// Invoke the future if given
			if commitTuple.deferLog != nil {
				if commitTuple.deferLog.errCh == nil || cap(commitTuple.deferLog.errCh) == 0 {
					commitTuple.deferLog.init()
				}
				commitTuple.deferLog.errCh <- nil
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
	defer r.shutdownLock.Unlock()

	if !r.shutdown {
		close(r.shutdownCh)
		r.shutdown = true
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

func max(a, b uint64) uint64 {
	if a >= b {
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
	r.raftState.setCurrentTerm(t)
	return nil
}

type followerReplication struct {
	matchIndex uint64
	nextIndex  uint64
}

// replicate is a long running routine that is used to manage
// the process of replicating logs to peers
func (r *Raft) replicate(inflight *inflight, triggerCh, stopCh chan struct{}, peer net.Addr) {
	// Initialize the indexes
	last := r.getLastLogIndex()
	indexes := followerReplication{
		matchIndex: last,
		nextIndex:  last + 1,
	}

	// Replicate when a new log arrives or if we timeout
	shouldStop := false
	for !shouldStop {
		select {
		case <-triggerCh:
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLogIndex(), peer)
		case <-randomTimeout(r.conf.CommitTimeout, 2*r.conf.CommitTimeout):
			shouldStop = r.replicateTo(inflight, &indexes, r.getLastLogIndex(), peer)
		case <-stopCh:
			return
		}
	}
}

// replicateTo is used to make lagging followers to to date
func (r *Raft) replicateTo(inflight *inflight, indexes *followerReplication, lastIndex uint64, peer net.Addr) (shouldStop bool) {
	// Create the base request
	var (
		l    Log
		req  AppendEntriesRequest
		resp AppendEntriesResponse
	)
START:
	req = AppendEntriesRequest{
		Term:              r.getCurrentTerm(),
		Leader:            r.CandidateId(),
		LeaderCommitIndex: r.getCommitIndex(),
	}
	fmt.Printf("**** Index %#v\n", indexes)
	// Log entry starts at 1
	if indexes.nextIndex > 1 {
		if err := r.logs.GetLog(indexes.nextIndex-1, &l); err != nil {
			r.logE.Printf("Failed to get log at index %d: %v",
				indexes.nextIndex-1, err)
			return
		}
	} else {
		req.PrevLogEntry = 0
		req.PrevLogTerm = 0
	}

	// Append up to MaxAppendEntries or up to the lastIndex
	req.Entries = make([]*Log, 0, 16)
	maxIndex := min(indexes.nextIndex+uint64(r.conf.MaxAppendEntries)-1, lastIndex)
	for i := indexes.nextIndex; i <= maxIndex; i++ {
		oldLog := new(Log)
		if err := r.logs.GetLog(i, oldLog); err != nil {
			r.logE.Printf("Failed to get log at index %d: %v", i, err)
			return
		}
		req.Entries = append(req.Entries, oldLog)
	}

	// Make the RPC call
	if err := r.trans.AppendEntries(peer, &req, &resp); err != nil {
		r.logE.Printf("Failed to AppendEntries to %v: %v", peer, err)
		return
	}

	// Check for a newer term, stop running
	if resp.Term > req.Term {
		return true
	}

	// Update the indexes based on success
	if resp.Success {
		// Mark any inflight logs as committed
		for i := indexes.matchIndex; i <= maxIndex; i++ {
			inflight.Commit(i)
		}

		indexes.matchIndex = maxIndex
		indexes.nextIndex = maxIndex + 1
	} else {
		r.logW.Printf("AppendEntries to %v rejected, sending older logs", peer)
		indexes.nextIndex = max(min(indexes.nextIndex-1, resp.LastLog+1), 1)

		indexes.matchIndex = indexes.nextIndex - 1
	}

	// Check if there are more logs to replicate
	if indexes.nextIndex <= lastIndex {
		goto START
	}
	return
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
