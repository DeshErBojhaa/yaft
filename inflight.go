package yaft

import (
	"sync"
)

// Inflight is used to track operations that are still in-flight
type inflight struct {
	sync.Mutex
	commitCh   chan *DeferLog
	operations map[uint64]*inflightLog
}

// inflightLog represents a single log entry that is in-flight
type inflightLog struct {
	future      *DeferLog
	commitCount int
	quorum      int
}

// NewInflight returns an in-flight struct that notifies
// the provided channel when logs are finished committing.
func NewInflight(commitCh chan *DeferLog) *inflight {
	return &inflight{
		commitCh:   commitCh,
		operations: make(map[uint64]*inflightLog),
	}
}

// Start is used to mark a logFuture as being inflight
func (i *inflight) Start(l *DeferLog, quorum int) {
	i.Lock()
	defer i.Unlock()

	op := &inflightLog{
		future:      l,
		commitCount: 0,
		quorum:      quorum,
	}
	i.operations[l.log.Index] = op
}

// Cancel is used to cancel all in-flight operations.
// This is done when the leader steps down, and all futures
// are sent the given error.
func (i *inflight) Cancel(err error) {
	i.Lock()
	defer i.Unlock()

	// Respond to all in-flight operations
	for _, op := range i.operations {
		op.future.response = err
		op.future.Response()
	}

	// Clear the map
	i.operations = make(map[uint64]*inflightLog)
}

// Commit is used by leader replication routines to indicate that
// a follower was finished committing a log to disk.
func (i *inflight) Commit(index uint64) {
	i.Lock()
	defer i.Unlock()

	op, ok := i.operations[index]
	if !ok {
		// Ignore if not in the map, as it may be committed already
		return
	}

	// Increment the commit count
	op.commitCount++

	// Check if we have committed this
	if op.commitCount < op.quorum {
		return
	}

	// Stop tracking since it is committed
	delete(i.operations, index)

	// Notify of commit
	i.commitCh <- op.future
}
