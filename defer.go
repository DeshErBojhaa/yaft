package yaft

import (
	"time"
)

// Defer is used to represent an action that may occur in the deferLog
type Defer interface {
	Error() error
}

// ApplyDefer is used for Apply() and can returns the FSM response
type ApplyDefer interface {
	Defer
	Response() interface{}
}

// DeferError can be embedded to allow a deferLog
// to provide an error in the deferLog
type DeferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *DeferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *DeferError) Error() error {
	if d.err != nil {
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	d.err = <-d.errCh
	return d.err
}

func (d *DeferError) Response() interface{} {
	return nil
}

// DeferLog is used to apply a log entry and waits until
// the log is considered committed
type DeferLog struct {
	DeferError
	log      Log
	quorum   majorityQuorum
	response interface{}
	dispatch time.Time
}

func (d *DeferLog) Error() error {
	return d.DeferError.Error()
}

func (d *DeferLog) Response() interface{} {
	return d.response
}

// MajorityQuorum is used by Apply transactions and requires
// a simple majority of nodes
type majorityQuorum struct {
	count       int
	votesNeeded int
}

func (m *majorityQuorum) Commit() bool {
	m.count++
	return m.count >= m.votesNeeded
}

func (m *majorityQuorum) IsCommitted() bool {
	return m.count >= m.votesNeeded
}
