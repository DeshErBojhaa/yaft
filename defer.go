package yaft

import (
	"time"
)

// Defer is used to represent an action that may occur in the future
type Defer interface {
	Error() error
}

// ApplyDefer is used for Apply() and can returns the FSM response
type ApplyDefer interface {
	Defer
	Response() interface{}
}

// deferError can be embedded to allow a future
// to provide an error in the future
type deferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	d.err = <-d.errCh
	return d.err
}

func (d *deferError) Response() interface{}{
	return nil
}

// deferLog is used to apply a log entry and waits until
// the log is considered committed
type deferLog struct {
	deferError
	log      Log
	quorum   majorityQuorum
	response interface{}
	dispatch time.Time
}

func (d *deferLog) Error() error {
	return d.deferError.Error()
}

func (d *deferLog) Response() interface{} {
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
