package yaft

import (
	"errors"
	"net"
)

// LogType describes various types of log entries.
type LogType uint8

// LogCommand is applied to a user FSM.
const (
	LogCommand LogType = iota

	// LogNoop is used to assert leadership
	LogNoop

	// LogAddPeer used to add a new peer
	LogAddPeer

	// LogRemovePeer used to remove an existing peer
	LogRemovePeer
)

var ErrLogNotFound = errors.New("log not found")

// Log entries are replicated to all members of the Raft cluster
// and form the heart of the replicated state machine.
type Log struct {
	Index uint64
	Term  uint64
	Type  LogType
	Data  []byte

	// Peer is not exported since it is not transmitted, only used
	// internally to construct the Data field.
	peer net.Addr
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion
type LogStore interface {
	// FirstIndex Returns the first index written. 0 for no entries.
	FirstIndex() (uint64, error)

	// LastIndex Returns the last index written. 0 for no entries.
	LastIndex() (uint64, error)

	// GetLog Gets a log entry at a given index
	GetLog(index uint64, log *Log) error

	// StoreLog Stores a log entry
	StoreLog(log *Log) error

	// StoreLogs Stores multiple log entries
	StoreLogs(logs []*Log) error

	// DeleteRange Deletes a range of log entries. The range is inclusive.
	DeleteRange(min, max uint64) error
}
