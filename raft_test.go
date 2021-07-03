package yaft

import (
	"testing"
)

// MockFSM is an implementation of the FSM interface, and just stores
// the logs sequentially
type MockFSM struct {
	logs [][]byte
}

func (m *MockFSM) Apply(log []byte) {
	m.logs = append(m.logs, log)
}

func TestRaft_StartStop(t *testing.T) {
	_, trans := NewDummyTransport()
	store := NewDummyStore()

	fsm := &MockFSM{}
	conf := DefaultConfig()

	raft, err := NewRaft(conf, store, store, nil, fsm, trans)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer raft.Shutdown()
}

