package yaft

import (
	"net"
)

// PeerStore provides an interface for persistent storage and
// retrieval of peers. We use a separate interface than StableStore
// since the peers may need to be edited by a human operator. For example,
// in a two node cluster, the failure of either node requires human intervention
// since consensus is impossible.
type PeerStore interface {
	// Peers returns the list of known peers
	Peers() ([]net.Addr, error)

	// SetPeers sets the list of known peers. This is invoked when
	// a peer is added or removed
	SetPeers([]net.Addr) error
}

// DummyPeerStore is used to provide a dummy list of peers for unit testing
type DummyPeerStore struct {
	StaticPeers []net.Addr
}

func (s *DummyPeerStore) Peers() ([]net.Addr, error) {
	return s.StaticPeers, nil
}

func (s *DummyPeerStore) SetPeers(p []net.Addr) error {
	s.StaticPeers = p
	return nil
}
