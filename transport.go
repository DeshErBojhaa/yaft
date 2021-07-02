package yaft

import "net"

// RPC has a command, and provides a Reponse mechanism
type RPC struct {
	// Type assert to determine the type
	Command  interface{}
	RespChan chan<- RPCResponse
}

// RPCResponse captures both a response and a potential error
type RPCResponse struct {
	Response interface{}
	Error    error
}

// Respond is used to respond with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

// Transport provides an interface for network transports
// to allow Raft to communicate with other nodes
type Transport interface {
	// Consume returns a channel that can be used to
	// consume and respond to RPC requests.
	Consume() <-chan RPC

	// LocalAddr is used to return the local address to distinguish from our peers
	LocalAddr() net.Addr

	// AppendEntries sends the appropriate RPC to the target node
	AppendEntries(target net.Addr, args *AppendEntriesRequest, resp *AppendEntriesResponse) error

	// RequestVote sends the appropriate RPC to the target node
	RequestVote(target net.Addr, args *RequestVoteRequest, resp *RequestVoteResponse) error

	//TODO: Snapshots?
}
