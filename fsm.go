package yaft

// FSM provides an interface that can be implemented by
// clients to make use of the replicated log
type FSM interface {
	// Apply is invoked once a log entry is committed to
	// quorum number of followers
	Apply([]byte)
}
