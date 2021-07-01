package yaft

import "time"

// Config provides any necessary configuraiton to
// the Raft server
type Config struct {
	// Time in follower state without a leader before we attempt an election
	HeartbeatTimeout time.Duration

	// Time without a leader before we attempt an election
	ElectionTimeout time.Duration
}

//DefaultConfig is used on bootstrap if other configs are
//not explicitly mentioned
func DefaultConfig() *Config {
	return &Config{
		HeartbeatTimeout: 100 * time.Millisecond,
		ElectionTimeout: 150 * time.Millisecond,
	}
}

