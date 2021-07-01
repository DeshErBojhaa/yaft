package yaft

// ConfigStore is used to provide stable storage
// of key configurations to ensure persistance.
type ConfigStore interface {
	// CurrentTerm Returns the current term
	CurrentTerm() (uint64, error)

	// VotedFor Returns the candidate we voted for this term
	VotedFor() (string, error)

	// SetCurrentTerm Sets the current term. Clears the current vote.
	SetCurrentTerm(uint64) error

	// SetVote Sets a candidate vote for the current term
	SetVote(string) error

	// CandidateID Returns our candidate ID. This should be unique
	// and constant across runs
	CandidateID() (string, error)
}

