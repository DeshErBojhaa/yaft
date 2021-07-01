package yaft

// Store is used to provide stable storage
// of key configurations to ensure persistence.
type Store interface {
	Set(key []byte, val []byte) error
	Get(key []byte) ([]byte, error)

	SetUint64(key []byte, val uint64) error
	GetUint64(key []byte) (uint64, error)
}
