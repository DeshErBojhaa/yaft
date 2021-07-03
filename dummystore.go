package yaft

import "sync"

// DummyStore implements the LogStore and Store interface.
// It should NOT EVER be used for production. It is used only for
// unit tests. Use the MDBStore implementation instead.
type DummyStore struct {
	mu         sync.RWMutex
	kv         map[string][]byte
	kvInt      map[string]uint64
	firstIndex uint64
	lastIndex  uint64
	logs       map[uint64]*Log

}

// NewDummyStore returns a new in-memory backend. Do not ever
// use for production. Only for testing.
func NewDummyStore() *DummyStore {
	i := &DummyStore{
		kv:    make(map[string][]byte),
		kvInt: make(map[string]uint64),
		logs:  make(map[uint64]*Log),
	}
	return i
}

// Get implements the StableStore interface.
func (d *DummyStore) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	val := d.kv[string(key)]
	if val == nil {
		return nil, ErrNotFound
	}
	return val, nil
}

// Set implements the StableStore interface.
func (d *DummyStore) Set(key []byte, val []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.kv[string(key)] = val
	return nil
}

// GetUint64 implements the StableStore interface.
func (d *DummyStore) GetUint64(key []byte) (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.kvInt[string(key)], nil
}

// SetUint64 implements the StableStore interface.
func (d *DummyStore) SetUint64(key []byte, val uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.kvInt[string(key)] = val
	return nil
}

// FirstIndex implements the LogStore interface.
func (d *DummyStore) FirstIndex() (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.firstIndex, nil
}

// LastIndex implements the LogStore interface.
func (d *DummyStore) LastIndex() (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lastIndex, nil
}

// GetLog implements the LogStore interface.
func (d *DummyStore) GetLog(index uint64, log *Log) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	l, ok := d.logs[index]
	if !ok {
		return ErrLogNotFound
	}
	*log = *l
	return nil
}

// StoreLog implements the LogStore interface.
func (d *DummyStore) StoreLog(log *Log) error {
	return d.StoreLogs([]*Log{log})
}

// StoreLogs implements the LogStore interface.
func (d *DummyStore) StoreLogs(logs []*Log) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, l := range logs {
		d.logs[l.Index] = l
		if d.firstIndex == 0 {
			d.firstIndex = l.Index
		}
		if l.Index > d.lastIndex {
			d.lastIndex = l.Index
		}
	}
	return nil
}

// DeleteRange implements the LogStore interface.
func (d *DummyStore) DeleteRange(min, max uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for j := min; j <= max; j++ {
		delete(d.logs, j)
	}
	if min <= d.firstIndex {
		d.firstIndex = max + 1
	}
	if max >= d.lastIndex {
		d.lastIndex = min - 1
	}
	if d.firstIndex > d.lastIndex {
		d.firstIndex = 0
		d.lastIndex = 0
	}
	return nil
}
