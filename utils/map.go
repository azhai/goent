package utils

import (
	"iter"
	"maps"
	"sync"
)

// CoMap is a thread-safe concurrent map with read-write locking.
type CoMap[K int | int64 | string, V any] struct {
	mu   sync.RWMutex
	data map[K]*V
}

func NewCoMap[K int | int64 | string, V any]() *CoMap[K, V] {
	return &CoMap[K, V]{
		data: make(map[K]*V),
	}
}

func (m *CoMap[K, V]) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

func (m *CoMap[K, V]) Get(key K) (*V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *CoMap[K, V]) Set(key K, value *V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *CoMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}

func (m *CoMap[K, V]) Update(data map[K]*V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	maps.Copy(m.data, data)
}

func (m *CoMap[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]K, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *CoMap[K, V]) Each() iter.Seq2[K, *V] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return func(yield func(K, *V) bool) {
		for k, v := range m.data {
			if !yield(k, v) {
				return
			}
		}
	}
}
