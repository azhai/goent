package utils

import (
	"iter"
	"sync"
)

type CoMap struct {
	mu   sync.RWMutex
	data map[string]any
}

func NewCoMap() *CoMap {
	return &CoMap{
		data: make(map[string]any),
	}
}

func (m *CoMap) Get(key string) (any, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	return v, ok
}

func (m *CoMap) Set(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
}

func (m *CoMap) Update(data map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range data {
		m.data[k] = v
	}
}

func (m *CoMap) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *CoMap) Each() iter.Seq2[string, any] {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return func(yield func(string, any) bool) {
		for k, v := range m.data {
			if !yield(k, v) {
				return
			}
		}
	}
}
