package utils

import "sync"

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
