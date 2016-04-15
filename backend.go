package longpoll

import (
	"sync"
	"fmt"
)

type Backend interface {
	Init(string) error
	Drop(string) error
	Add(string, ...interface{}) error
	Drain(string) ([]interface{}, error)
	ContentSize(string) (int, error)
}

type MemoryBackend struct {
	sync.Mutex
	data    map[string][]interface{}
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{data: make(map[string][]interface{})}
}

func (mb *MemoryBackend) Init(id string) error {
	mb.Lock()
	defer mb.Unlock()
	if _, ok := mb.data[id]; !ok {
		mb.data[id] = []interface{}{}
	}
	return nil
}

func (mb *MemoryBackend) Drop(id string) error {
	mb.Lock()
	defer mb.Unlock()
	if _, ok := mb.data[id]; !ok {
		return fmt.Errorf("no container for Id %v", id)
	}
	delete(mb.data, id)
	return nil
}

func (mb *MemoryBackend) Add(id string, data ...interface{}) error {
	mb.Lock()
	defer mb.Unlock()
	if _, ok := mb.data[id]; !ok {
		return fmt.Errorf("no container for Id %v", id)
	}
	mb.data[id] = append(mb.data[id], data...)
	return nil
}

func (mb *MemoryBackend) Drain(id string) ([]interface{}, error) {
	mb.Lock()
	defer mb.Unlock()
	if _, ok := mb.data[id]; !ok {
		return nil, fmt.Errorf("no container for Id %v", id)
	}
	res := mb.data[id]
	mb.data[id] = []interface{}{}
	return res, nil
}

func (mb *MemoryBackend) ContentSize(id string) (int, error) {
	mb.Lock()
	defer mb.Unlock()
	if _, ok := mb.data[id]; !ok {
		return -1, fmt.Errorf("no container for Id %v", id)
	}
	return len(mb.data[id]), nil
}

