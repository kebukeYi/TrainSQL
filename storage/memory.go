package storage

import (
	"bytes"
	"github.com/google/btree"
	"strings"
	"sync"
)

type MemoryStorage struct {
	lock  sync.RWMutex
	btree *btree.BTree
}

type Pair struct {
	Key   []byte
	Value []byte
}

func (p *Pair) Less(item btree.Item) bool {
	if item == nil {
		return false
	}
	return bytes.Compare(p.Key, item.(*Pair).Key) < 0
}
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		lock:  sync.RWMutex{},
		btree: btree.New(32),
	}
}
func (m *MemoryStorage) Get(key []byte) []byte {
	item := m.btree.Get(&Pair{Key: key})
	if item != nil {
		return item.(*Pair).Value
	}
	return nil
}
func (m *MemoryStorage) Set(key []byte, value []byte) {
	m.btree.ReplaceOrInsert(&Pair{Key: key, Value: value})
}
func (m *MemoryStorage) Delete(key []byte) {
	m.btree.Delete(&Pair{Key: key})
}
func (m *MemoryStorage) Scan(bounds *RangeBounds) []*ResultPair {
	var result []*ResultPair
	m.btree.AscendRange(&Pair{Key: bounds.StartKey}, &Pair{Key: bounds.EndKey}, func(item btree.Item) bool {
		result = append(result, &ResultPair{
			Key:   item.(*Pair).Key,
			Value: item.(*Pair).Value,
		})
		return true
	})
	return result
}

func (m *MemoryStorage) ScanPrefix(keyPrefix []byte, isValue bool) []*ResultPair {
	var result []*ResultPair
	m.btree.Ascend(func(item btree.Item) bool {
		if strings.HasPrefix(string(item.(*Pair).Key), string(keyPrefix)) {
			if isValue {
				result = append(result, &ResultPair{
					Key:   item.(*Pair).Key,
					Value: item.(*Pair).Value,
				})
			} else {
				result = append(result, &ResultPair{
					Key: item.(*Pair).Key,
				})
			}
		}
		return true
	})
	return result
}

func (m *MemoryStorage) Close() {
}

func (m *MemoryStorage) Sync() {

}

func (m *MemoryStorage) Lock() {
	m.lock.Lock()
}

func (m *MemoryStorage) UnLock() {
	m.lock.Unlock()
}
