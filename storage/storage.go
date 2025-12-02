package storage

import (
	"bytes"
	"github.com/google/btree"
)

type RangeBounds struct {
	StartKey []byte
	EndKey   []byte
}

type ResultPair struct {
	Key   []byte
	Value []byte
}

func (r *ResultPair) Less(item btree.Item) bool {
	if item == nil {
		return false
	}
	return bytes.Compare(r.Key, item.(*ResultPair).Key) < 0
}

func (receiver *ResultPair) ToString() string {
	return "[" + string(receiver.Key) + ":" + string(receiver.Value) + "]"
}

type Storage interface {
	Lock()
	UnLock()
	Get(key []byte) []byte
	Set(key []byte, value []byte)
	Delete(key []byte)
	//Scan [greaterOrEqual, lessThan)
	Scan(bounds *RangeBounds) []*ResultPair
	// ScanPrefix [^prefix]
	ScanPrefix(keyPrefix []byte, needValue bool) []*ResultPair
	Close() error
}
