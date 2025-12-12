package storage

import (
	"github.com/rosedblabs/rosedb/v2"
	"strings"
	"sync"
)

type DiskStorage struct {
	lock sync.RWMutex
	db   *rosedb.DB
}

func NewDiskStorage(dirPath string) *DiskStorage {
	options := rosedb.DefaultOptions
	options.DirPath = dirPath
	db, _ := rosedb.Open(options)
	return &DiskStorage{lock: sync.RWMutex{}, db: db}
}
func (disk *DiskStorage) Get(key []byte) []byte {
	value, _ := disk.db.Get(key)
	return value
}
func (disk *DiskStorage) Set(key []byte, value []byte) {
	disk.db.Put(key, value)
}
func (disk *DiskStorage) Delete(key []byte) {
	disk.db.Delete(key)
}
func (disk *DiskStorage) Scan(bounds *RangeBounds) []*ResultPair {
	var result []*ResultPair
	disk.db.AscendRange(bounds.StartKey, bounds.EndKey, func(k []byte, v []byte) (bool, error) {
		result = append(result, &ResultPair{Key: k, Value: v})
		return true, nil
	})
	return result
}
func (disk *DiskStorage) ScanPrefix(keyPrefix []byte, needValue bool) []*ResultPair {
	var result []*ResultPair
	disk.db.Ascend(func(Key []byte, Value []byte) (bool, error) {
		if strings.HasPrefix(string(Key), string(keyPrefix)) {
			if needValue {
				result = append(result, &ResultPair{
					Key:   Key,
					Value: Value,
				})
			} else {
				result = append(result, &ResultPair{
					Key: Key,
				})
			}
		}
		return true, nil
	})
	return result
}
func (disk *DiskStorage) Lock() {
	disk.lock.Lock()
}
func (disk *DiskStorage) UnLock() {
	disk.lock.Unlock()
}
func (disk *DiskStorage) Close() error {
	if disk.db == nil {
		return nil
	}
	err := disk.db.Close()
	if err != nil {
		return err
	}
	disk.db = nil
	return nil
}
