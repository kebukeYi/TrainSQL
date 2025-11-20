package storage

import (
	"github.com/rosedblabs/rosedb/v2"
	"practiceSQL/sql/util"
	"sync"
)

type DiskStorage struct {
	lock sync.RWMutex
	// bitCast 模型 kv 数据库;
	db *rosedb.DB
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
	disk.db.AscendKeys(keyPrefix, needValue, func(k []byte, v []byte) (bool, error) {
		// 数据库遍历时, 只有满足keyPrefix前缀的key会进入到这里;随后进行保存;
		result = append(result, &ResultPair{Key: k, Value: v})
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
func (disk *DiskStorage) Close() {
	err := disk.db.Close()
	if err != nil {
		util.Error("[DiskStorage] close error")
	}
}
func (disk *DiskStorage) Sync() {
	err := disk.db.Sync()
	if err != nil {
		util.Error("[DiskStorage] sync error")
	}
}
