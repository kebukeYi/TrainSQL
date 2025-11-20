package storage

import (
	"github.com/stretchr/testify/assert"
	"practiceSQL/sql/util"
	"testing"
)

var txnDirPath = "/usr/golanddata/practiceSQL/txn"

func GetDiskStorage(txnDirPath string) *DiskStorage {
	util.ClearPath(txnDirPath)
	return NewDiskStorage(txnDirPath)
}

func TestTransaction_get(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Set([]byte("key4"), []byte("value4"))
	t0.Delete([]byte("key2"))
	assert.Equal(t, []byte("value1"), t0.Get([]byte("key1")))
	assert.Equal(t, []byte(nil), t0.Get([]byte("key2")))
	t0.Commit()

	t1 := transactionManager.Begin()
	t1.Set([]byte("key1"), []byte("value1-1"))
	assert.Equal(t, []byte("value1-1"), t1.Get([]byte("key1")))
	assert.Equal(t, []byte(nil), t1.Get([]byte("key2")))
	assert.Equal(t, []byte("value3"), t1.Get([]byte("key3")))
	t1.Commit()
}

func TestGet_isolation(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key2"), []byte("value3"))
	t0.Set([]byte("key3"), []byte("value4"))
	t0.Commit()

	t1 := transactionManager.Begin()
	t1.Set([]byte("key1"), []byte("value2"))

	t2 := transactionManager.Begin()
	t3 := transactionManager.Begin()

	t3.Set([]byte("key2"), []byte("value4"))
	t3.Delete([]byte("key3"))
	t3.Commit()

	assert.Equal(t, []byte("value1"), t2.Get([]byte("key1")))
	assert.Equal(t, []byte("value3"), t2.Get([]byte("key2")))
	assert.Equal(t, []byte("value4"), t2.Get([]byte("key3")))
}

func TestScanPreFix(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("aabb"), []byte("value1"))
	t0.Set([]byte("abcc"), []byte("value2"))
	t0.Set([]byte("bbaa"), []byte("value3"))
	t0.Set([]byte("acca"), []byte("value4"))
	t0.Set([]byte("aaca"), []byte("value5"))
	t0.Set([]byte("bbca"), []byte("value6"))
	t0.Commit()

	data := []ResultPair{
		{Key: []byte("aabb"), Value: []byte("value1")},
		{Key: []byte("aaca"), Value: []byte("value5")},
	}

	t1 := transactionManager.Begin()
	pairs := t1.ScanPrefix([]byte("aa"), true)
	assert.Equal(t, data, pairs)

	data = []ResultPair{
		{Key: []byte("aabb"), Value: []byte("value1")},
		{Key: []byte("aaca"), Value: []byte("value5")},
		{Key: []byte("abcc"), Value: []byte("value2")},
		{Key: []byte("acca"), Value: []byte("value4")},
	}
	t2 := transactionManager.Begin()
	pairs = t2.ScanPrefix([]byte("a"), true)
	assert.Equal(t, data, pairs)

	data = []ResultPair{
		{Key: []byte("bbca"), Value: []byte("value6")},
	}
	t3 := transactionManager.Begin()
	pairs = t3.ScanPrefix([]byte("bbca"), true)
	assert.Equal(t, data, pairs)

	data = []ResultPair{}
	t4 := transactionManager.Begin()
	pairs = t4.ScanPrefix([]byte("c"), true)
	assert.Equal(t, data, pairs)
}

func TestScan_isolation(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("aabb"), []byte("value1"))
	t0.Set([]byte("abcc"), []byte("value2"))
	t0.Set([]byte("bbaa"), []byte("value3"))
	t0.Set([]byte("acca"), []byte("value4"))
	t0.Set([]byte("aaca"), []byte("value5"))
	t0.Set([]byte("bbca"), []byte("value6"))
	t0.Commit()
	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()
	t2.Set([]byte("acca"), []byte("value4-1"))
	t2.Set([]byte("aabb"), []byte("value1-1"))

	t3 := transactionManager.Begin()
	t3.Set([]byte("bbaa"), []byte("value3-1"))
	t3.Delete([]byte("bbaa"))
	t3.Commit()

	data1 := []ResultPair{
		{Key: []byte("aabb"), Value: []byte("value1")},
		{Key: []byte("aaca"), Value: []byte("value5")},
	}
	assert.Equal(t, data1, t1.ScanPrefix([]byte("aa"), true))

	data2 := []ResultPair{
		{Key: []byte("aabb"), Value: []byte("value1")},
		{Key: []byte("aaca"), Value: []byte("value5")},
		{Key: []byte("abcc"), Value: []byte("value2")},
		{Key: []byte("acca"), Value: []byte("value4")},
	}
	assert.Equal(t, data2, t1.ScanPrefix([]byte("a"), true))

	data3 := []ResultPair{
		{Key: []byte("bbca"), Value: []byte("value6")},
	}
	assert.Equal(t, data3, t1.ScanPrefix([]byte("bbca"), true))
}

func TestTransaction_set(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key2"), []byte("value3"))
	t0.Set([]byte("key3"), []byte("value4"))
	t0.Set([]byte("key4"), []byte("value5"))
	t0.Commit()

	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()
	t1.Set([]byte("key1"), []byte("value1-1"))
	t1.Set([]byte("key2"), []byte("value3-1"))
	t1.Set([]byte("key2"), []byte("value3-2"))

	t2.Set([]byte("key3"), []byte("value4-1"))
	t2.Set([]byte("key4"), []byte("value5-1"))
	t1.Commit()
	t2.Commit()

	t3 := transactionManager.Begin()
	assert.Equal(t, []byte("value1-1"), t3.Get([]byte("key1")))
	assert.Equal(t, []byte("value3-2"), t3.Get([]byte("key2")))
	assert.Equal(t, []byte("value4-1"), t3.Get([]byte("key3")))
	assert.Equal(t, []byte("value5-1"), t3.Get([]byte("key4")))
}

func TestSet_conflict(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key2"), []byte("value3"))
	t0.Set([]byte("key3"), []byte("value4"))
	t0.Set([]byte("key4"), []byte("value5"))
	t0.Commit()

	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()

	t1.Set([]byte("key1"), []byte("value1-1"))
	t1.Set([]byte("key1"), []byte("value1-2"))

	assert.Error(t, util.WriteConflict, t2.Set([]byte("key1"), []byte("value1-3")))

	t3 := transactionManager.Begin()
	t3.Set([]byte("key5"), []byte("value6"))
	t3.Commit()
	assert.Equal(t, util.WriteConflict, t1.Set([]byte("key5"), []byte("value6-1")))
	t1.Commit()
}

func TestTransaction_Delete(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Delete([]byte("key2"))
	t0.Delete([]byte("key3"))
	t0.Set([]byte("key3"), []byte("value3-1"))
	t0.Commit()

	t1 := transactionManager.Begin()
	t1.Get([]byte("key2"))
	assert.Equal(t, []byte(nil), t1.Get([]byte("key2")))

	data1 := []ResultPair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key3"), Value: []byte("value3-1")},
	}
	assert.Equal(t, data1, t1.ScanPrefix([]byte("ke"), true))
}

func TestTransaction_Delete_Conflict(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Commit()
	t1 := transactionManager.Begin()
	t1.Delete([]byte("key1"))
	t1.Set([]byte("key2"), []byte("value2-1"))

	t2 := transactionManager.Begin()
	assert.Error(t, util.WriteConflict, t2.Delete([]byte("key1")))
	assert.Error(t, util.WriteConflict, t2.Delete([]byte("key2")))
}

func TestDirty_read(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Commit()
	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()
	t2.Set([]byte("key1"), []byte("value1-1"))

	// tx1 can not read tx2's uncommit val;
	assert.Equal(t, []byte("value1"), t1.Get([]byte("key1")))
}

func TestUnrepeatable_read(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Commit()
	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()
	t2.Set([]byte("key1"), []byte("value1-1"))

	assert.Equal(t, []byte("value1"), t1.Get([]byte("key1")))
	t2.Commit()
	assert.Equal(t, []byte("value1"), t1.Get([]byte("key1")))
}

func TestPhantom_read(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Commit()
	t1 := transactionManager.Begin()
	t2 := transactionManager.Begin()
	data1 := []ResultPair{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	assert.Equal(t, data1, t1.ScanPrefix([]byte("key"), true))

	t2.Set([]byte("key2"), []byte("value2-1"))
	t2.Set([]byte("key4"), []byte("value4"))
	t2.Commit()
	assert.Equal(t, data1, t1.ScanPrefix([]byte("key"), true))
}

func TestTransaction_Rollback(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Set([]byte("key2"), []byte("value2"))
	t0.Set([]byte("key3"), []byte("value3"))
	t0.Commit()

	t1 := transactionManager.Begin()
	t1.Set([]byte("key1"), []byte("value1-1"))
	t1.Set([]byte("key2"), []byte("value2-1"))
	t1.Set([]byte("key3"), []byte("value3-1"))
	t1.Rollback()

	t2 := transactionManager.Begin()
	assert.Equal(t, []byte("value1"), t2.Get([]byte("key1")))
	assert.Equal(t, []byte("value2"), t2.Get([]byte("key2")))
	assert.Equal(t, []byte("value3"), t2.Get([]byte("key3")))
}
