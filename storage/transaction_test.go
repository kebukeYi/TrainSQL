package storage

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var txnDirPath = "/usr/golanddata/trainsql/txn"

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

func TestReopen_unCommit(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t1 := transactionManager.Begin()         // version 1; active[];
	t1.Set([]byte("key1"), []byte("value1")) // version 1; active[1];
	t1.Set([]byte("key2"), []byte("value2"))
	// t1.Commit() //  version 1; active[];
	// 没有 commit; version 1; active[1]; // 并没有在活跃列表中删除掉;

	// 数据库关闭;
	err := transactionManager.Close()
	if err != nil {
		return
	}
	time.Sleep(3 * time.Second)
	// 重新启动数据库;
	storage := NewDiskStorage(txnDirPath)
	transactionManager = NewTransactionManager(storage)
	t2 := transactionManager.Begin() // 会重新读取到 之前t1到 活跃列表中;因此避免了脏读发生;
	t2Val := t2.Get([]byte("key1"))
	fmt.Println(string(t2Val))
	assert.Equal(t, []byte(nil), t2Val)
}

func TestReopen_commit(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t1 := transactionManager.Begin()         // version 1; active[];
	t1.Set([]byte("key1"), []byte("value1")) // version 1; active[1];
	t1.Set([]byte("key2"), []byte("value2"))
	// t1.Commit() //  version 1; active[];
	t1.Commit() // version 1; active[];

	// 数据库关闭;
	err := transactionManager.Close()
	if err != nil {
		return
	}
	time.Sleep(3 * time.Second)
	// 重新启动数据库;
	storage := NewDiskStorage(txnDirPath)
	transactionManager = NewTransactionManager(storage)
	t2 := transactionManager.Begin() //
	t2Val := t2.Get([]byte("key1"))
	fmt.Println(string(t2Val))
	assert.Equal(t, []byte("value1"), t2Val)
}

func TestGet_RR(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t1 := transactionManager.Begin()         // version 1; active[];
	t1.Set([]byte("key1"), []byte("value1")) // version 1; active[1];
	t1.Set([]byte("key2"), []byte("value2")) // version 1; active[1];
	t1.Set([]byte("key2"), []byte("value3"))
	t1.Set([]byte("key3"), []byte("value4"))
	t1.Commit() // version 1; active[];

	// t2 启动什么也不做;
	t2 := transactionManager.Begin() // version 2; active[];

	t3 := transactionManager.Begin() // version 3; active[2];
	t3.Set([]byte("key1"), []byte("value1-1"))
	t3.Commit()

	// t2 读取时, 只会扫描 小于等于自己版本区间的数据, 因此 t3的数据根本扫描不到;
	// 无论 后来的事务提交与否, t2 读取的值都是 之前事务提交写入的;
	t2Val := t2.Get([]byte("key1")) // version 2; active[];
	fmt.Println(string(t2Val))
	assert.Equal(t, []byte("value1"), t2Val)
}

func TestGet_RR_visible(t *testing.T) {
	transactionManager := NewTransactionManager(GetDiskStorage(txnDirPath))
	t0 := transactionManager.Begin()
	t0.Set([]byte("key1"), []byte("value1"))
	t0.Commit()

	t1 := transactionManager.Begin() // version 2; active[2]
	t2 := transactionManager.Begin() // version 3; active[2,3]
	t2Val := t2.Get([]byte("key1"))
	fmt.Println(string(t2Val)) // value1
	assert.Equal(t, []byte("value1"), t2Val)

	// t1 去更新值,并提交;
	t1.Set([]byte("key1"), []byte("value1-1"))
	t1.Commit() // t1.Commit() 提交与否, 都不会改变 t2 的活跃事务列表; 因此t2 仍然读取不到t1提交的值;
	// t2 读取时, 会扫描到 t1 提交的值, 但是t1还在t2的活跃事务列表中, 所以处于不可见状态;
	t2Val = t2.Get([]byte("key1"))
	fmt.Println(string(t2Val)) // value1
	assert.Equal(t, []byte("value1"), t2Val)
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
