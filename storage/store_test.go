package storage

import (
	"bytes"
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/rosedblabs/rosedb/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

var storageDirPath = "/usr/golanddata/trainsql/storage"

func TestAscendGreaterOrEqual(t *testing.T) {
	// Create a test database instance
	util.ClearPath(storageDirPath)
	options := rosedb.DefaultOptions
	options.DirPath = storageDirPath

	db, err := rosedb.Open(options)
	assert.Nil(t, err)
	defer db.Close()

	// Insert some test data
	data := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("apple"), []byte("value1")},
		{[]byte("banana"), []byte("value2")},
		{[]byte("cherry"), []byte("value3")},
		{[]byte("date"), []byte("value4")},
		{[]byte("grape"), []byte("value5")},
		{[]byte("kiwi"), []byte("value6")},
	}

	for _, d := range data {
		if err := db.Put(d.key, d.value); err != nil {
			t.Fatalf("Failed to put data: %v", err)
		}
	}

	// Test AscendRange
	var resultAscendRange []string
	start := []byte("banana")
	end := []byte("grape")
	count := 0
	//db.AscendRange(start, end, func(k []byte, v []byte) (bool, error) {
	db.AscendGreaterOrEqual(start, func(k []byte, v []byte) (bool, error) {
		count++
		if bytes.Compare(k, end) == 1 {
			return false, nil
		}
		resultAscendRange = append(resultAscendRange, string(k))
		return true, nil
	})
	assert.Equal(t, []string{"banana", "cherry", "date", "grape"}, resultAscendRange)
	// 6个元素, 满足的有5个, 最终 count = 5;
	fmt.Println("AscendRange count:", count)
}

func TestAscendKeysOfPattern(t *testing.T) {
	util.ClearPath(storageDirPath)
	options := rosedb.DefaultOptions
	options.DirPath = storageDirPath
	db, err := rosedb.Open(options)
	assert.Nil(t, err)
	defer db.Close()

	err = db.Put([]byte("seafood"), []byte{1})
	err = db.Put([]byte("seafoo"), []byte{2})
	err = db.Put([]byte("sefoo"), []byte{3})
	err = db.Put([]byte("foose"), []byte{4})
	err = db.Put([]byte("fooseer"), []byte{5})
	assert.Nil(t, err)

	validate := func(targetKey [][]byte, targetValue [][]byte, pattern []byte) {
		var keys [][]byte
		var values [][]byte
		db.AscendKeys(pattern, true, func(key []byte, value []byte) (bool, error) {
			//if strings.HasPrefix(string(key), string(pattern)) {
			//	return true, nil
			//}
			keys = append(keys, key)
			values = append(values, value)
			return true, nil
		})
		assert.Equal(t, targetKey, keys)
		assert.Equal(t, targetValue, values)
	}

	//validate([][]byte{[]byte("aacd")}, nil)
	//str := "se.*" // 全部任何位置匹配;
	str := "^se" // 只有前缀允许匹配;
	//str := "se"  // 使用strings.HasPrefix(), 非正则表达式判断;
	//validate([][]byte{[]byte("seafood"), []byte("seafoo"), []byte("sefoo")}, []byte(str))
	validate([][]byte{[]byte("seafoo"), []byte("seafood"), []byte("sefoo")}, [][]byte{{2}, {1}, {3}}, []byte(str))

}

func TestPrefixDeleteKey(t *testing.T) {
	storage := NewMemoryStorage()
	storage.Set([]byte("key"), []byte("value"))
	storage.Set([]byte("key1"), []byte("value1"))
	storage.Set([]byte("key2"), []byte("value2"))
	storage.Delete([]byte("key1"))
	resultPairs := storage.ScanPrefix([]byte("key"), true)
	for _, pair := range resultPairs {
		fmt.Println(pair.ToString())
	}
	assert.Equal(t, []*ResultPair{
		{
			Key:   []byte("key"),
			Value: []byte("value"),
		},
		{
			Key:   []byte("key2"),
			Value: []byte("value2"),
		},
	}, resultPairs)

}

func testPointOpt(t *testing.T, storage Storage) {
	assert.Equal(t, storage.Get([]byte("not exist")), []byte(nil))

	storage.Set([]byte("key"), []byte("value"))
	assert.Equal(t, storage.Get([]byte("key")), []byte("value"))

	storage.Set([]byte("key"), []byte("value1"))
	assert.Equal(t, storage.Get([]byte("key")), []byte("value1"))
	storage.Delete([]byte("key"))
	assert.Equal(t, storage.Get([]byte("key")), []byte(nil))

	// key、value 为空的情况
	assert.Equal(t, storage.Get([]byte(nil)), []byte(nil))
	storage.Set([]byte(nil), []byte(nil))
	assert.Equal(t, storage.Get([]byte(nil)), []byte(nil))
}

func testScan(t *testing.T, storage Storage) {
	storage.Set([]byte("nnaes"), []byte("value"))
	storage.Set([]byte("amhue"), []byte("value"))
	storage.Set([]byte("meeae"), []byte("value"))
	storage.Set([]byte("uujeh"), []byte("value"))
	storage.Set([]byte("anehe"), []byte("value"))
	storage.Set([]byte("enehe"), []byte("value"))
	storage.Set([]byte("ynehe"), []byte("value"))
	storage.Set([]byte("znehe"), []byte("value"))

	data0 := []*ResultPair{
		{
			Key:   []byte("amhue"),
			Value: []byte("value"),
		},
		{
			Key:   []byte("anehe"),
			Value: []byte("value"),
		},
	}

	assert.Equal(t, data0, storage.Scan(&RangeBounds{StartKey: []byte("a"), EndKey: []byte("e")}))

	data1 := []*ResultPair{
		{
			Key:   []byte("enehe"),
			Value: []byte("value"),
		},
		{
			Key:   []byte("meeae"),
			Value: []byte("value"),
		},
		{
			Key:   []byte("nnaes"),
			Value: []byte("value"),
		},
		{
			Key:   []byte("uujeh"),
			Value: []byte("value"),
		},
	}

	assert.Equal(t, data1, storage.Scan(&RangeBounds{StartKey: []byte("b"), EndKey: []byte("x")}))
}

func testScanPrefix(t *testing.T, storage Storage) {
	storage.Set([]byte("ccnaes"), []byte("value1"))
	storage.Set([]byte("camhue"), []byte("value2"))
	storage.Set([]byte("deeae"), []byte("value3"))
	storage.Set([]byte("uujeh"), []byte("value4"))
	storage.Set([]byte("eeujeh"), []byte("value5"))
	storage.Set([]byte("canehe"), []byte("value6"))
	storage.Set([]byte("aanehe"), []byte("value7"))
	storage.Set([]byte("yynehe"), []byte("value8"))
	data1 := []*ResultPair{
		{
			Key:   []byte("camhue"),
			Value: []byte("value2"),
		},
		{
			Key:   []byte("canehe"),
			Value: []byte("value6"),
		},
	}
	assert.Equal(t, data1, storage.ScanPrefix([]byte("ca"), true))
}

func TestMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()
	testPointOpt(t, storage)
	testScan(t, storage)
	testScanPrefix(t, storage)
}

func TestDiskStorage(t *testing.T) {
	util.ClearPath(storageDirPath)
	storage := NewDiskStorage(storageDirPath)
	defer storage.Close()
	testPointOpt(t, storage)
	testScan(t, storage)
	testScanPrefix(t, storage)
}
