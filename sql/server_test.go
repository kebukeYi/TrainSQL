package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/kebukeYi/TrainSQL/storage"
	"testing"
)

var dirPath = "/usr/golanddata/trainsql/server"

func testKVServiceExecuteCreateTable(t *testing.T, storage storage.Storage) {
	server := NewServer(storage)
	session := server.Session()
	resultSet := session.Execute("create table test1(id int primary key , name varchar);")
	resultSet.ToString()
}

func testKVServiceExecuteInsertTable(t *testing.T, storage storage.Storage) {
	server := NewServer(storage)
	session := server.Session()
	session.Execute("create table test2(id int primary key, name varchar);")
	session.Execute("insert into test2(id,name) values(1, 'test');")
	resultSet := session.Execute("select * from test2;")
	resultSet.ToString()
}

func testKVServiceExecuteScan(t *testing.T, storage storage.Storage) {
	server := NewServer(storage)
	session := server.Session()
	session.Execute("create table test3(id int primary key, name varchar);")
	session.Execute("insert into test3(id,name) values(1, 'test');")
	session.Execute("insert into test3(id,name) values(2, 'text');")
	session.Execute("insert into test3(id,name) values(3, 'hh');")
	resultSet := session.Execute("select * from test3;")
	resultSet.ToString()
}

func testShowTables(t *testing.T, storage storage.Storage) {
	server := NewServer(storage)
	session := server.Session()
	set := session.Execute("begin;")
	fmt.Println(set.ToString())

	set = session.Execute("create table test4(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("create table test5(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("create table test6(id int primary key, name varchar);")
	fmt.Println(set.ToString())

	set = session.Execute("commit;")
	fmt.Println(set.ToString())

	showTableNames := session.ShowTableNames()
	fmt.Println(showTableNames)
}

func TestMemoryStorage(t *testing.T) {
	memoryStorage := storage.NewMemoryStorage()
	testKVServiceExecuteCreateTable(t, memoryStorage)
	testKVServiceExecuteInsertTable(t, memoryStorage)
	testKVServiceExecuteScan(t, memoryStorage)
	testShowTables(t, memoryStorage)
}

func TestDiskStorage(t *testing.T) {
	util.ClearPath(dirPath)
	diskStorage := storage.NewDiskStorage(dirPath)
	testKVServiceExecuteCreateTable(t, diskStorage)
	testKVServiceExecuteInsertTable(t, diskStorage)
	testKVServiceExecuteScan(t, diskStorage)
	testShowTables(t, diskStorage)
}
