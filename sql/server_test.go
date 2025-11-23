package sql

import (
	"github.com/kebukeYi/TrainSQL/storage"
	"testing"
)

var dirPath = "/usr/golanddata/trainsql/server"

func TestKVService_ExecuteCreateTable(t *testing.T) {
	//util.ClearPath(dirPath)
	// storage := storage.NewDiskStorage(dirPath)
	storage := storage.NewMemoryStorage()
	kvServer := NewKVServer(storage)
	session := kvServer.Session()
	resultSet := session.Execute("create table test(id int primary key , name varchar);")
	resultSet.ToString()
}

func TestKVService_ExecuteInsertTable(t *testing.T) {
	//util.ClearPath(dirPath)
	//storage := storage.NewDiskStorage(dirPath)
	storage := storage.NewMemoryStorage()
	kvServer := NewKVServer(storage)
	session := kvServer.Session()
	session.Execute("create table test(id int primary key, name varchar);")
	session.Execute("insert into test (id,name) values(1, 'test');")
	resultSet := session.Execute("select * from test;")
	resultSet.ToString()
}

func TestKVService_ExecuteScan(t *testing.T) {
	//util.ClearPath(dirPath)
	//storage := storage.NewDiskStorage(dirPath)
	storage := storage.NewMemoryStorage()
	kvServer := NewKVServer(storage)
	session := kvServer.Session()
	session.Execute("create table test(id int primary key, name varchar);")
	session.Execute("insert into test(id,name) values(1, 'test');")
	session.Execute("insert into test(id,name) values(2, 'text');")
	session.Execute("insert into test(id,name) values(3, 'hh');")
	resultSet := session.Execute("select * from test;")
	resultSet.ToString()
}
