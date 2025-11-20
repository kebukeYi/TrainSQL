package server

import (
	"practiceSQL/sql/util"
	"practiceSQL/storage"
	"testing"
)

var dirPath = "/usr/golanddata/practiceSQL/server"

func TestKVService_ExecuteCreateTable(t *testing.T) {
	util.ClearPath(dirPath)
	// diskStorage := storage.NewDiskStorage(dirPath)
	diskStorage := storage.NewMemoryStorage()
	kvServer := NewKVServer(diskStorage)
	session := kvServer.Session()
	resultSet := session.Execute("create table test(id int, name varchar);")
	resultSet.ToString()
}

func TestKVService_ExecuteInsertTable(t *testing.T) {
	util.ClearPath(dirPath)
	diskStorage := storage.NewDiskStorage(dirPath)
	kvServer := NewKVServer(diskStorage)
	session := kvServer.Session()
	session.Execute("create table test(id int, name varchar);")
	session.Execute("insert into test (id,name) values(1, 'test');")
	resultSet := session.Execute("select * from test;")
	resultSet.ToString()
}

func TestKVService_ExecuteScan(t *testing.T) {
	util.ClearPath(dirPath)
	diskStorage := storage.NewDiskStorage(dirPath)
	kvServer := NewKVServer(diskStorage)
	session := kvServer.Session()
	session.Execute("create table test(id int, name varchar);")
	session.Execute("insert into test(id,name) values(1, 'test');")
	session.Execute("insert into test(id,name) values(2, 'text');")
	session.Execute("insert into test(id,name) values(3, 'hh');")
	resultSet := session.Execute("select * from test;")
	resultSet.ToString()
}
