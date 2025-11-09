package util

import (
	"fmt"
	"os"
)

var FileManageTestDirectory = "/usr/golanddata/simpledb/file_manager"
var LogManageTestDirectory = "/usr/golanddata/simpledb/log_manager"
var RecordManageTestDirectory = "/usr/golanddata/simpledb/record_manager"
var BufferManageTestDirectory = "/usr/golanddata/simpledb/buffer_manager"
var MeatDataManageTestDirectory = "/usr/golanddata/simpledb/metadata_manager"
var QueryScanTestDirectory = "/usr/golanddata/simpledb/query_scan"
var TxTestDirectory = "/usr/golanddata/simpledb/tx"
var CCTestDirectory = "/usr/golanddata/simpledb/cc"

var IndexDirectory = "/usr/golanddata/simpledb/index"
var PlannerDirectory = "/usr/golanddata/simpledb/planner"

func ClearDir(path string) {
	if err := os.RemoveAll(path); err != nil {
		fmt.Printf("remove directory:%s error", path)
		panic(err)
	}
}
