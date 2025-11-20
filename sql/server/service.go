package server

import (
	"bytes"
	"encoding/gob"
	"practiceSQL/sql/server/executor"
	"practiceSQL/sql/types"
	"practiceSQL/sql/util"
	"practiceSQL/storage"
)

type Service interface {
	createRow(tableName string, row types.Row)
	scanTable(tableName string) []types.Row
	createTable(table *types.Table)
	getTable(tableName string) *types.Table
	mustGetTable(tableName string) *types.Table
	execute(e executor.Executor) types.ResultSet
	commit()
	rollback()
}

type KVService struct {
	txn *storage.Transaction
}

func NewKVService(t *storage.Transaction) *KVService {
	gob.Register(&types.ConstInt{})
	gob.Register(&types.ConstNull{})
	gob.Register(&types.ConstBool{})
	gob.Register(&types.ConstFloat{})
	gob.Register(&types.ConstString{})
	return &KVService{
		txn: t,
	}
}
func (s *KVService) execute(e executor.Executor) types.ResultSet {
	switch e.(type) {
	case *executor.CreatTableExecutor:
		return s.ExecuteCreateTable(e)
	case *executor.InsertTableExecutor:
		return s.ExecuteInsertTable(e)
	case *executor.ScanTableExecutor:
		return s.ExecuteScan(e)
	}
	return nil
}
func (s *KVService) createRow(tableName string, row types.Row) {
	table := s.mustGetTable(tableName)
	for i, column := range table.Columns {
		dateType := row[i].DateType()
		if dateType == types.Null {
			if column.Nullable == true {
				continue
			} else {
				util.Error("[createRow] column %s can not be null", column.Name)
			}
		}
		if dateType != column.DataType {
			util.Error("[createRow] column type not match")
		}
	}
	rowKey := GetRowKey(tableName, row[0])
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(row); err != nil {
		util.Error("encode row error:%s", err)
	}
	s.txn.Set(rowKey, buffer.Bytes())
}
func (s *KVService) scanTable(tableName string) []types.Row {
	// prefixRowKey: Row_user_*
	prefixRowKey := GetPrefixRowKey(tableName)
	resultPairs := s.txn.ScanPrefix(prefixRowKey, true)
	rows := make([]types.Row, 0)
	for _, resultPair := range resultPairs {
		row := types.Row{}
		decoder := gob.NewDecoder(bytes.NewReader(resultPair.Value))
		if err := decoder.Decode(&row); err != nil {
			util.Error("decode row error")
		}
		rows = append(rows, row)
	}
	return rows
}
func (s *KVService) createTable(table *types.Table) {
	if getTable := s.getTable(table.Name); getTable != nil {
		util.Error("table already exists")
	}
	if table.Columns == nil {
		util.Error("[createTable] table columns is nil")
	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(table); err != nil {
		util.Error("encode table error")
	}
	tableNameKey := GetTableNameKey(table.Name)
	s.txn.Set(tableNameKey, buffer.Bytes())
}
func (s *KVService) getTable(tableName string) *types.Table {
	tableNameKey := GetTableNameKey(tableName)
	tableBytes := s.txn.Get(tableNameKey)
	if tableBytes == nil {
		return nil
	}
	var buffer bytes.Buffer
	buffer.Write(tableBytes)
	decoder := gob.NewDecoder(&buffer)
	var table types.Table
	if err := decoder.Decode(&table); err != nil {
		util.Error("decode table error")
	}
	return &table
}
func (s *KVService) mustGetTable(tableName string) *types.Table {
	table := s.getTable(tableName)
	if table == nil {
		util.Error("table not exists")
	}
	return table
}
func (s *KVService) commit() {
	s.txn.Commit()
}
func (s *KVService) rollback() {
	s.txn.Rollback()
}
