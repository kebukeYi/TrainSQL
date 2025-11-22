package server

import (
	"bytes"
	"encoding/gob"
	"github.com/kebukeYi/TrainSQL/sql/server/executor"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/kebukeYi/TrainSQL/storage"
)

type Service interface {
	CreateRow(tableName string, row types.Row)
	ScanTable(tableName string) []types.Row
	CreateTable(table *types.Table)
	DropTable(tableName string)
	GetTable(tableName string) *types.Table
	MustGetTable(tableName string) *types.Table
	Execute(e executor.Executor) types.ResultSet
	Commit()
	Rollback()
	UpdateRow(table *types.Table, value types.Value, row []types.Value)
	DeleteRow(table *types.Table, value types.Value)
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
func (s *KVService) CreateRow(tableName string, row types.Row) {
	table := s.MustGetTable(tableName)
	for i, column := range table.Columns {
		dateType := row[i].DateType()
		if dateType == types.Null {
			if column.Nullable == true {
				continue
			} else {
				util.Error("[CreateRow] column %s can not be null", column.Name)
			}
		}
		if dateType != column.DataType {
			util.Error("[CreateRow] column type not match")
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
func (s *KVService) ScanTable(tableName string) []types.Row {
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
func (s *KVService) CreateTable(table *types.Table) {
	if getTable := s.GetTable(table.Name); getTable != nil {
		util.Error("table already exists")
	}
	if table.Columns == nil {
		util.Error("[CreateTable] table columns is nil")
	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(table); err != nil {
		util.Error("encode table error")
	}
	tableNameKey := GetTableNameKey(table.Name)
	s.txn.Set(tableNameKey, buffer.Bytes())
}
func (s *KVService) DropTable(tableName string) {
}
func (s *KVService) GetTable(tableName string) *types.Table {
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
func (s *KVService) MustGetTable(tableName string) *types.Table {
	table := s.GetTable(tableName)
	if table == nil {
		util.Error("table not exists")
	}
	return table
}
func (s *KVService) Commit() {
	s.txn.Commit()
}
func (s *KVService) Rollback() {
	s.txn.Rollback()
}
