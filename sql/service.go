package sql

import (
	"bytes"
	"encoding/gob"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/kebukeYi/TrainSQL/storage"
)

type Service interface {
	Commit()
	Rollback()
	Version() uint64
	CreateRow(tableName string, row types.Row)
	UpdateRow(table *types.Table, value types.Value, row []types.Value)
	DeleteRow(table *types.Table, value types.Value)
	ScanTable(tableName string, filter *types.Expression) []types.Row
	LoadIndex(name string, filed string, value types.Value) []types.Value
	SaveIndex(tableName string, colName string, value types.Value, indexSet []types.Value)
	ReadById(name string, index types.Value) types.Row
	CreateTable(table *types.Table)
	DropTable(tableName string)
	GetTableNames() []string
	GetTable(tableName string) *types.Table
	MustGetTable(tableName string) *types.Table
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
	// 校验 row 行每一列的的有效性;
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
	// 找到 此行的主键, 作为该行数据的唯一标识;
	pk := table.GetPrimaryKeyOfValue(row)
	// 查看主键对应的数据是否已经存在了;
	rowKey := GetRowKey(tableName, pk)
	// key: tableName_primaryKey 是否已经存在; Row_test1
	if get := s.txn.Get(rowKey); get != nil {
		util.Error("[CreateRow] row already exists")
	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(row); err != nil {
		util.Error("encode row error:%s", err)
	}
	s.txn.Set(rowKey, buffer.Bytes())
	// 维护索引, 主键索引需要维护吗?
	indexCol := make(map[int]types.ColumnV)
	for i, column := range table.Columns {
		if column.IsIndex {
			indexCol[i] = column
		}
	}
	// 多个索引;
	for i, column := range indexCol {
		loadIndex := s.LoadIndex(tableName, column.Name, row[i])
		loadIndex = append(loadIndex, pk)
		s.SaveIndex(tableName, column.Name, row[i], loadIndex)
	}
}
func (s *KVService) ScanTable(tableName string, filter *types.Expression) []types.Row {
	// prefixRowKey: Row_user
	// 扫描数据时, 需要过滤一些数据;
	prefixRowKey := GetPrefixRowKey(tableName)
	table := s.MustGetTable(tableName)
	resultPairs := s.txn.ScanPrefix(prefixRowKey, true)
	rows := make([]types.Row, 0)
	for _, resultPair := range resultPairs {
		row := types.Row{}
		decoder := gob.NewDecoder(bytes.NewReader(resultPair.Value))
		if err := decoder.Decode(&row); err != nil {
			util.Error("decode row error")
		}
		if filter != nil {
			colNames := make([]string, 0)
			for _, column := range table.Columns {
				colNames = append(colNames, column.Name)
			}
			expr := types.EvaluateExpr(filter, colNames, row, colNames, row)
			switch expr.(type) {
			case *types.ConstNull:
			case *types.ConstBool:
				if expr.(*types.ConstBool).Value == true {
					rows = append(rows, row)
				} else {
				}
			default:
				util.Error("[ScanTable] FilterExecutor.Execute Unexpected expression")
			}
		} else {
			rows = append(rows, row)
		}
	}
	return rows
}
func (s *KVService) CreateTable(table *types.Table) {
	if getTable := s.GetTable(table.Name); getTable != nil {
		util.Error("table already exists")
		return
	}
	table.Validate()

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(table); err != nil {
		util.Error("encode table error")
	}
	// tableNameKey: Table_test
	tableNameKey := GetTableNameKey(table.Name)
	s.txn.Set(tableNameKey, buffer.Bytes())
}
func (s *KVService) DropTable(tableName string) {
	table := s.MustGetTable(tableName)
	rows := s.ScanTable(tableName, nil)
	for _, row := range rows {
		primaryKeyOfValue := table.GetPrimaryKeyOfValue(row)
		s.DeleteRow(table, primaryKeyOfValue)
	}
	tableNameKey := GetTableNameKey(tableName)
	s.txn.Delete(tableNameKey)
}
func (s *KVService) GetTable(tableName string) *types.Table {
	// tableNameKey : Table_test
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
func (s *KVService) UpdateRow(table *types.Table, primaryId types.Value, row []types.Value) {
	newPk := table.GetPrimaryKeyOfValue(row)
	if primaryId != newPk {
		s.DeleteRow(table, primaryId)
		s.CreateRow(table.Name, row)
	}
	// 没有更新主键的情况:
	// 查询当前表的所有索引列; 判断是否更新了索引列;
	indexCol := make(map[int]types.ColumnV)
	for i, column := range table.Columns {
		if column.IsIndex {
			indexCol[i] = column
		}
	}
	// update user set name="kk" where index=30;
	// update user set index="kk" where id=10;

	for i, v := range indexCol {
		oldRow := s.ReadById(table.Name, primaryId)
		if oldRow != nil {
			if oldRow[i] == row[i] {
				continue
			}
		}
		oldIndex := s.LoadIndex(table.Name, v.Name, oldRow[i])
		oldIndex = types.Remove(oldIndex, oldRow[i])
		s.SaveIndex(table.Name, v.Name, oldRow[i], oldIndex)
		newIndex := s.LoadIndex(table.Name, v.Name, row[i])
		newIndex = append(newIndex, newPk)
		s.SaveIndex(table.Name, v.Name, row[i], newIndex)
	}
	rowKey := GetRowKey(table.Name, newPk)
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(row); err != nil {
		util.Error("encode row error")
	}
	s.txn.Set(rowKey, buffer.Bytes())
}
func (s *KVService) DeleteRow(table *types.Table, primaryIdDelete types.Value) {
	indexCols := make(map[int]types.ColumnV)
	for i, column := range table.Columns {
		if column.IsIndex {
			indexCols[i] = column
		}
	}
	// 每一个索引都关联着 主键; 所以当删除主键时,也需要将索引关系剔除;
	for i, indexCol := range indexCols {
		row := s.ReadById(table.Name, primaryIdDelete)
		if row != nil {
			index := s.LoadIndex(table.Name, indexCol.Name, row[i])
			index = types.Remove(index, primaryIdDelete)
			s.SaveIndex(table.Name, indexCol.Name, row[i], index)
		}
	}
	rowKey := GetRowKey(table.Name, primaryIdDelete)
	s.txn.Delete(rowKey)
}
func (s *KVService) LoadIndex(tableName string, colName string, colVal types.Value) []types.Value {
	indexKey := GetIndexKey(tableName, colName, colVal)
	values := s.txn.Get(indexKey)
	var index []types.Value
	if values != nil {
		var buffer bytes.Buffer
		buffer.Write(values)
		decoder := gob.NewDecoder(&buffer)
		if err := decoder.Decode(&index); err != nil {
			util.Error("decode index error")
		}
		return index
	}
	return nil
}
func (s *KVService) ReadById(tableName string, primaryId types.Value) types.Row {
	rowKey := GetRowKey(tableName, primaryId)
	values := s.txn.Get(rowKey)
	if values != nil {
		var buffer bytes.Buffer
		buffer.Write(values)
		decoder := gob.NewDecoder(&buffer)
		var row types.Row
		if err := decoder.Decode(&row); err != nil {
			util.Error("decode row error")
		}
		return row
	}
	return nil
}
func (s *KVService) SaveIndex(tableName string, colName string, colVal types.Value, indexSet []types.Value) {
	indexKey := GetIndexKey(tableName, colName, colVal)
	if len(indexSet) == 0 {
		s.txn.Delete(indexKey)
	} else {
		var buffer bytes.Buffer
		encoder := gob.NewEncoder(&buffer)
		if err := encoder.Encode(indexSet); err != nil {
			util.Error("encode index error")
		}
		s.txn.Set(indexKey, buffer.Bytes())
	}
}
func (s *KVService) GetTableNames() []string {
	tablePrefixKey := GetTableNamePrefixKey()
	pairs := s.txn.ScanPrefix(tablePrefixKey, false)
	names := make([]string, 0)
	for _, pair := range pairs {
		//var buffer bytes.Buffer
		//buffer.Write(pair.Value)
		//decoder := gob.NewDecoder(&buffer)
		//var table types.Table
		//if err := decoder.Decode(&table); err != nil {
		//	util.Error("decode table error")
		//}
		names = append(names, string(pair.Key))
	}
	return names
}
func (s *KVService) Version() uint64 {
	return uint64(s.txn.Version())
}
func (s *KVService) Commit() {
	s.txn.Commit()
}
func (s *KVService) Rollback() {
	s.txn.Rollback()
}
