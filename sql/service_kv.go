package sql

import (
	"bytes"
	"encoding/gob"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"github.com/kebukeYi/TrainSQL/storage"
)

var (
	Table_ = "Table_"
	Row_   = "Row_"
	Index_ = "Index_"
)

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
func (s *KVService) CreateRow(tableName string, row types.Row) error {
	table, err := s.MustGetTable(tableName)
	if err != nil {
		return util.Error("[CreateRow] table not exists")
	}
	// 校验 row 行每一列的的有效性;
	for i, column := range table.Columns {
		dateType := row[i].DateType()
		if dateType == types.Null {
			if column.Nullable == true {
				continue
			} else {
				return util.Error("[CreateRow] column %s can not be null", column.Name)
			}
		}
		if dateType != column.DataType {
			return util.Error("[CreateRow] column type not match")
		}
	}
	// 找到 此行的主键, 作为该行数据的唯一标识;
	pk := table.GetPrimaryKeyOfValue(row)
	// 查看主键对应的数据是否已经存在了;
	rowKey := GetRowKey(tableName, pk)
	// key: tableName_primaryKey 是否已经存在; Row_test1
	if get := s.txn.Get(rowKey); get != nil {
		return util.Error("[CreateRow] row already exists")
	}
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(row); err != nil {
		return util.Error("#CreateRow encode row error:%s", err)
	}
	err = s.txn.Set(rowKey, buffer.Bytes())
	if err != nil {
		return util.Error("#CreateRow set row error:%s", err)
	}
	// 维护索引, 主键索引需要维护吗?
	indexCol := make(map[int]types.ColumnV)
	for i, column := range table.Columns {
		if column.IsIndex {
			indexCol[i] = column
		}
	}
	// 多个索引;
	for i, column := range indexCol {
		loadIndex, err := s.LoadIndex(tableName, column.Name, row[i])
		if err != nil {
			return err
		}
		loadIndex = append(loadIndex, pk)
		err = s.SaveIndex(tableName, column.Name, row[i], loadIndex)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *KVService) ScanTable(tableName string, filter *types.Expression) ([]types.Row, error) {
	// prefixRowKey: Row_user
	// 扫描数据时, 需要过滤一些数据;
	prefixRowKey := GetPrefixRowKey(tableName)
	table, err := s.MustGetTable(tableName)
	if err != nil {
		return nil, err
	}
	resultPairs := s.txn.ScanPrefix(prefixRowKey, true)
	rows := make([]types.Row, 0)
	for _, resultPair := range resultPairs {
		row := types.Row{}
		value := resultPair.Value
		if len(value) == 0 || value == nil {
			continue
		}
		decoder := gob.NewDecoder(bytes.NewReader(value))
		if err := decoder.Decode(&row); err != nil {
			return nil, util.Error("#ScanTable decode row error")
		}
		if filter != nil {
			colNames := make([]string, 0)
			for _, column := range table.Columns {
				colNames = append(colNames, column.Name)
			}
			expr, err := types.EvaluateExpr(filter, colNames, row, colNames, row)
			if err != nil {
				return nil, err
			}
			switch expr.(type) {
			case *types.ConstNull:
			case *types.ConstBool:
				if expr.(*types.ConstBool).Value == true {
					rows = append(rows, row)
				} else {
				}
			default:
				return nil, util.Error("#ScanTable filter.EvaluateExpr Unexpected expression")
			}
		} else {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
func (s *KVService) CreateTable(table *types.Table) error {
	getTable, err := s.GetTable(table.Name)
	if err != nil {
		return err
	}
	if getTable != nil {
		return util.Error("#CreateTable table already exists")
	}
	table.Validate()

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(table); err != nil {
		return util.Error("#CreateTable encode table error")
	}
	// tableNameKey: Table_test
	tableNameKey := GetTableNameKey(table.Name)
	return s.txn.Set(tableNameKey, buffer.Bytes())
}
func (s *KVService) DropTable(tableName string) error {
	table, err := s.MustGetTable(tableName)
	if err != nil {
		return err
	}
	rows, err := s.ScanTable(tableName, nil)
	if err != nil {
		return err
	}
	for _, row := range rows {
		primaryKeyOfValue := table.GetPrimaryKeyOfValue(row)
		err = s.DeleteRow(table, primaryKeyOfValue)
		if err != nil {
			return err
		}
	}
	tableNameKey := GetTableNameKey(tableName)
	return s.txn.Delete(tableNameKey)
}
func (s *KVService) GetTable(tableName string) (*types.Table, error) {
	// tableNameKey : Table_test
	tableNameKey := GetTableNameKey(tableName)
	tableBytes := s.txn.Get(tableNameKey)
	if tableBytes == nil {
		return nil, nil
	}
	var buffer bytes.Buffer
	buffer.Write(tableBytes)
	decoder := gob.NewDecoder(&buffer)
	var table types.Table
	if err := decoder.Decode(&table); err != nil {
		return nil, err
	}
	return &table, nil
}
func (s *KVService) MustGetTable(tableName string) (*types.Table, error) {
	table, err := s.GetTable(tableName)
	if table == nil {
		if err != nil {
			// 解码错误
			return nil, err
		} else {
			return nil, util.Error("#MustGetTable table not exists")
		}
	}
	return table, nil
}
func (s *KVService) UpdateRow(table *types.Table, primaryId types.Value, row []types.Value) error {
	newPk := table.GetPrimaryKeyOfValue(row)
	if primaryId != newPk {
		err := s.DeleteRow(table, primaryId)
		if err != nil {
			return err
		}
		err = s.CreateRow(table.Name, row)
		if err != nil {
			return err
		}
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
		oldRow, err := s.ReadById(table.Name, primaryId)
		if err != nil {
			return err
		}
		if oldRow != nil {
			if oldRow[i] == row[i] {
				continue
			}
		}
		oldIndex, err := s.LoadIndex(table.Name, v.Name, oldRow[i])
		if err != nil {
			return err
		}
		oldIndex = types.Remove(oldIndex, oldRow[i])
		err = s.SaveIndex(table.Name, v.Name, oldRow[i], oldIndex)
		if err != nil {
			return err
		}
		newIndex, err := s.LoadIndex(table.Name, v.Name, row[i])
		if err != nil {
			return err
		}
		newIndex = append(newIndex, newPk)
		err = s.SaveIndex(table.Name, v.Name, row[i], newIndex)
		if err != nil {
			return err
		}
	}
	rowKey := GetRowKey(table.Name, newPk)
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(row); err != nil {
		return util.Error("#UpdateRow encode row error")
	}
	return s.txn.Set(rowKey, buffer.Bytes())
}
func (s *KVService) DeleteRow(table *types.Table, primaryIdDelete types.Value) error {
	indexCols := make(map[int]types.ColumnV)
	for i, column := range table.Columns {
		if column.IsIndex {
			indexCols[i] = column
		}
	}
	// 每一个索引都关联着 主键; 所以当删除主键时,也需要将索引关系剔除;
	for i, indexCol := range indexCols {
		row, err := s.ReadById(table.Name, primaryIdDelete)
		if err != nil {
			return err
		}
		if row != nil {
			index, err := s.LoadIndex(table.Name, indexCol.Name, row[i])
			if err != nil {
				return err
			}
			index = types.Remove(index, primaryIdDelete)
			err = s.SaveIndex(table.Name, indexCol.Name, row[i], index)
			if err != nil {
				return err
			}
		}
	}
	rowKey := GetRowKey(table.Name, primaryIdDelete)
	return s.txn.Delete(rowKey)
}
func (s *KVService) LoadIndex(tableName string, colName string, colVal types.Value) ([]types.Value, error) {
	indexKey := GetIndexKey(tableName, colName, colVal)
	values := s.txn.Get(indexKey)
	var index []types.Value
	if values != nil {
		var buffer bytes.Buffer
		buffer.Write(values)
		decoder := gob.NewDecoder(&buffer)
		if err := decoder.Decode(&index); err != nil {
			return nil, util.Error("#LoadIndex decode index error")
		}
		return index, nil
	}
	return nil, nil
}
func (s *KVService) ReadById(tableName string, primaryId types.Value) (types.Row, error) {
	rowKey := GetRowKey(tableName, primaryId)
	values := s.txn.Get(rowKey)
	if values != nil {
		var buffer bytes.Buffer
		buffer.Write(values)
		decoder := gob.NewDecoder(&buffer)
		var row types.Row
		if err := decoder.Decode(&row); err != nil {
			return nil, util.Error("#ReadById decode row error")
		}
		return row, nil
	}
	return nil, nil
}
func (s *KVService) SaveIndex(tableName string, colName string, colVal types.Value, indexSet []types.Value) error {
	indexKey := GetIndexKey(tableName, colName, colVal)
	if len(indexSet) == 0 {
		return s.txn.Delete(indexKey)
	} else {
		var buffer bytes.Buffer
		encoder := gob.NewEncoder(&buffer)
		if err := encoder.Encode(indexSet); err != nil {
			return util.Error("#SaveIndex encode index error")
		}
		return s.txn.Set(indexKey, buffer.Bytes())
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
		rawKey := GetTableName(pair.Key)
		names = append(names, string(rawKey))
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
func GetTableNameKey(tableName string) []byte {
	return []byte(Table_ + tableName)
}
func GetTableName(tableNameKey []byte) []byte {
	return tableNameKey[len(Table_):]
}
func GetTableNamePrefixKey() []byte {
	return []byte(Table_)
}
func GetRowKey(tableName string, value types.Value) []byte {
	buf := []byte(Row_)
	buf = append(buf, tableName...)
	buf = append(buf, value.Bytes()...) // Row_test1
	return buf
}
func GetPrefixRowKey(tableName string) []byte {
	// Row+user+ id1 +version
	// Row+user+ id2 +version
	// Row+user+ id3 +version
	return []byte(Row_ + tableName)
}
func GetIndexKey(tableName string, colName string, value types.Value) []byte {
	buf := []byte(Index_)
	buf = append(buf, tableName...)
	buf = append(buf, colName...)
	buf = append(buf, value.Bytes()...)
	return buf
}
