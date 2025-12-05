package sql

import "github.com/kebukeYi/TrainSQL/sql/types"

var (
	Table_ = "Table_"
	Row_   = "Row_"
	Index_ = "Index_"
)

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
