package server

import "practiceSQL/sql/types"

var (
	Table_ = "Table_"
	Row_   = "Row_"
)

func GetTableNameKey(tableName string) []byte {
	return []byte(Table_ + tableName)
}
func GetTableNamePrefixKey(tableName string) []byte {
	return []byte(Table_ + tableName)
}

func GetRowKey(tableName string, value types.Value) []byte {
	buf := []byte(Row_)
	buf = append(buf, tableName...)
	buf = append(buf, value.Bytes()...)
	return buf
}
func GetPrefixRowKey(tableName string) []byte {
	// Row+user+ id1 +version
	// Row+user+ id2 +version
	// Row+user+ id3 +version
	return []byte(Row_ + tableName)
}
