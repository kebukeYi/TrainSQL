package sql

import (
	"testing"
)

func TestParserCreateTable(t *testing.T) {
	sql := "CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);"
	parser := NewParser(sql)
	statement := parser.Parse()
	creatData := statement.(*CreatTableData)
	creatData.Execute(nil)
}
func TestParserInsert(t *testing.T) {
	sql := "INSERT INTO user (id, name, age) VALUES (1, 'zhangsan', 18),(2, 'lisi', 23);"
	parser := NewParser(sql)
	statement := parser.Parse()
	insertData := statement.(*InsertData)
	insertData.Execute(nil)
}
func TestParserSelect(t *testing.T) {
	sql := "SELECT * FROM user;"
	parser := NewParser(sql)
	statement := parser.Parse()
	selectData := statement.(*SelectData)
	selectData.Execute(nil)
}
