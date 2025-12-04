package sql

import (
	"testing"
)

func TestParserCreateTable(t *testing.T) {
	sql := "CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);"
	parser := NewParser(sql)
	statement, err := parser.Parse()
	if err != nil {
		t.Error(err)
		return
	}
	creatData := statement.(*CreatTableData)
	creatData.Statement()
}
func TestParserInsert(t *testing.T) {
	sql := "INSERT INTO user (id, name, age) VALUES (1, 'zhangsan', 18),(2, 'lisi', 23);"
	parser := NewParser(sql)
	statement, err := parser.Parse()
	if err != nil {
		t.Error(err)
		return
	}
	insertData := statement.(*InsertData)
	insertData.Statement()
}
func TestParserSelect(t *testing.T) {
	sql := "SELECT * FROM user;"
	parser := NewParser(sql)
	statement, err := parser.Parse()
	if err != nil {
		t.Error(err)
		return
	}
	selectData := statement.(*SelectData)
	selectData.Statement()
}

func TestParserSelectWhere(t *testing.T) {
	sql := "SELECT * FROM user where id = 1 group by a order by b limit 12 offset 2;"
	parser := NewParser(sql)
	statement, err := parser.Parse()
	if err != nil {
		t.Error(err)
		return
	}
	selectData := statement.(*SelectData)
	selectData.Statement()
}

func TestParserExplain(t *testing.T) {
	sql := "Explain select * from user;"
	parser := NewParser(sql)
	statement, err := parser.Parse()
	if err != nil {
		t.Error(err)
		return
	}
	explainData := statement.(*ExplainData)
	explainData.Statement()
}
