package parser

import (
	"fmt"
	"testing"
)

func TestTerm(t *testing.T) {
	lexer := NewSQLParser("a = 100")
	term := lexer.Term()
	fmt.Printf("term:%v\n", term)
}

func TestSelectSQL(t *testing.T) {
	sqlParser := NewSQLParser("select name,age,sex from student where age = 20 and sex = \"male\";")
	queryData := sqlParser.Select()
	for _, field := range queryData.fields {
		fmt.Printf("field:%v\n", field)
	}
	for _, table := range queryData.tables {
		fmt.Printf("table:%v\n", table)
	}
	fmt.Printf("pred:%v\n", queryData.pred)
}

func TestCreateTableSQL(t *testing.T) {
	sqlParser := NewSQLParser("create table student (stuID int, name varchar(255), age int, sex varchar(4) );")
	updateCmd := sqlParser.ParseCommand().(*CreateTableData)
	fmt.Printf("updateCmd.tblName:%v\n", updateCmd.tblName)
	fmt.Printf("updateCmd.sch:%v\n", updateCmd.sch)
}

func TestCreatIndexSQL(t *testing.T) {
	sqlParser := NewSQLParser("CREATE INDEX indexStuID ON student(stuID);")
	updateCmd := sqlParser.ParseCommand().(*IndexData)
	fmt.Printf("updateCmd.idxName:%v\n", updateCmd.idxName)
	fmt.Printf("updateCmd.tblName:%v\n", updateCmd.tblName)
	fmt.Printf("updateCmd.fldName:%v\n", updateCmd.fldName)
}

func TestInsertSQL(t *testing.T) {
	sqlParser := NewSQLParser("insert into student(stuID,name,age,sex) values(100, \"zhangsan\", 20, \"male\");")
	updateCmd := sqlParser.ParseCommand().(*InsertData)
	fmt.Printf("updateCmd.tblName:%v\n", updateCmd.tblName)
	fmt.Printf("updateCmd.flds:%v\n", updateCmd.flds)
	fmt.Printf("updateCmd.vals:%v\n", updateCmd.vals)
}

func TestDeleteSQL(t *testing.T) {
	sqlParser := NewSQLParser("delete from student where age = 20;")
	updateCmd := sqlParser.ParseCommand().(*DeleteData)
	fmt.Printf("updateCmd.tblName:%v\n", updateCmd.tblName)
	fmt.Printf("updateCmd.pred:%v\n", updateCmd.pred)
}

func TestUpdateSQL(t *testing.T) {
	sqlParser := NewSQLParser("update student set age = 21 where age = 20;")
	updateCmd := sqlParser.ParseCommand().(*ModifyData)
	fmt.Printf("updateCmd.tblName:%v\n", updateCmd.tblName)
	fmt.Printf("updateCmd.fldName:%v\n", updateCmd.fldName)
	fmt.Printf("updateCmd.newVal:%v\n", updateCmd.newVal)
	fmt.Printf("updateCmd.pred:%v\n", updateCmd.pred)
}
