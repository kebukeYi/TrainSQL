package parser

import (
	"github.com/kebukeYi/TrainSQL/record_manager"
)

type CreateTableData struct {
	tblName string                 // 表名;
	sch     *record_manager.Schema // 都有哪些字段,字段名字,字段类型,字段长度;
}

func NewCreateTableData(tblName string, sch *record_manager.Schema) *CreateTableData {
	return &CreateTableData{
		tblName: tblName,
		sch:     sch,
	}
}

func (c *CreateTableData) TableName() string {
	return c.tblName
}

func (c *CreateTableData) NewSchema() *record_manager.Schema {
	return c.sch
}
