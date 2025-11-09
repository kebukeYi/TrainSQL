package parser

import (
	"github.com/kebukeYi/TrainSQL/query"
)

type InsertData struct {
	tblName string
	flds    []string          // 每列的字段名;
	vals    []*query.Constant // 每列具体的值;
}

func NewInsertData(tblName string, flds []string, vals []*query.Constant) *InsertData {
	return &InsertData{
		tblName: tblName,
		flds:    flds,
		vals:    vals,
	}
}

func (i *InsertData) TableName() string {
	return i.tblName
}

func (i *InsertData) Fields() []string {
	return i.flds
}

func (i *InsertData) Vals() []*query.Constant {
	return i.vals
}
