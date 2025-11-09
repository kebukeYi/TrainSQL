package parser

import "github.com/kebukeYi/TrainSQL/query"

type ModifyData struct {
	tblName string
	fldName string
	newVal  *query.Expression // 表示修改的值; set age = 12;
	pred    *query.Predicate  // where age = 12;
}

func NewModifyData(tblName string, fldName string, newVal *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		tblName: tblName,
		fldName: fldName,
		newVal:  newVal,
		pred:    pred,
	}
}

func (m *ModifyData) TableName() string {
	return m.tblName
}

func (m *ModifyData) TargetField() string {
	return m.fldName
}

func (m *ModifyData) NewValue() *query.Expression {
	return m.newVal
}

func (m *ModifyData) Pred() *query.Predicate {
	return m.pred
}
