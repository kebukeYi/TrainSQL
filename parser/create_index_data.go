package parser

import "fmt"

type IndexData struct {
	tblName string // user
	fldName string // studentID
	idxName string // studentID_index
}

func NewIndexData(idxName string, tblName string, fldName string) *IndexData {
	return &IndexData{
		idxName: idxName,
		tblName: tblName,
		fldName: fldName,
	}
}

func (i *IndexData) IndexName() string {
	return i.idxName
}

func (i *IndexData) TableName() string {
	return i.tblName
}

func (i *IndexData) FieldName() string {
	return i.fldName
}

func (i *IndexData) ToString() string {
	str := fmt.Sprintf("index name: %s, table name: %s, field name: %s", i.idxName, i.tblName, i.fldName)
	return str
}
