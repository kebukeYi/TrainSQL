package executor

import (
	"practiceSQL/sql/types"
)

type InsertTableExecutor struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func NewInsertTableExecutor(tableName string, columns []string, values [][]*types.Expression) *InsertTableExecutor {
	return &InsertTableExecutor{
		TableName: tableName,
		Columns:   columns,
		Values:    values,
	}
}

func (i *InsertTableExecutor) Name() {
}
