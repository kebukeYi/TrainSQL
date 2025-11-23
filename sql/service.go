package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type Service interface {
	Commit()
	Rollback()
	Version() uint64
	CreateRow(tableName string, row types.Row)
	UpdateRow(table *types.Table, value types.Value, row []types.Value)
	DeleteRow(table *types.Table, value types.Value)
	ScanTable(tableName string, filter *types.Expression) []types.Row
	LoadIndex(name string, filed string, value types.Value) []types.Value
	SaveIndex(tableName string, colName string, value types.Value, indexSet []types.Value)
	ReadById(name string, index types.Value) types.Row
	CreateTable(table *types.Table)
	DropTable(tableName string)
	GetTableNames() []string
	GetTable(tableName string) *types.Table
	MustGetTable(tableName string) *types.Table
}
