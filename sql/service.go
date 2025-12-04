package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type Service interface {
	Commit()
	Rollback()
	Version() uint64
	CreateRow(tableName string, row types.Row) error
	UpdateRow(table *types.Table, value types.Value, row []types.Value) error
	DeleteRow(table *types.Table, value types.Value) error
	ScanTable(tableName string, filter *types.Expression) ([]types.Row, error)
	LoadIndex(name string, filed string, value types.Value) ([]types.Value, error)
	SaveIndex(tableName string, colName string, value types.Value, indexSet []types.Value) error
	ReadById(name string, index types.Value) (types.Row, error)
	CreateTable(table *types.Table) error
	DropTable(tableName string) error
	GetTable(tableName string) (*types.Table, error)
	MustGetTable(tableName string) (*types.Table, error)
	GetTableNames() []string
}
