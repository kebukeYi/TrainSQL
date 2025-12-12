package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type CreatTableExecutor struct {
	Schema *types.Table
}

func NewCreateTableExecutor(schema *types.Table) *CreatTableExecutor {
	return &CreatTableExecutor{
		Schema: schema,
	}
}
func (c *CreatTableExecutor) Execute(s Service) types.ResultSet {
	tableName := c.Schema.Name
	err := s.CreateTable(c.Schema)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: err.Error()}
	}
	return &types.CreateTableResult{TableName: tableName}
}

type DropTableExecutor struct {
	TableName string
}

func NewDropTableExecutor(tableName string) *DropTableExecutor {
	return &DropTableExecutor{
		TableName: tableName,
	}
}
func (d *DropTableExecutor) Execute(s Service) types.ResultSet {
	tableName := d.TableName
	err := s.DropTable(tableName)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: err.Error()}
	}
	return &types.DropTableResult{TableName: tableName}
}
