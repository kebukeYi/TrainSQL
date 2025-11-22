package executor

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/server"
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
func (c *CreatTableExecutor) Execute(s server.Service) types.ResultSet {
	fmt.Println("ExecuteCreateTable")
	tableName := c.Schema.Name
	s.CreateTable(c.Schema)
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
func (d *DropTableExecutor) Execute(s server.Service) types.ResultSet {
	fmt.Println("ExecuteDropTable")
	tableName := d.TableName
	s.DropTable(tableName)
	return &types.DropTableResult{TableName: tableName}
}
