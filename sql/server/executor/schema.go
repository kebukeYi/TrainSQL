package executor

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
func (c *CreatTableExecutor) Name() {
}
