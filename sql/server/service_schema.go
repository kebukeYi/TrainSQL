package server

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/server/executor"
	"github.com/kebukeYi/TrainSQL/sql/types"
)

func (s *KVService) ExecuteCreateTable(e executor.Executor) types.ResultSet {
	fmt.Println("ExecuteCreateTable")
	creatTableExecutor := e.(*executor.CreatTableExecutor)
	tableName := creatTableExecutor.Schema.Name
	s.createTable(creatTableExecutor.Schema)
	return &types.CreateTableResult{TableName: tableName}
}
