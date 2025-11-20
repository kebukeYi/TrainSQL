package server

import (
	"fmt"
	"practiceSQL/sql/server/executor"
	"practiceSQL/sql/types"
)

func (s *KVService) ExecuteCreateTable(e executor.Executor) types.ResultSet {
	fmt.Println("ExecuteCreateTable")
	creatTableExecutor := e.(*executor.CreatTableExecutor)
	tableName := creatTableExecutor.Schema.Name
	s.createTable(creatTableExecutor.Schema)
	return &types.CreateTableResult{TableName: tableName}
}
