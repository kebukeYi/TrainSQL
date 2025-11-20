package server

import (
	"fmt"
	"practiceSQL/sql/server/executor"
	"practiceSQL/sql/types"
)

func (s *KVService) ExecuteScan(e executor.Executor) types.ResultSet {
	fmt.Println("ExecuteScan")
	scanTableExecutor := e.(*executor.ScanTableExecutor)
	table := s.mustGetTable(scanTableExecutor.TableName)
	columns := table.Columns
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	rows := s.scanTable(scanTableExecutor.TableName)
	return &types.SelectTableResult{
		Columns: columnNames,
		Rows:    rows,
	}
}
