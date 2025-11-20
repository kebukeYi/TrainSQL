package server

import (
	"fmt"
	"practiceSQL/sql/server/executor"
	"practiceSQL/sql/types"
	"practiceSQL/sql/util"
)

// 列对齐自动填充;
// tbl:
// insert into tbl values(1, 2, 3);
// a       b       c      d
// 1       2       3     无值   (default 填充)
func padRow(table *types.Table, row types.Row) types.Row {
	for id, column := range table.Columns {
		if id >= len(row) {
			if column.DefaultValue == nil {
				util.Error("[padRow] Column %s has no default value;\n", column.Name)
			} else {
				row = append(row, column.DefaultValue)
			}
		}
	}
	return row
}

// tbl:
// insert into tbl(d, c) values(1, 2);
//
//	a          b       c          d
//
// default   default   2          1
func makeRow(table *types.Table, columns []string, row types.Row) types.Row {
	if len(columns) != len(row) {
		util.Error("[makeRow] Columns and values count not match;\n")
	}
	input := make(map[string]types.Value)
	for i, column := range columns {
		input[column] = row[i]
	}
	var newRow []types.Value
	for _, column := range table.Columns {
		if input[column.Name] == nil {
			if column.DefaultValue == nil {
				util.Error("[makeRow] Column %s has no default value;\n", column.Name)
			} else {
				input[column.Name] = column.DefaultValue
				newRow = append(row, input[column.Name])
			}
		} else {
			newRow = append(row, input[column.Name])
		}
	}
	return newRow
}

func (s *KVService) ExecuteInsertTable(e executor.Executor) types.ResultSet {
	fmt.Println("ExecuteInsertTable")
	count := 0
	insertTableExecutor := e.(*executor.InsertTableExecutor)
	mustGetTable := s.mustGetTable(insertTableExecutor.TableName)
	// 每一行
	for _, expressions := range insertTableExecutor.Values {
		var row []types.Value
		for _, expression := range expressions {
			row = append(row, expression.V)
		}

		if insertTableExecutor.Columns == nil {
			padRow(mustGetTable, row)
		} else {
			makeRow(mustGetTable, insertTableExecutor.Columns, row)
		}
		s.createRow(insertTableExecutor.TableName, row)
		count++
	}
	return &types.InsertTableResult{
		Count: count,
	}
}
