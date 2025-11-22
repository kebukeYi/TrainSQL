package executor

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/plan"
	"github.com/kebukeYi/TrainSQL/sql/server"
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type ScanTableExecutor struct {
	TableName string
	Filter    *types.Expression
}

func NewScanTableExecutor(tableName string, filter *types.Expression) *ScanTableExecutor {
	return &ScanTableExecutor{
		TableName: tableName,
		Filter:    filter,
	}
}
func (scan *ScanTableExecutor) Execute(s server.Service) types.ResultSet {
	fmt.Println("ExecuteScan")
	table := s.MustGetTable(scan.TableName)
	columns := table.Columns
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	rows := s.ScanTable(scan.TableName)
	return &types.ScanTableResult{
		Columns: columnNames,
		Rows:    rows,
	}
}

type IndexScanTableExecutor struct {
	TableName string
	Filed     string
	Value     types.Value
}

func NewIndexScanExecutor(tableName string, filed string, value types.Value) *IndexScanTableExecutor {
	return &IndexScanTableExecutor{
		TableName: tableName,
		Filed:     filed,
		Value:     value,
	}
}
func (scan *IndexScanTableExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type PrimaryKeyScanExecutor struct {
	TableName string
	Value     types.Value
}

func NewPrimaryKeyScanExecutor(tableName string, value types.Value) *PrimaryKeyScanExecutor {
	return &PrimaryKeyScanExecutor{
		TableName: tableName,
		Value:     value,
	}
}
func (scan *PrimaryKeyScanExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type FilterExecutor struct {
	Source    Executor
	Predicate *types.Expression
}

func NewFilterExecutor(source Executor, predicate *types.Expression) *FilterExecutor {
	return &FilterExecutor{
		Source:    source,
		Predicate: predicate,
	}
}
func (filter *FilterExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type ProjectExecutor struct {
	Source Executor
	Exprs  map[*types.Expression]string
}

func NewProjectExecutor(source Executor, exprs map[*types.Expression]string) *ProjectExecutor {
	return &ProjectExecutor{
		Source: source,
		Exprs:  exprs,
	}
}
func (project *ProjectExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type OffsetExecutor struct {
	Source Executor
	Offset int
}

func NewOffsetExecutor(source Executor, offset int) *OffsetExecutor {
	return &OffsetExecutor{
		Source: source,
		Offset: offset,
	}
}
func (offset *OffsetExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type OrderExecutor struct {
	Source  Executor
	OrderBy map[string]plan.OrderDirection
}

func NewOrderExecutor(source Executor, orderBy map[string]plan.OrderDirection) *OrderExecutor {
	return &OrderExecutor{
		Source:  source,
		OrderBy: orderBy,
	}
}
func (order *OrderExecutor) Execute(server.Service) types.ResultSet {
	return nil
}

type LimitExecutor struct {
	Source Executor
	Limit  int
}

func NewLimitExecutor(source Executor, limit int) *LimitExecutor {
	return &LimitExecutor{
		Source: source,
		Limit:  limit,
	}
}
func (limit *LimitExecutor) Execute(server.Service) types.ResultSet {
	return nil
}
