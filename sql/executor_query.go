package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"sort"
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
func (scan *ScanTableExecutor) Execute(s Service) types.ResultSet {
	fmt.Println("ExecuteScan")
	table := s.MustGetTable(scan.TableName)
	columns := table.Columns
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	rows := s.ScanTable(scan.TableName, scan.Filter)
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
func (scan *IndexScanTableExecutor) Execute(s Service) types.ResultSet {
	table := s.MustGetTable(scan.TableName)
	columnNames := make([]string, 0)
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}
	loadIndex := s.LoadIndex(table.Name, scan.Filed, scan.Value)
	sort.Slice(loadIndex, func(i, j int) bool {
		// todo Value比较函数, 有待优化;
		if loadIndex[i].Compare(loadIndex[j]) == -1 {
			return true
		}
		return false
	})
	rows := make([]types.Row, len(loadIndex))
	for _, index := range loadIndex {
		if row := s.ReadById(scan.TableName, index); row != nil {
			rows = append(rows, row)
		}
	}
	return &types.ScanTableResult{
		Columns: columnNames,
		Rows:    rows,
	}
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
func (scan *PrimaryKeyScanExecutor) Execute(s Service) types.ResultSet {
	// 扫描过程: 针对 主键id 进行扫描过滤;
	table := s.MustGetTable(scan.TableName)
	columnNames := make([]string, 0)
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}
	value := scan.Value
	if v, ok := value.(*types.ConstFloat); ok {
		value = &types.ConstInt{
			Value: int64(v.Value),
		}
	}
	rows := make([]types.Row, 0)
	if row := s.ReadById(scan.TableName, value); row != nil {
		rows = append(rows, row)
	}
	return &types.ScanTableResult{
		Columns: columnNames,
		Rows:    rows,
	}
}

// FilterExecutor 扫描过程: 针对 where 表达式进行过滤;
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
func (filter *FilterExecutor) Execute(s Service) types.ResultSet {
	resultSet := filter.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		newRow := make([]types.Row, 0)
		for _, row := range set.Rows {
			expr := types.EvaluateExpr(filter.Predicate, set.Columns, row, set.Columns, row)
			switch expr.(type) {
			case *types.ConstNull:
			case *types.ConstBool:
				if expr.(*types.ConstBool).Value == true {
					newRow = append(newRow, row)
				}
			default:
				util.Error("FilterExecutor.Execute Unexpected expression")
			}
		}
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    newRow,
		}
	}
	util.Error("FilterExecutor.Execute error resultSet type")
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
func (project *ProjectExecutor) Execute(s Service) types.ResultSet {
	resultSet := project.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		selected := make([]int, 0)
		newColumnNanes := make([]string, 0)
		for expression, alias := range project.Exprs {
			if expression.Field != "" {
				for index, column := range set.Columns {
					if column == expression.Field {
						selected = append(selected, index)
						if alias == "" {
							alias = column
						}
						newColumnNanes = append(newColumnNanes, alias)
					}
				}
			}
		}
		newRows := make([]types.Row, 0)
		for _, row := range set.Rows {
			newRowColumns := make([]types.Value, 0)
			for _, i2 := range selected {
				newRowColumns = append(newRowColumns, row[i2])
			}
			newRows = append(newRows, newRowColumns)
		}
		return &types.ScanTableResult{
			Columns: newColumnNanes,
			Rows:    newRows,
		}
	}
	util.Error("ProjectExecutor.Execute error resultSet type")
	return nil
}

type OrderExecutor struct {
	Source  Executor
	OrderBy map[string]OrderDirection
}

func NewOrderExecutor(source Executor, orderBy map[string]OrderDirection) *OrderExecutor {
	return &OrderExecutor{
		Source:  source,
		OrderBy: orderBy,
	}
}
func (order *OrderExecutor) Execute(s Service) types.ResultSet {
	resultSet := order.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		// 找到 order by 的列对应表中的列的位置;
		orderColIndex := make(map[string]int, 0)
		for colName, _ := range order.OrderBy {
			for index, column := range set.Columns {
				if column == colName {
					orderColIndex[column] = index
				}
			}
			if len(orderColIndex) == 0 {
				util.Error("OrderExecutor.Execute error column name")
			}
		}
		// 多个行(容器)参与比较;
		sort.Slice(set.Rows, func(i, j int) bool {
			// select a,b from user order by c,d desc e asc;
			// 迭代 order_by 参数, 可能存在多个 desc asc 列值;
			for colName, direction := range order.OrderBy {
				// 每一行的固定列值来参与 排序;
				iValue := set.Rows[i][orderColIndex[colName]]
				jValue := set.Rows[j][orderColIndex[colName]]
				compare := iValue.Compare(jValue)
				if compare == 0 {
				} else {
					if direction == OrderAsc {
						if compare == -1 {
							return true
						}
						return false
					} else {
						if compare == 1 {
							return true
						}
						return false
					}
				}
			}
			return true
		})
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    set.Rows,
		}
	}
	util.Error("OrderExecutor.Execute error resultSet type")
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
func (limit *LimitExecutor) Execute(s Service) types.ResultSet {
	// limit 10 offset 10;
	resultSet := limit.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    set.Rows[0:limit.Limit],
		}
	}
	util.Error("LimitExecutor.Execute error resultSet type")
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
func (offset *OffsetExecutor) Execute(s Service) types.ResultSet {
	// limit 10 offset 10;
	resultSet := offset.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    set.Rows[offset.Offset:],
		}
	}
	util.Error("OffsetExecutor.Execute error resultSet type")
	return nil
}
