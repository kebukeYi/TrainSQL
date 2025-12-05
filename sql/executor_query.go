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
	table, err := s.MustGetTable(scan.TableName)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#ScanTableExecutor.Execute error: %s", err.Error())}
	}
	columns := table.Columns
	var columnNames []string
	for _, column := range columns {
		columnNames = append(columnNames, column.Name)
	}
	rows, err := s.ScanTable(scan.TableName, scan.Filter)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#ScanTableExecutor.Execute error: %s", err.Error())}
	}
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
	table, err := s.MustGetTable(scan.TableName)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#IndexScanTableExecutor.Execute error: %s", err.Error())}
	}
	columnNames := make([]string, 0)
	for _, column := range table.Columns {
		columnNames = append(columnNames, column.Name)
	}
	loadIndex, err := s.LoadIndex(table.Name, scan.Filed, scan.Value)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#IndexScanTableExecutor.Execute error: %s", err.Error())}
	}
	sort.Slice(loadIndex, func(i, j int) bool {
		if ok, cmp := loadIndex[i].PartialCmp(loadIndex[j]); ok {
			// true:  表示索引 i 的元素应该排在索引 j 的元素之前
			// false: 表示索引 i 的元素应该排在索引 j 的元素之后
			// return numbers[i] < numbers[j]  // i < j 返回 true，表示 i 应该在前;
			return cmp == 1
		}
		return false
	})
	rows := make([]types.Row, 0)
	for _, index := range loadIndex {
		if row, err := s.ReadById(scan.TableName, index); row != nil {
			rows = append(rows, row)
		} else if err != nil {
			return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#IndexScanTableExecutor.Execute error: %s", err.Error())}
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
	table, err := s.MustGetTable(scan.TableName)
	if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#PrimaryKeyScanExecutor.Execute error: %s", err.Error())}
	}
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
	if row, err := s.ReadById(scan.TableName, value); row != nil {
		rows = append(rows, row)
	} else if err != nil {
		return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#PrimaryKeyScanExecutor.Execute error: %s", err.Error())}
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
			expr, err := types.EvaluateExpr(filter.Predicate, set.Columns, row, set.Columns, row)
			if err != nil {
				return &types.ErrorResult{ErrorMessage: fmt.Sprintf("#FilterExecutor.Execute error: %s", err.Error())}
			}
			switch expr.(type) {
			case *types.ConstNull:
			case *types.ConstBool:
				if expr.(*types.ConstBool).Value == true {
					newRow = append(newRow, row)
				}
			default:
				return &types.ErrorResult{ErrorMessage: "#FilterExecutor Execute Unexpected expression"}
			}
		}
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    newRow,
		}
	}
	return &types.ErrorResult{ErrorMessage: "#FilterExecutor.Execute error resultSet type"}
}

type ProjectExecutor struct {
	Source Executor
	//Exprs  map[*types.Expression]string
	Exprs []*SelectCol
}

func NewProjectExecutor(source Executor, exprs []*SelectCol) *ProjectExecutor {
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
		for _, selectCol := range project.Exprs {
			alias := selectCol.Alis
			expression := selectCol.Expr
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

type OrderDirection struct {
	colName   string
	direction OrderType
}

type OrderExecutor struct {
	Source Executor
	// 重大bug: map的遍历没有顺序性; 应该使用 切片数组, 保证遍历时按照插入顺序返回;
	// OrderBy map[string]OrderDirection
	OrderBy []*OrderDirection
}

func NewOrderExecutor(source Executor, orderBy []*OrderDirection) *OrderExecutor {
	return &OrderExecutor{
		Source:  source,
		OrderBy: orderBy,
	}
}
func (order *OrderExecutor) Execute(s Service) types.ResultSet {
	resultSet := order.Source.Execute(s)
	if set, ok := resultSet.(*types.ScanTableResult); ok {
		// 找到 order by 的列对应表中的列的位置;
		orderColIndex := make(map[string]int)
		for _, orderDirection := range order.OrderBy {
			for index, column := range set.Columns {
				if column == orderDirection.colName {
					orderColIndex[column] = index
				}
			}
			if len(orderColIndex) == 0 {
				return &types.ErrorResult{ErrorMessage: "#OrderExecutor.Execute error column name"}
			}
		}
		// 多个行(容器)参与比较;
		sort.Slice(set.Rows, func(i, j int) bool {
			// select a,b from user order by c,d desc e asc;
			// 迭代 order_by 参数, 可能存在多个 desc asc 列值;
			for _, orderDirection := range order.OrderBy {
				// 每一行的固定列值来参与 排序;
				iValue := set.Rows[i][orderColIndex[orderDirection.colName]]
				jValue := set.Rows[j][orderColIndex[orderDirection.colName]]
				allow, cmp := iValue.PartialCmp(jValue)
				if !allow {
					continue
				}
				if cmp == 0 {
					continue // 判断下一个比较条件
				}
				if orderDirection.direction == OrderAsc {
					return cmp < 0
				} else {
					return cmp > 0
				}
			}
			// 比较完毕, 默认返回 true, 不改动位置;
			return true
		})

		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    set.Rows,
		}
	}
	return &types.ErrorResult{ErrorMessage: "#OrderExecutor.Execute error resultSet type"}
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
		if limit.Limit > len(set.Rows) {
			limit.Limit = len(set.Rows)
		}
		rows := set.Rows[:limit.Limit]
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    rows,
		}
	}
	return &types.ErrorResult{
		ErrorMessage: "#LimitExecutor.Execute error resultSet type",
	}
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
		if offset.Offset > len(set.Rows) {
			offset.Offset = len(set.Rows) - 1
		}
		return &types.ScanTableResult{
			Columns: set.Columns,
			Rows:    set.Rows[offset.Offset:],
		}
	}
	return &types.ErrorResult{
		ErrorMessage: "#OffsetExecutor.Execute error resultSet type",
	}
}
