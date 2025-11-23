package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"sort"
)

type AggregateExecutor struct {
	Source  Executor
	Exprs   map[*types.Expression]string
	GroupBy *types.Expression
}

func NewAggregateExecutor(source Executor, exprs map[*types.Expression]string, groupBy *types.Expression) *AggregateExecutor {
	return &AggregateExecutor{
		Source:  source,
		Exprs:   exprs,
		GroupBy: groupBy,
	}
}
func (agg *AggregateExecutor) Execute(s Service) types.ResultSet {
	resultSet := agg.Source.Execute(s)
	switch resultSet.(type) {
	case *types.ScanTableResult:
		result := resultSet.(*types.ScanTableResult)
		newCols := make([]string, 0)
		newRows := make([]types.Row, 0)
		// 计算函数
		calc := func(colVal types.Value, rows []types.Row) []types.Value {
			newRow := make([]types.Value, 0)
			for expression, alias := range agg.Exprs {
				if expression.Function != nil {
					cal := BuildCal(expression.Function.FuncName)
					val := cal.Calc(expression.Function.ColName, result.Columns, rows)
					// min(a)            -> min
					// min(a) as min_val -> min_val
					if len(agg.Exprs) > len(newCols) {
						if alias == "" {
							newCols = append(newCols, expression.Function.FuncName)
						} else {
							newCols = append(newCols, alias)
						}
					}
					newRow = append(newRow, val)
				} else if expression.Field != "" {
					if agg.GroupBy != nil {
						if agg.GroupBy.Field != expression.Field {
							util.Error("AggregateExecutor: can not find group by column")
						}
					}
					if len(agg.Exprs) > len(newCols) {
						if alias == "" {
							newCols = append(newCols, expression.Field)
						} else {
							newCols = append(newCols, alias)
						}
					}
					newRow = append(newRow, colVal)
				} else {
					util.Error("AggregateExecutor: not support expression type")
				}
			}
			return newRow
		} // over cal

		// 判断有没有 Group By
		// select c2, min(c1), max(c3) from t group by c2;
		// c1 c2 c3
		// 1 aa 4.6
		// 3 cc 3.4
		// 2 bb 5.2
		// 4 cc 6.1
		// 5 aa 8.3
		// ----|------
		// ----|------
		// ----v------
		// 1 aa 4.6
		// 5 aa 8.3
		//
		// 2 bb 5.2
		//
		// 3 cc 3.4
		// 4 cc 6.1
		pos := -1
		if agg.GroupBy != nil && agg.GroupBy.Field != "" {
			// 对数据进行分组，然后计算每组的统计, 找到要分组的列索引index;
			for i, column := range result.Columns {
				if column == agg.GroupBy.Field {
					pos = i
					break
				}
			}
			if pos == -1 {
				util.Error("AggregateExecutor: can not find group by column")
			}
			// 针对 Group By 的列进行分组
			aggMap := make(map[types.Value][]types.Row)
			for _, row := range result.Rows {
				// 获取当前行中「GROUP BY 列」的值，作为分组的 “键”
				key := row[pos]
				aggMap[key] = append(aggMap[key], row)
			}

			for K, v := range aggMap {
				row := calc(K, v)
				newRows = append(newRows, row)
			}
		} else {
			row := calc(nil, result.Rows)
			newRows = append(newRows, row)
		}
		return &types.ScanTableResult{
			Columns: newCols,
			Rows:    newRows,
		}
	default:
		util.Error("AggregateExecutor: not support resultSet type")
	}
	util.Error("AggregateExecutor: not support resultSet type")
	return nil
}

type Calculator interface {
	Calc(colName string, cols []string, rows []types.Row) types.Value
}

func BuildCal(funcName string) Calculator {
	if funcName == "COUNT" {
		return &CountCal{}
	} else if funcName == "SUM" {
		return &SumCal{}
	} else if funcName == "AVG" {
		return &AvgCal{}
	} else if funcName == "MAX" {
		return &MaxCal{}
	} else if funcName == "MIN" {
		return &MinCal{}
	} else {
		util.Error("AggregateExecutor.BuildCal: not support function name")
	}
	return nil
}

type CountCal struct {
}

func (c *CountCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	count := 0
	nullVal := &types.ConstNull{}
	for _, row := range rows {
		if row[pos] != nullVal {
			count++
		}
	}
	return &types.ConstInt{Value: int64(count)}
}

type SumCal struct {
}

func (s *SumCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	sum := 0.0
	for _, row := range rows {
		value := row[pos]
		switch value.(type) {
		case *types.ConstNull:
		case *types.ConstInt:
			sum += float64(value.(*types.ConstInt).Value)
		case *types.ConstFloat:
			sum += value.(*types.ConstFloat).Value
		default:
			util.Error("AggregateExecutor.SumCal: not support value type")
		}
	}
	if sum == 0.0 {
		return &types.ConstNull{}
	} else {
		return &types.ConstFloat{Value: sum}
	}
}

type AvgCal struct {
}

func (a *AvgCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	sumCal := SumCal{}
	sum := sumCal.Calc(colName, cols, rows)
	countCal := CountCal{}
	count := countCal.Calc(colName, cols, rows)
	if s, ok := sum.(*types.ConstFloat); ok {
		if c, ok := count.(*types.ConstInt); ok {
			return &types.ConstFloat{Value: s.Value / float64(c.Value)}
		}
	}
	return &types.ConstNull{}
}

type MaxCal struct {
}

func (m *MaxCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	miaxVal := &types.ConstNull{}
	values := make([]types.Value, 0)
	for _, row := range rows {
		if row[pos] != miaxVal {
			values = append(values, row[pos])
		}
	}
	if len(values) != 0 {
		sort.Slice(values, func(i, j int) bool {
			return values[i].Compare(values[j]) == -1
		})
		return values[len(values)-1]
	}
	return miaxVal
}

type MinCal struct {
}

func (m *MinCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	minVal := &types.ConstNull{}
	values := make([]types.Value, 0)
	for _, row := range rows {
		if row[pos] != minVal {
			values = append(values, row[pos])
		}
	}
	if len(values) != 0 {
		sort.Slice(values, func(i, j int) bool {
			return values[i].Compare(values[j]) == -1
		})
		return values[0]
	}
	return minVal
}
