package executor

import (
	"github.com/kebukeYi/TrainSQL/sql/server"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
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
func (agg *AggregateExecutor) Execute(s server.Service) types.ResultSet {
	resultSet := s.Execute(agg.Source)
	switch resultSet.(type) {
	case *types.ScanTableResult:
		result := resultSet.(*types.ScanTableResult)
		newCols := make([]string, 0)
		newRows := make([]types.Row, 0)
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
			aggMap := make(map[types.Value][]types.Row)
			//
			for _, row := range result.Rows {
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
	if funcName == "count" {
		return &CountCal{}
	} else if funcName == "sum" {
		return &SumCal{}
	} else if funcName == "avg" {
		return &AvgCal{}
	} else if funcName == "max" {
		return &MaxCal{}
	} else if funcName == "min" {
		return &MinCal{}
	} else {
		util.Error("AggregateExecutor.BuildCal: not support function name")
	}
	return nil
}

func BuildCalculator(name string) Calculator {
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
	count := 0
	for _, row := range rows {
		// todo 有待改变
		if row[pos] != nil {
			count++
		}
	}
	return &types.ConstInt{Value: int64(count)}
}

type SumCal struct {
}

func (s *SumCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	var sum float64
	return &types.ConstFloat{Value: sum}
}

type AvgCal struct {
}

func (a *AvgCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	var sum float64
	return &types.ConstFloat{Value: sum / float64(len(rows))}
}

type MaxCal struct {
}

func (m *MaxCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	var max types.Value
	return max
}

type MinCal struct {
}

func (m *MinCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	var min types.Value
	return min
}
