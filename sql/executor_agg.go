package sql

import (
	"bytes"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"sort"
	"strings"
)

type AggregateExecutor struct {
	Source   Executor
	SeqExprs []*SelectCol // 保证插入顺序列;
	// Exprs    map[*types.Expression]string // <表达式, 别名>
	GroupBy *types.Expression
}

func NewAggregateExecutor(source Executor, exprs []*SelectCol, groupBy *types.Expression) *AggregateExecutor {
	return &AggregateExecutor{
		Source:   source,
		SeqExprs: exprs,
		GroupBy:  groupBy,
	}
}
func (agg *AggregateExecutor) Execute(s Service) types.ResultSet {
	resultSet := agg.Source.Execute(s)
	switch resultSet.(type) {
	case *types.ScanTableResult:
		result := resultSet.(*types.ScanTableResult)
		newColNames := make([]string, 0)
		newRows := make([]types.Row, 0)

		// 对每个分组, 进行计算 聚集 函数;
		calc := func(colVal types.Value, rows []types.Row) []types.Value {
			newRow := make([]types.Value, 0)
			// 计算 表达式;
			//
			for _, selectCol := range agg.SeqExprs {
				expression := selectCol.Expr
				alias := selectCol.Alis
				// 如果是 函数类型的;
				if expression.Function != nil {
					cal := BuildCal(expression.Function.FuncName)
					// 当前列名字 + 所有列 => 对应列下标 + 所有的行 + 当前函数 => 对应的列结果;
					val := cal.Calc(expression.Function.ColName, result.Columns, rows)
					// min(a)            -> 默认列名为 min(a)
					// min(a) as min_val -> 默认列名为 min_val
					// 如果函数列 > 列数;
					if len(agg.SeqExprs) > len(newColNames) {
						if alias == "" {
							defaultColName := expression.Function.FuncName + "_" + expression.Function.ColName
							newColNames = append(newColNames, defaultColName)
						} else {
							newColNames = append(newColNames, alias)
						}
					}
					newRow = append(newRow, val)
				} else if expression.Field != "" { // select c2 列;
					// select c2, min(c1), max(c3) from t group by c2;
					if agg.GroupBy != nil {
						if agg.GroupBy.Field != expression.Field {
							util.Error("AggregateExecutor: can not find group by column")
						}
					}

					if len(agg.SeqExprs) > len(newColNames) {
						if alias == "" {
							newColNames = append(newColNames, expression.Field)
						} else {
							newColNames = append(newColNames, alias)
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

			// 针对 Group By 的列进行分组;
			// aggMap := make(map[types.Value][]types.Row)
			aggMap := make(map[uint32][]types.Row)
			for _, row := range result.Rows {
				// 获取当前行中「GROUP BY 列」的值, 作为分组的 “键”;
				key := row[pos]
				hashKey := key.Hash()
				// 当前列肯定有多个 相同的值, 直接追加即可;
				// aggMap[key] = append(aggMap[key], row)
				aggMap[hashKey] = append(aggMap[hashKey], row)
			}
			// 存在 group by 关键字,对,每一组进行聚合函数计算:
			// 1. 列的值并没有重复, 原来是多少行就还是多少行;
			// 2. 列的值出现了重复, 重复的行需进行重叠分组;
			// aggMap的长度就等于 要返回的行数量;
			for _, rows := range aggMap {
				// 传入的每个分组的 row[];
				// 假如没有分组, 那么每次就传入一条row;
				// row := calc(K, v)
				value := rows[0][pos]
				row := calc(value, rows)
				newRows = append(newRows, row)
			}
		} else {
			row := calc(nil, result.Rows)
			newRows = append(newRows, row)
		}
		return &types.ScanTableResult{
			Columns: newColNames,
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
	funcName = strings.ToUpper(funcName)
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
		util.Error("AggregateExecutor.BuildCal: not support function name : %s \n", funcName)
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
			break
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
		if bytes.Compare(row[pos].Bytes(), nullVal.Bytes()) != 0 {
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
			break
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
			break
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	nullVal := &types.ConstNull{}
	values := make([]types.Value, 0)
	for _, row := range rows {
		if bytes.Compare(row[pos].Bytes(), nullVal.Bytes()) != 0 {
			values = append(values, row[pos])
		}
	}
	if len(values) != 0 {
		sort.Slice(values, func(i, j int) bool {
			if ok, cmp := values[i].PartialCmp(values[j]); ok {
				return cmp == -1
			}
			return false
		})
		return values[len(values)-1]
	}
	return nullVal
}

type MinCal struct {
}

func (m *MinCal) Calc(colName string, cols []string, rows []types.Row) types.Value {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
			break
		}
	}
	if pos == -1 {
		util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	nullVal := &types.ConstNull{}
	values := make([]types.Value, 0)
	for _, row := range rows {
		if bytes.Compare(row[pos].Bytes(), nullVal.Bytes()) != 0 {
			values = append(values, row[pos])
		}
	}
	if len(values) != 0 {
		sort.Slice(values, func(i, j int) bool {
			if ok, cmp := values[i].PartialCmp(values[j]); ok {
				return cmp == -1
			}
			return false
		})
		return values[0]
	}
	return nullVal
}
