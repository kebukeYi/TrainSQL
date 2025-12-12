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
	SeqExprs []*SelectCol // 保证遍历时按照插入顺序输出;
	GroupBy  *types.Expression
}

func NewAggregateExecutor(source Executor, exprs []*SelectCol, groupBy *types.Expression) *AggregateExecutor {
	return &AggregateExecutor{
		Source:   source,
		SeqExprs: exprs,
		GroupBy:  groupBy,
	}
}

func (agg *AggregateExecutor) Execute(s Service) types.ResultSet {
	// 批处理模型, 非火山模型;
	resultSet := agg.Source.Execute(s)
	switch resultSet.(type) {
	case *types.ScanTableResult:
		result := resultSet.(*types.ScanTableResult)
		newColNames := make([]string, 0)
		newRows := make([]types.Row, 0)

		// 对每个分组, 进行计算 聚集 函数;
		calc := func(colVal types.Value, rows []types.Row) ([]types.Value, error) {
			newRow := make([]types.Value, 0)
			// 计算 表达式;
			for _, selectCol := range agg.SeqExprs {
				expression := selectCol.Expr
				alias := selectCol.Alis
				// 如果是 函数类型的;
				if expression.Function != nil {
					cal, err := BuildCal(expression.Function.FuncName)
					if err != nil {
						return nil, util.Error("AggregateExecutor: not support function name : %s \n", expression.Function.FuncName)
					}
					// 当前列名字 + 所有列 => 对应列下标 + 所有的行 + 当前函数 => 对应的列结果;
					val, err := cal.Calc(expression.Function.ColName, result.Columns, rows)
					if err != nil {
						return nil, err
					}
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
							return nil, util.Error("AggregateExecutor: not support group by column")
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
					return nil, util.Error("AggregateExecutor: not support expression type")
				}
			}
			return newRow, nil
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
				return &types.ErrorResult{ErrorMessage: util.Error("AggregateExecutor: can not find group by column").Error()}
			}

			// 针对 Group By 的列进行分组;
			// aggMap := make(map[types.Value][]types.Row)
			aggMap := make(map[uint32][]types.Row)
			for _, row := range result.Rows {
				// 获取当前行中「GROUP BY 列」的值, 作为分组的 “键”;
				key := row[pos]
				hashKey := key.Hash() // 对每行的 列值进行hash计算, 作为相同值的key来分组;
				// 当前列肯定有多个 相同的值, 直接追加即可;
				// aggMap[key] = append(aggMap[key], row)
				aggMap[hashKey] = append(aggMap[hashKey], row)
			}

			// 存在 group by 关键字,对每一组进行聚合函数计算:
			// 1. 列的值并没有重复, 原来是多少行就还是多少行;
			// 2. 列的值出现了重复, 重复的行需进行重叠分组;
			// aggMap的长度就等于 要返回的行数量;
			for _, rows := range aggMap {
				// 传入的每个分组的多行 row[];假如没有分组, 那么每次就传入一条row;
				value := rows[0][pos]
				row, err := calc(value, rows)
				if err != nil {
					return &types.ErrorResult{ErrorMessage: err.Error()}
				}
				newRows = append(newRows, row)
			}
		} else {
			// 没有存在分组关键字, 那么可以认为对全部行数据进行分组计算;
			row, err := calc(nil, result.Rows)
			if err != nil {
				return &types.ErrorResult{ErrorMessage: err.Error()}
			}
			newRows = append(newRows, row)
		}
		return &types.ScanTableResult{
			Columns: newColNames,
			Rows:    newRows,
		}
	default:
		return &types.ErrorResult{ErrorMessage: util.Error("AggregateExecutor: not support resultSet type").Error()}
	}
}

type Calculator interface {
	Calc(colName string, cols []string, rows []types.Row) (types.Value, error)
}

func BuildCal(funcName string) (Calculator, error) {
	funcName = strings.ToUpper(funcName)
	if funcName == "COUNT" {
		return &CountCal{}, nil
	} else if funcName == "SUM" {
		return &SumCal{}, nil
	} else if funcName == "AVG" {
		return &AvgCal{}, nil
	} else if funcName == "MAX" {
		return &MaxCal{}, nil
	} else if funcName == "MIN" {
		return &MinCal{}, nil
	} else {
		return nil, util.Error("AggregateExecutor.BuildCal: not support function name : %s \n", funcName)
	}
}

type CountCal struct {
}

func (c *CountCal) Calc(colName string, cols []string, rows []types.Row) (types.Value, error) {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, util.Error("AggregateExecutor.CountCal: can not find column")
	}
	// a b      c
	// 1 X     NULL
	// 2 NULL  6.4
	// 3 Z     1.5
	count := 0
	nullVal := &types.ConstNull{}
	for _, row := range rows {
		// 只要当前行的列值, 不为null, 那么就统计其有效;
		if bytes.Compare(row[pos].Bytes(), nullVal.Bytes()) != 0 {
			count++
		}
	}
	return &types.ConstInt{Value: int64(count)}, nil
}

type SumCal struct {
}

func (s *SumCal) Calc(colName string, cols []string, rows []types.Row) (types.Value, error) {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, util.Error("AggregateExecutor.CountCal: can not find column")
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
			return nil, util.Error("AggregateExecutor.SumCal: not support value type")
		}
	}
	if sum == 0.0 {
		return &types.ConstNull{}, nil
	} else {
		return &types.ConstFloat{Value: sum}, nil
	}
}

type AvgCal struct {
}

func (a *AvgCal) Calc(colName string, cols []string, rows []types.Row) (types.Value, error) {
	sumCal := SumCal{}
	sum, err := sumCal.Calc(colName, cols, rows)
	if err != nil {
		return nil, err
	}
	countCal := CountCal{}
	count, err := countCal.Calc(colName, cols, rows)
	if err != nil {
		return nil, err
	}
	if s, ok := sum.(*types.ConstFloat); ok {
		if c, ok := count.(*types.ConstInt); ok {
			return &types.ConstFloat{Value: s.Value / float64(c.Value)}, nil
		}
	}
	return &types.ConstNull{}, nil
}

type MaxCal struct {
}

func (m *MaxCal) Calc(colName string, cols []string, rows []types.Row) (types.Value, error) {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, util.Error("AggregateExecutor.CountCal: can not find column")
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
		return values[len(values)-1], nil
	}
	return nullVal, nil
}

type MinCal struct {
}

func (m *MinCal) Calc(colName string, cols []string, rows []types.Row) (types.Value, error) {
	pos := -1
	for i, col := range cols {
		if col == colName {
			pos = i
			break
		}
	}
	if pos == -1 {
		return nil, util.Error("AggregateExecutor.CountCal: can not find column")
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
		return values[0], nil
	}
	return nullVal, nil
}
