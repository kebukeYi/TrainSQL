package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type NestedLoopJoinExecutor struct {
	Left      Executor
	Right     Executor
	Predicate *types.Expression
	Outer     bool
}

func NewNestedLoopJoinExecutor(left Executor, right Executor, predicate *types.Expression, outer bool) *NestedLoopJoinExecutor {
	return &NestedLoopJoinExecutor{
		Left:      left,
		Right:     right,
		Predicate: predicate,
		Outer:     outer,
	}
}
func (n *NestedLoopJoinExecutor) Execute(s Service) types.ResultSet {
	leftSet := n.Left.Execute(s)
	if left, ok := leftSet.(*types.ScanTableResult); ok {
		newRows := make([]types.Row, 0)
		var newCols []string
		newCols = append(newCols, leftSet.(*types.ScanTableResult).Columns...)
		rightSet := n.Right.Execute(s)
		if right, ok := rightSet.(*types.ScanTableResult); ok {
			// 左边列+右边列; 最后再统一进行取舍;
			newCols = append(newCols, rightSet.(*types.ScanTableResult).Columns...)

			// 左边多个行;
			for _, lrow := range left.Rows {
				matched := false
				for _, rrow := range right.Rows {
					if n.Predicate != nil {
						value := types.EvaluateExpr(n.Predicate, left.Columns, lrow, right.Columns, rrow)
						switch value.(type) {
						case *types.ConstBool:
							if value.(*types.ConstBool).Value == true {
								matched = true
								newRows = append(newRows, append(lrow, rrow...))
							}
						}
					} else {
						// 没有 on 条件限制;
						newRows = append(newRows, append(lrow, rrow...))
					}
				}
				// 左行 和右边所有行, 都没有 匹配的;
				if !matched && n.Outer {
					// 右边行 的每一列都置为空;
					row := make(types.Row, 0)
					for i := 0; i < len(right.Rows[0]); i++ {
						row = append(row, &types.ConstNull{})
					}
					newRows = append(newRows, row)
				}
			}
		}
		return &types.ScanTableResult{
			Columns: newCols,
			Rows:    newRows,
		}
	}
	return nil
}

type HashJoinExecutor struct {
	Left      Executor
	Right     Executor
	Predicate *types.Expression
	Outer     bool
}

func NewHashJoinExecutor(left Executor, right Executor, predicate *types.Expression, outer bool) *HashJoinExecutor {
	return &HashJoinExecutor{
		Left:      left,
		Right:     right,
		Predicate: predicate,
		Outer:     outer,
	}
}
func (h *HashJoinExecutor) Execute(s Service) types.ResultSet {
	leftSet := h.Left.Execute(s)
	if left, ok := leftSet.(*types.ScanTableResult); ok {
		lcols := left.Columns
		lrows := left.Rows
		newRows := make([]types.Row, 0)
		var newCols []string
		newCols = append(newCols, leftSet.(*types.ScanTableResult).Columns...)
		rightSet := h.Right.Execute(s)
		if right, ok := rightSet.(*types.ScanTableResult); ok {
			rrols := right.Columns
			rrows := right.Rows
			// 左边列+右边列; 最后再统一进行取舍;
			newCols = append(newCols, rightSet.(*types.ScanTableResult).Columns...)
			lfield := ""
			rfield := ""
			if h.Predicate != nil {
				// 解析 HashJoin 条件
				lfield, rfield = parseJoinFilter(h.Predicate)
				if lfield == "" || rfield == "" {
					util.Error("HashJoinExecutor: can not find join field")
				}
			}
			lpos := -1
			// 获取 join 列在表中列的位置
			for i, rol := range lcols {
				if rol == lfield {
					lpos = i
				}
			}
			if lpos == -1 {
				util.Error("HashJoinExecutor: can not find join field in left")
			}
			rpos := -1
			for i, rol := range rrols {
				if rol == rfield {
					rpos = i
				}
			}
			if rpos == -1 {
				util.Error("HashJoinExecutor: can not find join field in right")
			}
			table := make(map[types.Value][]types.Row)
			for _, row := range rrows {
				table[row[rpos]] = append(table[row[rpos]], row)
			}

			// 扫描左边获取记录
			for _, lrow := range lrows {
				if rows, ok := table[lrow[lpos]]; ok {
					for _, row := range rows {
						lrow = append(lrow, row...)
						newRows = append(newRows, lrow)
					}
				} else {
					if h.Outer {
						row := make(types.Row, 0)
						for i := 0; i < len(right.Rows[0]); i++ {
							row = append(row, &types.ConstNull{})
						}
						lrow = append(lrow, row...)
						newRows = append(newRows, lrow)
					}
				}
			}

			return &types.ScanTableResult{
				Columns: newCols,
				Rows:    newRows,
			}
		}
	}
	return nil
}

func parseJoinFilter(expression *types.Expression) (string, string) {
	if expression != nil {
		if expression.Field != "" {
			return expression.Field, ""
		} else if expression.OperationVal != nil {
			switch expression.OperationVal.(type) {
			case *types.OperationEqual:
				equal := expression.OperationVal.(*types.OperationEqual)
				lv, _ := parseJoinFilter(equal.Left)
				rv, _ := parseJoinFilter(equal.Right)
				return lv, rv
			default:
				return "", ""
			}
		}
	}
	return "", ""
}
