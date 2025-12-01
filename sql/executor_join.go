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
	// 左边结果集;
	leftSet := n.Left.Execute(s)
	if left, ok := leftSet.(*types.ScanTableResult); ok {
		newRows := make([]types.Row, 0)
		var newCols []string
		// 结果集需要有参与表的所有列;
		newCols = append(newCols, leftSet.(*types.ScanTableResult).Columns...)
		// 右边结果集;
		rightSet := n.Right.Execute(s)
		if right, ok := rightSet.(*types.ScanTableResult); ok {
			// 左边列+右边列; 最后再统一进行取舍;
			newCols = append(newCols, rightSet.(*types.ScanTableResult).Columns...)

			// 左边多个行;
			for _, lrow := range left.Rows {
				matched := false
				for _, rrow := range right.Rows {
					// 判断当前两行是否满足 on 条件;
					if n.Predicate != nil {
						value := types.EvaluateExpr(n.Predicate, left.Columns, lrow, right.Columns, rrow)
						switch value.(type) {
						case *types.ConstNull:
						case *types.ConstBool:
							if value.(*types.ConstBool).Value == true {
								matched = true
								// 合并两行,组成新的一行,并添加到结果集;
								newRows = append(newRows, append(lrow, rrow...))
							}
						default:
							util.Error("NestedLoopJoinExecutor.EvaluateExpr Unexpected expression")
						}
					} else {
						// 没有 on 条件限制;
						newRows = append(newRows, append(lrow, rrow...))
					}
				} // for right rows over;

				// 当前左行和当前右行,没有匹配上&&但是属于左右连接, 那就需要将右行的列, 全都置为null, 进行左行补充;
				if !matched && n.Outer {
					// 右边行 的每一列都置为空;
					row := make(types.Row, 0)
					// 有多少列, 就置为多少null;
					for i := 0; i < len(right.Rows[0]); i++ {
						row = append(row, &types.ConstNull{})
					}
					newRows = append(newRows, row)
				}
			} // for join over
		}

		return &types.ScanTableResult{
			Columns: newCols,
			Rows:    newRows,
		}
	}
	util.Error("NestedLoopJoinExecutor.Execute Unexpected result set")
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
				// 解析 HashJoin 条件;
				hashJoinFilterVal := parseJoinFilter(h.Predicate)
				if hashJoinFilterVal == nil {
					util.Error("HashJoinExecutor: can not find join field")
				}
				lfield = hashJoinFilterVal.leftVal
				rfield = hashJoinFilterVal.rightVal
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

			// 构建哈希表
			table := make(map[types.Value][]types.Row)
			for _, row := range rrows {
				table[row[rpos]] = append(table[row[rpos]], row)
			}

			// 扫描左边获取记录;
			for _, lrow := range lrows {
				if rows, ok := table[lrow[lpos]]; ok {
					for _, row := range rows {
						lrow = append(lrow, row...)
						newRows = append(newRows, lrow)
					}
				} else {
					// 没有找到匹配的, 但是需要补全null列;
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
	util.Error("HashJoinExecutor.Execute Unexpected result set")
	return nil
}

type HashJoinFilterVal struct {
	leftVal  string
	rightVal string
}

func parseJoinFilter(expression *types.Expression) *HashJoinFilterVal {
	if expression != nil {
		hashJoinFilterVal := &HashJoinFilterVal{}
		if expression.Field != "" {
			hashJoinFilterVal.leftVal = expression.Field
			hashJoinFilterVal.rightVal = ""
			return hashJoinFilterVal
		} else if expression.OperationVal != nil {
			switch expression.OperationVal.(type) {
			case *types.OperationEqual:
				equal := expression.OperationVal.(*types.OperationEqual)
				hashJoinFilterVal.leftVal = parseJoinFilter(equal.Left).leftVal
				hashJoinFilterVal.rightVal = parseJoinFilter(equal.Right).rightVal
				return hashJoinFilterVal
			default:
				return nil
			}
		}
	}
	return nil
}
