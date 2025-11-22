package executor

import (
	"github.com/kebukeYi/TrainSQL/sql/server"
	"github.com/kebukeYi/TrainSQL/sql/types"
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
func (n *NestedLoopJoinExecutor) Execute(server.Service) types.ResultSet {

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
func (h *HashJoinExecutor) Execute(server.Service) types.ResultSet {

	return nil
}

func parseJoinFilter() (string, string) {
	return "", ""
}
