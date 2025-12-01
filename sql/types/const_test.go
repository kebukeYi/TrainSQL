package types

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func PartialCompare(a, b Value) (bool, int) {
	return a.PartialCmp(b)
}
func floatCompare(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

// OrderDirection 排序方向
type OrderDirection int

const (
	Asc OrderDirection = iota
	Desc
)

type OrderByClause struct {
	ColumnIndex int
	Direction   OrderDirection
}

func TestConstPartialCmp(t *testing.T) {
	rows := [][]Value{
		{NewConstInt(1), NewConstFloat(2), NewConstInt(3)},
		{NewConstInt(2), NewConstFloat(2), NewConstInt(4)},
		{NewConstInt(3), NewConstFloat(1), NewConstInt(5)},
		{NewConstInt(4), NewConstFloat(2), NewConstInt(5)},
		{NewConstInt(5), NewConstFloat(3), NewConstInt(5)},
		{NewConstInt(5), NewConstFloat(2.8), NewConstInt(5)},
	}
	OrderBy := []OrderByClause{
		{ColumnIndex: 2, Direction: Asc},
		{ColumnIndex: 1, Direction: Desc},
	}

	// 按第3列升序, 第2列降序, 排序;
	sort.Slice(rows, func(i, j int) bool {
		for _, byClause := range OrderBy {
			val1 := rows[i][byClause.ColumnIndex]
			val2 := rows[j][byClause.ColumnIndex]
			ok, cmp := val1.PartialCmp(val2)
			if !ok {
				// 不可比较，继续下一个排序条件
				continue
			}
			if cmp == 0 {
				// 相等，继续下一个排序条件
				continue
			}
			// 根据排序方向返回结果
			if byClause.Direction == Asc {
				// 如果 val1 < val2，返回 true
				return cmp < 0
			} else {
				// 如果 val1 > val2，返回 true
				return cmp > 0
			}
		}
		// 所有排序条件都相等
		// 返回 false 表示 i 不应该在 j 之前, 保持原顺序;
		return false
	})

	for _, row := range rows {
		fmt.Printf("id: %s, float: %s, int: %s\n", row[0].Bytes(), row[1].Bytes(), row[2].Bytes())
	}
}

func TestConstHash(t *testing.T) {
	assert.Equal(t, NewConstInt(1).Hash(), NewConstInt(1).Hash())
	assert.Equal(t, NewConstFloat(1.1).Hash(), NewConstFloat(1.1).Hash())
	assert.Equal(t, NewConstBool(true).Hash(), NewConstBool(true).Hash())
	assert.Equal(t, NewConstString("string").Hash(), NewConstString("string").Hash())
	assert.Equal(t, NewConstNull().Hash(), NewConstNull().Hash())
}
