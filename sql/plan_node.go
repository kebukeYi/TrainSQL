package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"strings"
)

type Node interface {
	FormatNode(f *strings.Builder, prefix string, root bool)
}

type UpdateNode struct {
	TableName string
	Source    Node
	columns   map[string]*types.Expression
}

func (u *UpdateNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Update on  %s", u.TableName))
	u.Source.FormatNode(f, prefix, false)
}

type CreateTableNode struct {
	Schema *types.Table
}

func (c *CreateTableNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Create Table %s", c.Schema.Name))
}

type DropTableNode struct {
	TableName string
}

func (d *DropTableNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Drop Table %s;", d.TableName))
}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *InsertNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Insert into  %s;", i.TableName))
}

type ScanNode struct {
	TableName string
	Filter    *types.Expression
}

func (s *ScanNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Seq Scan on  %s", s.TableName))
	if s.Filter != nil {
		f.WriteString(fmt.Sprintf(" (%s)", s.Filter.ToString()))
	}
}

type DeleteNode struct {
	TableName string
	Source    Node
}

func (d *DeleteNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Delete on  %s;", d.TableName))
	d.Source.FormatNode(f, prefix, false)
}

type OrderType int

var (
	OrderAsc  OrderType = 1
	OrderDesc OrderType = 2
)

type OrderNode struct {
	Source  Node
	OrderBy []*OrderDirection
}

func (o *OrderNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	descParts := make([]string, len(o.OrderBy))
	index := 0
	direction := "asc"
	for _, orderDirection := range o.OrderBy {
		if orderDirection.direction == OrderDesc {
			direction = "desc"
		} else {
			direction = "asc"
		}
		descParts[index] = fmt.Sprintf("%s %s", orderDirection.colName, direction)
		index++
	}
	f.WriteString(fmt.Sprintf("Order By (%s)", strings.Join(descParts, ",")))
	o.Source.FormatNode(f, prefix, false)
}

type LimitNode struct {
	Source Node
	Limit  int
}

func (l *LimitNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Limit %d", l.Limit))
	l.Source.FormatNode(f, prefix, false)
}

type OffsetNode struct {
	Source Node
	Offset int
}

func (o *OffsetNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Offset  %d", o.Offset))
	o.Source.FormatNode(f, prefix, false)
}

type ProjectNode struct {
	Source Node
	Exprs  []*SelectCol
}

func (p *ProjectNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	exprs := make([]string, len(p.Exprs))
	for i, expr := range p.Exprs {
		exprStr := expr.Expr.ToString()
		if expr.Alis != "" {
			exprStr += fmt.Sprintf(" as %s", expr.Alis)
		}
		exprs[i] = exprStr
	}
	f.WriteString(fmt.Sprintf("Projection (%s)", strings.Join(exprs, ", ")))
	p.Source.FormatNode(f, prefix, false)
}

type NestedLoopJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (n *NestedLoopJoinNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(" Nested Loop Join ")
	if n.Predicate != nil {
		f.WriteString(fmt.Sprintf("(%s)", n.Predicate.ToString()))
	}
	n.Left.FormatNode(f, prefix, false)
	n.Right.FormatNode(f, prefix, false)
}

type HashJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (h *HashJoinNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(" Hash Join ")
	if h.Predicate != nil {
		f.WriteString(fmt.Sprintf("(%s)", h.Predicate.ToString()))
	}
	h.Left.FormatNode(f, prefix, false)
	h.Right.FormatNode(f, prefix, false)
}

type AggregateNode struct {
	Source  Node
	Exprs   []*SelectCol
	GroupBy *types.Expression
}

func (a *AggregateNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	exprs := make([]string, len(a.Exprs))
	for i, expr := range a.Exprs {
		exprStr := expr.Expr.ToString()
		if expr.Alis != "" {
			exprStr += fmt.Sprintf(" as %s", expr.Alis)
		}
		exprs[i] = exprStr
	}
	f.WriteString(fmt.Sprintf("Aggregate (%s)", strings.Join(exprs, ", ")))
	a.Source.FormatNode(f, prefix, false)
}

type FilterNode struct {
	Source    Node
	Predicate *types.Expression
}

func (fi *FilterNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Filter (%s)", fi.Predicate.ToString()))
	fi.Source.FormatNode(f, prefix, false)
}

type IndexScanNode struct {
	TableName string
	Filed     string
	Value     types.Value
}

func (i *IndexScanNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Index Scan On %s %s", i.TableName, i.Filed))
}

type PrimaryKeyScanNode struct {
	TableName string
	Value     types.Value
}

func (p *PrimaryKeyScanNode) FormatNode(f *strings.Builder, prefix string, root bool) {
	if !root {
		f.WriteString("\n")
	} else {
		f.WriteString("           SQL PLAN           \n")
		f.WriteString("------------------------------\n")
	}
	if prefix == "" {
		prefix = "  ->  "
	} else {
		// 第一步：输出当前前缀
		f.WriteString(prefix)
		// 第二步：生成子节点的新前缀
		prefix = "   " + prefix
	}
	f.WriteString(fmt.Sprintf("Primary key Scan On %s %s", p.TableName, p.Value.Bytes()))
}

type FromItem interface {
	Item()
}

type TableItem struct {
	TableName string
}

func (t *TableItem) Item() {
}

type JoinType int

var (
	CrossType JoinType = 1
	InnerType JoinType = 2
	LeftType  JoinType = 3
	RightType JoinType = 4
)

type JoinItem struct {
	Left      FromItem
	Right     FromItem
	JoinType  JoinType
	Predicate *types.Expression
}

func (j *JoinItem) Item() {
}
