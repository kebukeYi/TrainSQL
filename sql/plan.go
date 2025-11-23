package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type Node interface {
	node()
}

type UpdateNode struct {
	TableName string
	Source    Node
	columns   map[string]*types.Expression
}

func (u *UpdateNode) node() {

}

type CreateTableNode struct {
	Schema *types.Table
}

func (c *CreateTableNode) node() {
}

type DropTableNode struct {
	TableName string
}

func (d *DropTableNode) node() {
}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *InsertNode) node() {
}

type ScanNode struct {
	TableName string
	Filter    *types.Expression
}

func (s *ScanNode) node() {
}

type DeleteNode struct {
	TableName string
	Source    Node
}

func (d *DeleteNode) node() {
}

type OrderDirection int

var (
	OrderAsc  OrderDirection = 1
	OrderDesc OrderDirection = 2
)

type OrderNode struct {
	Source  Node
	OrderBy map[string]OrderDirection
}

func (o *OrderNode) node() {
}

type LimitNode struct {
	Source Node
	Limit  int
}

func (l *LimitNode) node() {}

type OffsetNode struct {
	Source Node
	Offset int
}

func (o *OffsetNode) node() {}

type ProjectNode struct {
	Source Node
	Exprs  map[*types.Expression]string
}

func (p *ProjectNode) node() {
}

type NestedLoopJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (n *NestedLoopJoinNode) node() {
}

type HashJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (h *HashJoinNode) node() {
}

type AggregateNode struct {
	Source  Node
	Exprs   map[*types.Expression]string
	GroupBy *types.Expression
}

func (a *AggregateNode) node() {
}

type FilterNode struct {
	Source    Node
	Predicate *types.Expression
}

func (f *FilterNode) node() {
}

type IndexScanNode struct {
	TableName string
	Filed     string
	Value     types.Value
}

func (i *IndexScanNode) node() {
}

type PrimaryKeyScanNode struct {
	TableName string
	Value     types.Value
}

func (p *PrimaryKeyScanNode) node() {
}

type Plan struct {
	node    Node
	Service Service
	ast     Statement
}

func NewPlan(ast Statement, service Service) *Plan {
	plan := &Plan{Service: service}
	plan.ast = ast
	return plan
}

func (p *Plan) Execute() types.ResultSet {
	p.node = p.BuildNode(p.ast)
	executor := p.BuildExecutor(p.node)
	resultSet := executor.Execute(p.Service)
	return resultSet
}
func (p *Plan) BuildNode(ast Statement) Node {
	var node Node
	switch ast.(type) {
	case *CreatTableData:
		columnVs := make([]types.ColumnV, 0)
		columns := ast.(*CreatTableData).Columns
		for _, column := range columns {
			columnV := types.ColumnV{
				Name:     column.Name,
				DataType: column.DateType,
			}
			columnV.Nullable = !column.PrimaryKey
			if column.DefaultValue != nil {
				columnV.DefaultValue = column.DefaultValue.ConstVal
			}
			columnV.PrimaryKey = column.PrimaryKey
			columnV.IsIndex = column.IsIndex
			columnVs = append(columnVs, columnV)
		}
		node = &CreateTableNode{
			Schema: &types.Table{
				Name:    ast.(*CreatTableData).TableName,
				Columns: columnVs,
			},
		}
	case *DropTableData:
		node = &DropTableNode{
			TableName: ast.(*DropTableData).TableName,
		}
	case *InsertData:
		node = &InsertNode{
			TableName: ast.(*InsertData).TableName,
			Columns:   ast.(*InsertData).Columns,
			Values:    ast.(*InsertData).Values,
		}
	case *SelectData:
		selectData := ast.(*SelectData)
		node = p.BuildFromItem(selectData.From, selectData.WhereClause)
		hasAgg := false
		// aggregate、group by
		if selectData.SelectCol != nil || len(selectData.SelectCol) != 0 {
			for expr, _ := range selectData.SelectCol {
				// 如果是 Function，说明是 agg
				if expr.Function != nil {
					hasAgg = true
					break
				}
			}
			if selectData.GroupBy != nil {
				hasAgg = true
			}
			if hasAgg {
				node = &AggregateNode{
					Source:  node,
					Exprs:   selectData.SelectCol,
					GroupBy: selectData.GroupBy,
				}
			}
		}
		if selectData.Having != nil {
			node = &FilterNode{
				Source:    node,
				Predicate: selectData.Having,
			}
		}
		if selectData.OrderBy != nil || len(selectData.OrderBy) > 0 {
			node = &OrderNode{
				Source:  node,
				OrderBy: selectData.OrderBy,
			}
		}
		if selectData.Offset != nil {
			constInt, ok := selectData.Offset.ConstVal.(*types.ConstInt)
			if !ok {
				util.Error("offset value must be int")
			}
			node = &OffsetNode{
				Source: node,
				Offset: int(constInt.Value),
			}
		}
		if selectData.Limit != nil {
			constInt, ok := selectData.Limit.ConstVal.(*types.ConstInt)
			if !ok {
				util.Error("limit value must be int")
			}
			node = &LimitNode{
				Source: node,
				Limit:  int(constInt.Value),
			}
		}
		if len(selectData.SelectCol) != 0 && !hasAgg {
			node = &ProjectNode{
				Source: node,
				Exprs:  selectData.SelectCol,
			}
		}
	case *UpdateData:
		node = &UpdateNode{
			TableName: ast.(*UpdateData).TableName,
			Source:    p.buildScan(ast.(*UpdateData).TableName, ast.(*UpdateData).WhereClause),
			columns:   ast.(*UpdateData).Columns,
		}
	case *DeleteData:
		node = &DeleteNode{
			TableName: ast.(*DeleteData).TableName,
			Source:    p.buildScan(ast.(*DeleteData).TableName, ast.(*DeleteData).WhereClause),
		}
	case *BeginData:
		util.Error("not support begin command")
	case *CommitData:
		util.Error("not support commit command")
	case *RollbackData:
		util.Error("not support rollback command")
	case *ExplainData:
		util.Error("not support explain command")
	default:
		panic("not support ast type")
	}
	return node
}
func (p *Plan) BuildFromItem(item FromItem, filter *types.Expression) Node {
	switch item.(type) {
	case *TableItem:
		return p.buildScan(item.(*TableItem).TableName, filter)

	case *JoinItem:
		joinItem := item.(*JoinItem)
		if joinItem.JoinType == RightType {
			joinItem.Left, joinItem.Right = joinItem.Right, joinItem.Left
		}
		outer := true
		if joinItem.JoinType == CrossType ||
			joinItem.JoinType == InnerType {
			outer = false
		}

		if joinItem.JoinType == CrossType {
			return &NestedLoopJoinNode{
				Left:      p.BuildFromItem(joinItem.Left, joinItem.Predicate),
				Right:     p.BuildFromItem(joinItem.Right, joinItem.Predicate),
				Predicate: joinItem.Predicate,
				Outer:     outer,
			}
		} else {
			return &HashJoinNode{
				Left:      p.BuildFromItem(joinItem.Left, joinItem.Predicate),
				Right:     p.BuildFromItem(joinItem.Right, joinItem.Predicate),
				Predicate: joinItem.Predicate,
				Outer:     outer,
			}
		}
	}
	return nil
}
func (p *Plan) BuildExecutor(node Node) Executor {
	switch node.(type) {
	case *CreateTableNode:
		return NewCreateTableExecutor(node.(*CreateTableNode).Schema)
	case *DropTableNode:
		return NewDropTableExecutor(node.(*DropTableNode).TableName)
	case *InsertNode:
		return NewInsertTableExecutor(node.(*InsertNode).TableName,
			node.(*InsertNode).Columns, node.(*InsertNode).Values)
	case *ScanNode:
		return NewScanTableExecutor(node.(*ScanNode).TableName, node.(*ScanNode).Filter)
	case *UpdateNode:
		updateNode := node.(*UpdateNode)
		sourceExecutor := p.BuildExecutor(updateNode.Source)
		return NewUpdateTableExecutor(updateNode.TableName, sourceExecutor, updateNode.columns)
	case *DeleteNode:
		return NewDeleteTableExecutor(node.(*DeleteNode).TableName, p.BuildExecutor(node.(*DeleteNode).Source))
	case *OrderNode:
		orderNode := node.(*OrderNode)
		source := p.BuildExecutor(orderNode.Source)
		return NewOrderExecutor(source, node.(*OrderNode).OrderBy)
	case *LimitNode:
		limitNode := node.(*LimitNode)
		source := p.BuildExecutor(limitNode.Source)
		return NewLimitExecutor(source, node.(*LimitNode).Limit)
	case *OffsetNode:
		offsetNode := node.(*OffsetNode)
		source := p.BuildExecutor(offsetNode.Source)
		return NewOffsetExecutor(source, node.(*OffsetNode).Offset)
	case *ProjectNode:
		projectNode := node.(*ProjectNode)
		source := p.BuildExecutor(projectNode.Source)
		return NewProjectExecutor(source, projectNode.Exprs)
	case *NestedLoopJoinNode:
		return NewNestedLoopJoinExecutor(p.BuildExecutor(node.(*NestedLoopJoinNode).Left),
			p.BuildExecutor(node.(*NestedLoopJoinNode).Right), node.(*NestedLoopJoinNode).Predicate, node.(*NestedLoopJoinNode).Outer)
	case *AggregateNode:
		return NewAggregateExecutor(p.BuildExecutor(node.(*AggregateNode).Source), node.(*AggregateNode).Exprs, node.(*AggregateNode).GroupBy)
	case *FilterNode:
		return NewFilterExecutor(p.BuildExecutor(node.(*FilterNode).Source), node.(*FilterNode).Predicate)
	case *IndexScanNode:
		return NewIndexScanExecutor(node.(*IndexScanNode).TableName, node.(*IndexScanNode).Filed, node.(*IndexScanNode).Value)
	case *PrimaryKeyScanNode:
		return NewPrimaryKeyScanExecutor(node.(*PrimaryKeyScanNode).TableName, node.(*PrimaryKeyScanNode).Value)
	case *HashJoinNode:
		return NewHashJoinExecutor(p.BuildExecutor(node.(*HashJoinNode).Left),
			p.BuildExecutor(node.(*HashJoinNode).Right), node.(*HashJoinNode).Predicate, node.(*HashJoinNode).Outer)
	}
	return nil
}

func (p *Plan) buildScan(tableName string, whereClause *types.Expression) Node {
	scanFilter := p.parseScanFilter(whereClause)
	if scanFilter != nil {
		table := p.Service.MustGetTable(scanFilter.field)
		if table == nil {
			return nil
		}
		for _, column := range table.Columns {
			if column.Name == scanFilter.field && column.PrimaryKey == true {
				return &PrimaryKeyScanNode{
					TableName: tableName,
					Value:     scanFilter.value,
				}
			}
			if column.Name == scanFilter.field && column.IsIndex == true {
				return &IndexScanNode{
					TableName: tableName,
					Filed:     scanFilter.field,
					Value:     scanFilter.value,
				}
			}
		}
		return &ScanNode{
			TableName: tableName,
			Filter:    whereClause,
		}
	} else {
		return &ScanNode{
			TableName: tableName,
			Filter:    nil,
		}
	}
}

type FilterValue struct {
	field string
	value types.Value
}

func (p *Plan) parseScanFilter(filter *types.Expression) *FilterValue {
	if filter == nil {
		return nil
	}
	if filter.Field != "" {
		return &FilterValue{
			field: filter.Field,
			value: &types.ConstNull{},
		}
	} else if filter.ConstVal != nil {
		return &FilterValue{
			field: "",
			value: filter.ConstVal,
		}
	} else if filter.OperationVal != nil {
		switch filter.OperationVal.(type) {
		case *types.OperationEqual:
			equal := filter.OperationVal.(*types.OperationEqual)
			left := p.parseScanFilter(equal.Left)
			right := p.parseScanFilter(equal.Right)
			return &FilterValue{
				field: left.field,
				value: right.value,
			}
		}
		return nil
	} else {
		// 无值返回 nil
		return nil
	}
}
