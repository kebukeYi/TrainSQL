package plan

import (
	"github.com/kebukeYi/TrainSQL/sql/parser"
	"github.com/kebukeYi/TrainSQL/sql/server"
	"github.com/kebukeYi/TrainSQL/sql/server/executor"
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
	Asc  OrderDirection = 1
	Desc OrderDirection = 2
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
	// node Node
	Service server.Service
}

func (p *Plan) BuildNode(ast parser.Statement) Node {
	var node Node
	switch ast.(type) {
	case *parser.CreatTableData:
		columnVs := make([]types.ColumnV, 0)
		columns := ast.(*parser.CreatTableData).Columns
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
				Name:    ast.(*parser.CreatTableData).TableName,
				Columns: columnVs,
			},
		}
	case *parser.DropTableData:
		node = &DropTableNode{
			TableName: ast.(*parser.DropTableData).TableName,
		}
	case *parser.InsertData:
		node = &InsertNode{
			TableName: ast.(*parser.InsertData).TableName,
			Columns:   ast.(*parser.InsertData).Columns,
			Values:    ast.(*parser.InsertData).Values,
		}
	case *parser.SelectData:
		selectData := ast.(*parser.SelectData)
		selectNode := p.BuildFromItem(selectData.From, selectData.WhereClause)
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
					Source:  selectNode,
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
		if selectData.OrderBy != nil {
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
	case *parser.UpdateData:
		node = &UpdateNode{
			TableName: ast.(*parser.UpdateData).TableName,
			Source:    p.buildScan(ast.(*parser.UpdateData).TableName, ast.(*parser.UpdateData).WhereClause),
			columns:   ast.(*parser.UpdateData).Columns,
		}

	case *parser.DeleteData:
		node = &DeleteNode{
			TableName: ast.(*parser.DeleteData).TableName,
			Source:    p.buildScan(ast.(*parser.DeleteData).TableName, ast.(*parser.DeleteData).WhereClause),
		}
	case *parser.BeginData:
		util.Error("not support begin command")
	case *parser.CommitData:
		util.Error("not support commit command")
	case *parser.RollbackData:
		util.Error("not support rollback command")
	case *parser.ExplainData:
		util.Error("not support explain command")
	default:
		panic("not support ast type")
	}
	return node
}
func (p *Plan) BuildFromItem(item parser.FromItem, filter *types.Expression) Node {
	switch item.(type) {
	case *parser.TableItem:
		return p.buildScan(item.(*parser.TableItem).TableName, filter)

	case *parser.JoinItem:
		joinItem := item.(*parser.JoinItem)
		if joinItem.JoinType == parser.RightType {
			joinItem.Left, joinItem.Right = joinItem.Right, joinItem.Left
		}
		outer := true
		if joinItem.JoinType == parser.CrossType ||
			joinItem.JoinType == parser.InnerType {
			outer = false
		}

		if joinItem.JoinType == parser.CrossType {
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
func (p *Plan) BuildExecutor(node Node) executor.Executor {
	switch node.(type) {
	case *CreateTableNode:
		return executor.NewCreateTableExecutor(node.(*CreateTableNode).Schema)
	case *DropTableNode:
		return executor.NewDropTableExecutor(node.(*DropTableNode).TableName)
	case *InsertNode:
		return executor.NewInsertTableExecutor(node.(*InsertNode).TableName,
			node.(*InsertNode).Columns, node.(*InsertNode).Values)
	case *ScanNode:
		return executor.NewScanTableExecutor(node.(*ScanNode).TableName, node.(*ScanNode).Filter)
	case *UpdateNode:
		updateNode := node.(*UpdateNode)
		sourceExecutor := p.BuildExecutor(updateNode.Source)
		return executor.NewUpdateTableExecutor(updateNode.TableName, sourceExecutor, updateNode.columns)
	case *DeleteNode:
		return executor.NewDeleteTableExecutor(node.(*DeleteNode).TableName, p.BuildExecutor(node.(*DeleteNode).Source))
	case *OrderNode:
		orderNode := node.(*OrderNode)
		source := p.BuildExecutor(orderNode.Source)
		return executor.NewOrderExecutor(source, node.(*OrderNode).OrderBy)
	case *LimitNode:
		limitNode := node.(*LimitNode)
		source := p.BuildExecutor(limitNode.Source)
		return executor.NewLimitExecutor(source, node.(*LimitNode).Limit)
	case *OffsetNode:
		offsetNode := node.(*OffsetNode)
		source := p.BuildExecutor(offsetNode.Source)
		return executor.NewOffsetExecutor(source, node.(*OffsetNode).Offset)
	case *ProjectNode:
		projectNode := node.(*ProjectNode)
		source := p.BuildExecutor(projectNode.Source)
		return executor.NewProjectExecutor(source, projectNode.Exprs)
	case *NestedLoopJoinNode:
		return executor.NewNestedLoopJoinExecutor(p.BuildExecutor(node.(*NestedLoopJoinNode).Left),
			p.BuildExecutor(node.(*NestedLoopJoinNode).Right), node.(*NestedLoopJoinNode).Predicate, node.(*NestedLoopJoinNode).Outer)
	case *AggregateNode:
		return executor.NewAggregateExecutor(p.BuildExecutor(node.(*AggregateNode).Source), node.(*AggregateNode).Exprs, node.(*AggregateNode).GroupBy)
	case *FilterNode:
		return executor.NewFilterExecutor(p.BuildExecutor(node.(*FilterNode).Source), node.(*FilterNode).Predicate)
	case *IndexScanNode:
		return executor.NewIndexScanExecutor(node.(*IndexScanNode).TableName, node.(*IndexScanNode).Filed, node.(*IndexScanNode).Value)
	case *PrimaryKeyScanNode:
		return executor.NewPrimaryKeyScanExecutor(node.(*PrimaryKeyScanNode).TableName, node.(*PrimaryKeyScanNode).Value)
	case *HashJoinNode:
		return executor.NewHashJoinExecutor(p.BuildExecutor(node.(*HashJoinNode).Left),
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
		return nil
	}
}

type FilterValue struct {
	field string
	value types.Value
}

func (p *Plan) parseScanFilter(filter *types.Expression) *FilterValue {
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
