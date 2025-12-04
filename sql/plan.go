package sql

import (
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type Plan struct {
	node    Node      // 嵌套
	Service Service   // 事务
	ast     Statement // 抽象语法树
}

func NewPlan(ast Statement, service Service) *Plan {
	plan := &Plan{Service: service}
	plan.ast = ast
	return plan
}
func (p *Plan) Execute() types.ResultSet {
	var err error
	p.node, err = p.BuildNode()
	if err != nil {
		return &types.ErrorResult{
			ErrorMessage: err.Error(),
		}
	}
	executor := p.BuildExecutor(p.node)
	resultSet := executor.Execute(p.Service)
	return resultSet
}
func (p *Plan) BuildNode() (Node, error) {
	var node Node
	var err error
	var ast Statement
	ast = p.ast
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
		node, err = p.BuildFromItem(selectData.From, selectData.WhereClause)
		if err != nil {
			return nil, err
		}
		hasAgg := false
		// aggregate、group by
		if selectData.SelectCols != nil || len(selectData.SelectCols) != 0 {
			for _, selectCol := range selectData.SelectCols {
				// 如果是 Function，说明是 agg
				if selectCol.Expr.Function != nil {
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
					Exprs:   selectData.SelectCols,
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
				return nil, util.Error("#BuildNode offset value must be int")
			}
			node = &OffsetNode{
				Source: node,
				Offset: int(constInt.Value),
			}
		}

		if selectData.Limit != nil {
			constInt, ok := selectData.Limit.ConstVal.(*types.ConstInt)
			if !ok {
				return nil, util.Error("#BuildNode limit value must be int")
			}
			node = &LimitNode{
				Source: node,
				Limit:  int(constInt.Value),
			}
		}

		if len(selectData.SelectCols) != 0 && !hasAgg {
			node = &ProjectNode{
				Source: node,
				Exprs:  selectData.SelectCols,
			}
		}
	case *UpdateData:
		buildScan, err := p.buildScan(ast.(*UpdateData).TableName, ast.(*UpdateData).WhereClause)
		if err != nil {
			return nil, err
		}
		node = &UpdateNode{
			TableName: ast.(*UpdateData).TableName,
			Source:    buildScan,
			columns:   ast.(*UpdateData).Columns,
		}
	case *DeleteData:
		buildScan, err := p.buildScan(ast.(*DeleteData).TableName, ast.(*DeleteData).WhereClause)
		if err != nil {
			return nil, err
		}
		node = &DeleteNode{
			TableName: ast.(*DeleteData).TableName,
			Source:    buildScan,
		}
	case *BeginData:
		return nil, util.Error("#BuildNode not support begin command")
	case *CommitData:
		return nil, util.Error("#BuildNode not support commit command")
	case *RollbackData:
		return nil, util.Error("#BuildNode not support rollback command")
	case *ExplainData:
		return nil, util.Error("#BuildNode not support explain command")
	default:
		return nil, util.Error("#BuildNode not support ast type")
	}
	return node, nil
}
func (p *Plan) BuildFromItem(item FromItem, filter *types.Expression) (Node, error) {
	switch item.(type) {
	case *TableItem:
		return p.buildScan(item.(*TableItem).TableName, filter)

	case *JoinItem:
		joinItem := item.(*JoinItem)
		// 如果是右连接, 则交换位置;
		if joinItem.JoinType == RightType {
			joinItem.Left, joinItem.Right = joinItem.Right, joinItem.Left
		}
		outer := true
		if joinItem.JoinType == CrossType || joinItem.JoinType == InnerType {
			outer = false
		}

		left, err := p.BuildFromItem(joinItem.Left, joinItem.Predicate)
		if err != nil {
			return nil, err
		}
		right, err := p.BuildFromItem(joinItem.Right, joinItem.Predicate)
		if err != nil {
			return nil, err
		}
		if joinItem.JoinType == CrossType {
			return &NestedLoopJoinNode{
				Left:      left,
				Right:     right,
				Predicate: joinItem.Predicate,
				Outer:     outer,
			}, nil
		} else {
			return &HashJoinNode{
				Left:      left,
				Right:     right,
				Predicate: joinItem.Predicate,
				Outer:     outer,
			}, nil
		}
	}
	return nil, nil
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
func (p *Plan) buildScan(tableName string, whereClause *types.Expression) (Node, error) {
	scanFilter := p.parseScanFilter(whereClause)
	if scanFilter != nil {
		table, err := p.Service.MustGetTable(tableName)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, err
		}
		for _, column := range table.Columns {
			if column.Name == scanFilter.field && column.PrimaryKey == true {
				return &PrimaryKeyScanNode{
					TableName: tableName,
					Value:     scanFilter.value,
				}, nil
			}
			if column.Name == scanFilter.field && column.IsIndex == true {
				return &IndexScanNode{
					TableName: tableName,
					Filed:     scanFilter.field,
					Value:     scanFilter.value,
				}, nil
			}
		}
		return &ScanNode{
			TableName: tableName,
			Filter:    whereClause,
		}, nil
	} else {
		return &ScanNode{
			TableName: tableName,
			Filter:    nil,
		}, nil
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
