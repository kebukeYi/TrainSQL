package plan

import (
	"practiceSQL/sql/parser"
	"practiceSQL/sql/server/executor"
	"practiceSQL/sql/types"
)

type Node interface {
	node()
}

type Plan struct {
	node Node
}

func (p *Plan) BuildNode(ast parser.Statement) {
	switch ast.(type) {
	case *parser.CreatTableData:
		columnVs := make([]types.ColumnV, 0)
		columns := ast.(*parser.CreatTableData).Columns
		for _, column := range columns {
			columnV := types.ColumnV{
				Name:     column.Name,
				DataType: column.DateType,
				Nullable: column.Nullable,
			}
			if column.DefaultValue != nil {
				columnV.DefaultValue = column.DefaultValue.V
			}
			columnVs = append(columnVs, columnV)
		}
		p.node = &CreateTable{
			Schema: &types.Table{
				Name:    ast.(*parser.CreatTableData).TableName,
				Columns: columnVs,
			},
		}
	case *parser.InsertData:
		p.node = &Insert{
			TableName: ast.(*parser.InsertData).TableName,
			Columns:   ast.(*parser.InsertData).Columns,
			Values:    ast.(*parser.InsertData).Values,
		}
	case *parser.SelectData:
		p.node = &Scan{
			TableName: ast.(*parser.SelectData).TableName,
		}
	default:
		panic("not support ast type")
	}
}

func (p *Plan) GetExecutor() executor.Executor {
	e := p.BuildExecutor()
	return e
}

func (p *Plan) BuildExecutor() executor.Executor {
	switch p.node.(type) {
	case *CreateTable:
		return executor.NewCreateTableExecutor(p.node.(*CreateTable).Schema)
	case *Insert:
		return executor.NewInsertTableExecutor(p.node.(*Insert).TableName,
			p.node.(*Insert).Columns, p.node.(*Insert).Values)
	case *Scan:
		return executor.NewScanTableExecutor(p.node.(*Scan).TableName)
	}
	return nil
}

type CreateTable struct {
	Schema *types.Table
}

func (c *CreateTable) node() {
}

type Insert struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *Insert) node() {
}

type Scan struct {
	TableName string
}

func (s *Scan) node() {
}
