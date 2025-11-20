package parser

import (
	"fmt"
	"practiceSQL/sql/types"
)

type Statement interface {
	Execute()
}

type CreatTableData struct {
	TableName string
	Columns   []*types.Column
}

func (c *CreatTableData) Execute() {
	fmt.Println("create table", c.TableName)
	for _, column := range c.Columns {
		fmt.Printf("column: %s, %d, %v ", column.Name, column.DateType, column.Nullable)
		if column.DefaultValue == nil {
			fmt.Println()
			continue
		}
		con := column.DefaultValue.V
		switch con.(type) {
		case *types.ConstInt:
			fmt.Print("default value:", con.(*types.ConstInt).Value)
		case *types.ConstFloat:
			fmt.Print("default value:", con.(*types.ConstFloat).Value)
		case *types.ConstString:
			fmt.Print("default value:", con.(*types.ConstString).Value)
		case *types.ConstBool:
			fmt.Print("default value:", con.(*types.ConstBool).Value)
		case *types.ConstNull:
			fmt.Print("default value:", con.(*types.ConstNull).Value)
		}
		fmt.Println()
	}
}

type DropTableData struct {
	Table string
}

func (d *DropTableData) Execute() {
}

type InsertData struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *InsertData) Execute() {
	fmt.Println(" insert into ", i.TableName)
	fmt.Println(i.Columns)
	// 每行
	for _, value := range i.Values {
		for _, expression := range value {
			//fmt.Printf("%s   ", i.Columns[id])
			con := expression.V
			switch con.(type) {
			case *types.ConstInt:
				fmt.Printf("%d   ", con.(*types.ConstInt).Value)
			case *types.ConstFloat:
				fmt.Printf("%f   ", con.(*types.ConstFloat).Value)
			case *types.ConstString:
				fmt.Printf("%s   ", con.(*types.ConstString).Value)
			case *types.ConstBool:
				fmt.Printf("%v   ", con.(*types.ConstBool).Value)
			case *types.ConstNull:
				fmt.Printf("%v   ", con.(*types.ConstNull).Value)
			}
		}
		fmt.Println()
	}
}

type DeleteData struct {
	TableName string
}

func (d *DeleteData) Execute() {
}

type UpdateData struct {
	TableName string
}

func (u *UpdateData) Execute() {
}

type SelectData struct {
	TableName string
}

func (s *SelectData) Execute() {
	fmt.Println("select from", s.TableName)
}

type CreateIndexData struct {
	TableName string
	IndexName string
	Columns   []*types.Column
}

func (c *CreateIndexData) Execute() {
}

type DropIndexData struct {
	TableName string
	IndexName string
}

func (d *DropIndexData) Execute() {
}
