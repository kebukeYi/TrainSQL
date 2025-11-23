package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/types"
)

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

type Statement interface {
	Statement() types.ResultSet
}

type CreatTableData struct {
	TableName string
	Columns   []*types.Column
}

func (c *CreatTableData) Statement() types.ResultSet {
	fmt.Println("create table", c.TableName)
	for _, column := range c.Columns {
		fmt.Printf("column: %s, %d, %v ", column.Name, column.DateType, column.Nullable)
		if column.DefaultValue == nil {
			fmt.Println()
			continue
		}
		con := column.DefaultValue.ConstVal
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
	return nil
}

type DropTableData struct {
	TableName string
}

func (d *DropTableData) Statement() types.ResultSet {
	return nil
}

type InsertData struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *InsertData) Statement() types.ResultSet {
	fmt.Println(" insert into ", i.TableName)
	fmt.Println(i.Columns)
	// 每行
	for _, value := range i.Values {
		for _, expression := range value {
			//fmt.Printf("%s   ", i.Columns[id])
			con := expression.ConstVal
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
	return nil
}

type DeleteData struct {
	TableName   string
	WhereClause *types.Expression
}

func (d *DeleteData) Statement() types.ResultSet {
	return nil
}

type UpdateData struct {
	TableName   string
	Columns     map[string]*types.Expression
	WhereClause *types.Expression
}

func (u *UpdateData) Statement() types.ResultSet {
	return nil
}

type SelectData struct {
	SelectCol   map[*types.Expression]string
	From        FromItem
	WhereClause *types.Expression
	GroupBy     *types.Expression
	Having      *types.Expression
	OrderBy     map[string]OrderDirection
	Limit       *types.Expression
	Offset      *types.Expression
}

func (s *SelectData) Statement() types.ResultSet {
	fmt.Println("select from", s.From.(*TableItem).TableName)
	return nil
}

type CreateIndexData struct {
	TableName string
	IndexName string
	Columns   []*types.Column
}

func (c *CreateIndexData) Statement() types.ResultSet {
	return nil
}

type BeginData struct {
}

func (b *BeginData) Statement() types.ResultSet {
	return nil
}

type CommitData struct {
}

func (c *CommitData) Statement() types.ResultSet {
	return nil
}

type RollbackData struct {
}

func (r *RollbackData) Statement() types.ResultSet {
	return nil
}

type ExplainData struct {
	Statements Statement
}

func (e *ExplainData) Statement() types.ResultSet {
	return nil
}
