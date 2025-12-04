package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/types"
)

type Statement interface {
	Statement() types.ResultSet
}

type ShowTableData struct {
	TableName string
}

func (s *ShowTableData) Statement() types.ResultSet {
	fmt.Println("show table", s.TableName)
	return nil
}

type ShowTDataBaseData struct {
}

func (s *ShowTDataBaseData) Statement() types.ResultSet {
	return nil
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
			fmt.Printf("default value: %d ", con.(*types.ConstInt).Into())
		case *types.ConstFloat:
			fmt.Printf("default value:%f ", con.(*types.ConstFloat).Into())
		case *types.ConstString:
			fmt.Printf("default value:%s ", con.(*types.ConstString).Into())
		case *types.ConstBool:
			fmt.Printf("default value: %v ", con.(*types.ConstBool).Into())
		case *types.ConstNull:
			fmt.Printf("default value: %s ", con.(*types.ConstNull).Into())
		}
		fmt.Println()
	}
	return nil
}

type DropTableData struct {
	TableName string
}

func (d *DropTableData) Statement() types.ResultSet {
	fmt.Println("drop table", d.TableName)
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
			con := expression.ConstVal
			switch con.(type) {
			case *types.ConstInt:
				fmt.Printf("%d   ", con.(*types.ConstInt).Into())
			case *types.ConstFloat:
				fmt.Printf("%f   ", con.(*types.ConstFloat).Into())
			case *types.ConstString:
				fmt.Printf("%s   ", con.(*types.ConstString).Into())
			case *types.ConstBool:
				fmt.Printf("%v   ", con.(*types.ConstBool).Into())
			case *types.ConstNull:
				fmt.Printf("%s   ", con.(*types.ConstNull).Into())
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
	fmt.Println("delete from", d.TableName)
	fmt.Println("where", d.WhereClause.ToString())
	return nil
}

type UpdateData struct {
	TableName   string
	Columns     map[string]*types.Expression
	WhereClause *types.Expression
}

func (u *UpdateData) Statement() types.ResultSet {
	fmt.Println("update", u.TableName)
	for k, v := range u.Columns {
		fmt.Println("update column:", k, "value:", v.ToString())
	}
	fmt.Println("where", u.WhereClause.ToString())
	return nil
}

type SelectCol struct {
	Expr *types.Expression
	Alis string
}

type SelectData struct {
	SelectCols  []*SelectCol
	From        FromItem
	WhereClause *types.Expression
	GroupBy     *types.Expression
	Having      *types.Expression
	OrderBy     []*OrderDirection
	Limit       *types.Expression
	Offset      *types.Expression
}

func (s *SelectData) Statement() types.ResultSet {
	fmt.Print("select")
	if len(s.SelectCols) > 0 {
		for _, col := range s.SelectCols {
			fmt.Print(" ")
			fmt.Print(col.Expr.ToString(), col.Alis)
			fmt.Print(", ")
		}
	} else {
		fmt.Print(" * ")
	}
	fmt.Print(" from ", s.From.(*TableItem).TableName)
	if s.WhereClause != nil {
		fmt.Print(" where ", s.WhereClause.ToString())
	}
	if s.GroupBy != nil {
		fmt.Print(" group by ", s.GroupBy.ToString())
	}
	if s.Having != nil {
		fmt.Print(" having ", s.Having.ToString())
	}
	if s.OrderBy != nil {
		fmt.Print(" order by ")
		for _, order := range s.OrderBy {
			fmt.Print(order.colName, order.direction)
		}
	}
	if s.Limit != nil {
		fmt.Print(" limit ", s.Limit.ToString())
	}
	if s.Offset != nil {
		fmt.Print(" offset ", s.Offset.ToString())
	}
	fmt.Println()
	return nil
}

type CreateIndexData struct {
	TableName string
	IndexName string
	Columns   []*types.Column
}

func (c *CreateIndexData) Statement() types.ResultSet {
	fmt.Println("create index", c.IndexName, "on", c.TableName)
	for _, column := range c.Columns {
		fmt.Println("column:", column.Name)
	}
	fmt.Println("index:", c.IndexName)
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
	return e.Statements.Statement()
}
