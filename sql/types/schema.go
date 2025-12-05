package types

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type Table struct {
	Name    string
	Columns []ColumnV
}

func (t *Table) Validate() error {
	if t.Columns == nil || len(t.Columns) == 0 {
		util.Error("[Table] %s columns is nil", t.Name)
	}
	// 校验是否有主键
	count := 0
	for _, column := range t.Columns {
		if column.PrimaryKey {
			count++
		}
		if column.PrimaryKey && column.Nullable {
			return util.Error("[Table] %s column %s can not be nullable", t.Name, column.Name)
		}
		if column.DefaultValue != nil {
			// 尽管列的定义是 int string bool, float, 但是仍允许列为 默认值为  NULL;
			if column.DefaultValue.DateType() == Null {
				continue
			}
			if column.DefaultValue.DateType() != column.DataType {
				return util.Error("[Table] %s column %s default value type %d not match %d", t.Name, column.Name, column.DefaultValue.DateType(), column.DataType)
			}
		}
	}
	if count != 1 {
		return util.Error("[Table] %s has no primary key", t.Name)
	}
	return nil
}

func (t *Table) GetPrimaryKeyOfValue(row Row) Value {
	for i, column := range t.Columns {
		if column.PrimaryKey {
			return row[i]
		}
	}
	return nil
}

func (t *Table) ToString() string {
	str := fmt.Sprintf("TABLE_NAME: %s\n", t.Name)
	str += "COLUMNS: { \n"
	for _, column := range t.Columns {
		str += column.ToString()
		str += "\n"
	}
	return str + "}"
}

type ColumnV struct {
	Name         string
	DataType     DataType
	Nullable     bool
	DefaultValue Value
	PrimaryKey   bool
	IsIndex      bool
}

func (c *ColumnV) ToString() string {
	dataTypeInfo := GetDataTypeInfo(c.DataType)
	col_desc := fmt.Sprintf("%s %s", c.Name, dataTypeInfo)
	if c.PrimaryKey {
		col_desc += " PRIMARY KEY"
	}
	if !c.Nullable && !c.PrimaryKey {
		col_desc += " NOT NULL"
	}
	if c.DefaultValue != nil {
		col_desc += " DEFAULT " + string(c.DefaultValue.Bytes())
	}
	return col_desc
}
