package types

import (
	"fmt"
	"strconv"
)

type DataType int32

const (
	Boolean DataType = iota
	Integer
	Float
	String
	Null
)

type Column struct {
	Name         string
	DateType     DataType
	Nullable     bool
	DefaultValue *Expression
}

type Expression struct {
	V Const
}

func NewExpression(con Const) *Expression {
	return &Expression{V: con}
}

type Const interface {
	Into() interface{}
	Bytes() []byte
	DateType() DataType
}

type ConstInt struct {
	Value int64
}

func (i *ConstInt) Into() interface{} {
	return i.Value
}
func (i *ConstInt) Bytes() []byte {
	return []byte(strconv.FormatInt(i.Value, 10))
}
func (i *ConstInt) DateType() DataType {
	return Integer
}

type ConstFloat struct {
	Value float64
}

func (f *ConstFloat) Into() interface{} {
	return f.Value
}
func (f *ConstFloat) Bytes() []byte {
	return []byte(strconv.FormatFloat(f.Value, 'f', -1, 64))
}
func (f *ConstFloat) DateType() DataType {
	return Float
}

type ConstString struct {
	Value string
}

func (s *ConstString) Bytes() []byte {
	return []byte(s.Value)
}

func (s *ConstString) Into() interface{} {
	return s.Value
}

func (s *ConstString) DateType() DataType {
	return String
}

type ConstBool struct {
	Value bool
}

func (b *ConstBool) Bytes() []byte {
	if b.Value {
		return []byte("true")
	}
	return []byte("false")
}

func (b *ConstBool) Into() interface{} {
	return b.Into
}
func (b *ConstBool) DateType() DataType {
	return Boolean
}

type ConstNull struct {
	Value struct{}
}

func (n *ConstNull) Bytes() []byte {
	return []byte("null")
}

func (n *ConstNull) Into() interface{} {
	return n.Value
}
func (n *ConstNull) DateType() DataType {
	return Null
}

type Value Const
type Row []Value

type ResultSet interface {
	ToString()
}

type CreateTableResult struct {
	TableName string
}

func (c *CreateTableResult) ToString() {
	fmt.Println("CreateTableResult:", c.TableName)
}

type InsertTableResult struct {
	Count int
}

func (i *InsertTableResult) ToString() {
	fmt.Println("InsertTableResult:", i.Count)
}

type SelectTableResult struct {
	Columns []string
	Rows    []Row
}

func (s *SelectTableResult) ToString() {
	fmt.Println("SelectTableResult:")
	for _, row := range s.Rows {
		for _, value := range row {
			fmt.Printf("%s ", value.Bytes())
		}
		fmt.Println()
	}
}
