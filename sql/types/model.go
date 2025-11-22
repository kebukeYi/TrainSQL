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
	PrimaryKey   bool
	IsIndex      bool
}

type Expression struct {
	Field        string
	ConstVal     Const
	OperationVal Operation
	Function     *Function
}

func NewExpression(con Const) *Expression {
	return &Expression{ConstVal: con}
}

type Operation interface {
	operation()
}

type OperationEqual struct {
	Left  *Expression
	Right *Expression
}

func (o *OperationEqual) operation() {

}

type OperationGreaterThan struct {
	Left  *Expression
	Right *Expression
}

func (o *OperationGreaterThan) operation() {

}

type OperationLessThan struct {
	Left  *Expression
	Right *Expression
}

func (o *OperationLessThan) operation() {

}

type Function struct {
	FuncName string
	ColName  string
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
	return b.Value
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
	ToString() string
}

type CreateTableResult struct {
	TableName string
}

func (c *CreateTableResult) ToString() string {
	return fmt.Sprintf("CREATE TABLE: %s", c.TableName)
}

type DropTableResult struct {
	TableName string
}

func (d *DropTableResult) ToString() string {
	return fmt.Sprintf("DROP TABLE: %s", d.TableName)
}

type InsertTableResult struct {
	Count int
}

func (i *InsertTableResult) ToString() string {
	return fmt.Sprintf("INSERT %d rows", i.Count)
}

type ScanTableResult struct {
	Columns []string
	Rows    []Row
}

func (s *ScanTableResult) ToString() string {
	fmt.Println("ScanTableResult:")
	for _, row := range s.Rows {
		for _, value := range row {
			fmt.Printf("%s ", value.Bytes())
		}
		fmt.Println()
	}
	return ""
}

type UpdateTableResult struct {
	Count int
}

func (u *UpdateTableResult) ToString() string {
	return fmt.Sprintf("UPDATE %d rows", u.Count)
}

type DeleteTableResult struct {
	Count int
}

func (d *DeleteTableResult) ToString() string {
	return fmt.Sprintf("DELETE %d rows", d.Count)
}

type BeginResult struct {
	Version int
}

func (b *BeginResult) ToString() string {
	return fmt.Sprintf("TRANSACTION %d BEGIN", b.Version)

}

type ExplainResult struct {
	Plan string
}

func (b *ExplainResult) ToString() string {
	return b.Plan
}

type CommitResult struct {
	Version int
}

func (b *CommitResult) ToString() string {
	return fmt.Sprintf("TRANSACTION %d COOMIT", b.Version)

}

type RollbackResult struct {
	Version int
}

func (b *RollbackResult) ToString() string {
	return fmt.Sprintf("TRANSACTION %d ROLLBACK", b.Version)

}
