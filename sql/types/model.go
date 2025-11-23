package types

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"strconv"
	"strings"
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

func EvaluateExpr(expr *Expression, lcols []string, lrows []Value, rcols []string, rrows []Value) Value {
	if expr.Field != "" {
		lpos := -1
		for i, lcol := range lcols {
			if lcol == expr.Field {
				lpos = i
			}
		}
		if lpos == -1 {
			util.Error("HashJoinExecutor: can not find join field in left")
		}
		return lrows[lpos]
	}
	if expr.ConstVal != nil {
		return expr.ConstVal
	}

	if expr.OperationVal != nil {
		switch expr.OperationVal.(type) {
		case *OperationEqual:
			equal := expr.OperationVal.(*OperationEqual)
			lv := EvaluateExpr(equal.Left, lcols, lrows, rcols, rrows)
			rv := EvaluateExpr(equal.Right, lcols, lrows, rcols, rrows)
			return CompareValue(lv, rv, expr.OperationVal)
		case *OperationGreaterThan:
			greaterThan := expr.OperationVal.(*OperationGreaterThan)
			lv := EvaluateExpr(greaterThan.Left, lcols, lrows, rcols, rrows)
			rv := EvaluateExpr(greaterThan.Right, lcols, lrows, rcols, rrows)
			return CompareValue(lv, rv, expr.OperationVal)
		case *OperationLessThan:
			lessThan := expr.OperationVal.(*OperationLessThan)
			lv := EvaluateExpr(lessThan.Left, lcols, lrows, rcols, rrows)
			rv := EvaluateExpr(lessThan.Right, lcols, lrows, rcols, rrows)
			return CompareValue(lv, rv, expr.OperationVal)
		}
	}
	util.Error("[EvaluateExpr] not support operation")
	return nil
}

func CompareValue(lv, rv Value, operation Operation) Value {
	switch operation.(type) {
	case *OperationEqual:
		// lv 小返回-1, lv大返回1, 相等返回0; 不能比较的返回错误; 只要一方为null, 返回-2;
		compare := lv.Compare(rv)
		if compare == 0 {
			return &ConstBool{Value: true}
		}
		return &ConstBool{Value: false}
	case *OperationGreaterThan:
		compare := lv.Compare(rv)
		if compare == 1 {
			return &ConstBool{Value: true}
		}
		return &ConstBool{Value: false}
	case *OperationLessThan:
		compare := lv.Compare(rv)
		if compare == -1 {
			return &ConstBool{Value: true}
		}
		return &ConstBool{Value: false}
	}
	return nil
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
	Compare(c Const) int
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
func (i *ConstInt) Compare(c Const) int {
	switch c.(type) {
	case *ConstInt:
		if i.Value == c.(*ConstInt).Value {
			return 0
		}
		if i.Value < c.(*ConstInt).Value {
			return -1
		}
		return 1
	case *ConstFloat:
		if float64(i.Value) == c.(*ConstFloat).Value {
			return 0
		}
		if float64(i.Value) < c.(*ConstFloat).Value {
			return -1
		}
		return 1
	case *ConstNull:
		return 1
	default:
		return 0
	}
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
func (f *ConstFloat) Compare(c Const) int {
	switch c.(type) {
	case *ConstInt:
		if f.Value == float64(c.(*ConstInt).Value) {
			return 0
		}
		if f.Value < float64(c.(*ConstInt).Value) {
			return -1
		}
		return 1
	case *ConstFloat:
		if f.Value == c.(*ConstFloat).Value {
			return 0
		}
		if f.Value < c.(*ConstFloat).Value {
			return -1
		}
		return 1
	case *ConstNull:
		return 1
	default:
		return 0
	}
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

func (s *ConstString) Compare(c Const) int {
	switch c.(type) {
	case *ConstString:
		return strings.Compare(s.Value, c.(*ConstString).Value)
	case *ConstNull:
		return 1
	default:
		return 0
	}
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
func (b *ConstBool) Compare(c Const) int {
	switch c.(type) {
	case *ConstBool:
		if b.Value == c.(*ConstBool).Value {
			return 0
		}
		if b.Value {
			return 1
		}
		return -1
	case *ConstNull:
		return 1
	default:
		return 0
	}
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
func (n *ConstNull) Compare(c Const) int {
	switch c.(type) {
	case *ConstNull:
		return 0
	default:
		return -1
	}
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
	return fmt.Sprintf("TRANSACTION %d COMMIT", b.Version)

}

type RollbackResult struct {
	Version int
}

func (b *RollbackResult) ToString() string {
	return fmt.Sprintf("TRANSACTION %d ROLLBACK", b.Version)

}

type ErrorResult struct {
	ErrorMessage string
}

func (e *ErrorResult) ToString() string {
	return fmt.Sprintf("ERROR: %s", e.ErrorMessage)
}
func Remove(index []Value, value Value) []Value {
	for i, v := range index {
		if v.Compare(value) == 0 {
			return append(index[:i], index[i+1:]...)
		}
	}
	return index
}
