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

func GetDataTypeInfo(dataType DataType) string {
	switch dataType {
	case Integer:
		return "Integer"
	case Float:
		return "Float"
	case String:
		return "String"
	case Null:
		return "Null"
	default:
		return "UNKNOWN"
	}
}

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

func (e *Expression) ToString() string {
	if e.Field != "" {
		return fmt.Sprintf("%s", e.Field)
	} else if e.Function != nil {
		return fmt.Sprintf("%s(%s)", e.Function.FuncName, e.Function.ColName)
	} else if e.OperationVal != nil {
		switch e.OperationVal.(type) {
		case *OperationEqual:
			return fmt.Sprintf("%s = %s", e.OperationVal.(*OperationEqual).Left.ToString(), e.OperationVal.(*OperationEqual).Right.ToString())
		case *OperationGreaterThan:
			return fmt.Sprintf("%s > %s", e.OperationVal.(*OperationGreaterThan).Left.ToString(), e.OperationVal.(*OperationGreaterThan).Right.ToString())
		case *OperationLessThan:
			return fmt.Sprintf("%s < %s", e.OperationVal.(*OperationLessThan).Left.ToString(), e.OperationVal.(*OperationLessThan).Right.ToString())
		}
	} else if e.ConstVal != nil {
		return fmt.Sprintf("%s", e.ConstVal.Bytes())
	}
	return ""
}

func EvaluateExpr(expr *Expression, lcols []string, lrows []Value, rcols []string, rrows []Value) (Value, error) {
	// 假如字段类型不为空, 那就默认获取左表字段值;
	// note:仅仅解析第一对参数值, 所以以后传参只传第一对即;
	if expr.Field != "" {
		lpos := -1
		for i, lcol := range lcols {
			if lcol == expr.Field {
				lpos = i
				break
			}
		}
		if lpos == -1 {
			return nil, util.Error("#EvaluateExpr: can not find join field[%s] in left", expr.Field)
		}
		return lrows[lpos], nil
	}

	// 过滤类型是 常量值, 直接返回即可;
	if expr.ConstVal != nil {
		return expr.ConstVal, nil
	}

	// 左列值 和 右列值进行比较;
	if expr.OperationVal != nil {
		switch expr.OperationVal.(type) {
		case *OperationEqual:
			equal := expr.OperationVal.(*OperationEqual)
			// 传入左列表达式, 左列值;
			lv, err := EvaluateExpr(equal.Left, lcols, lrows, rcols, rrows)
			if err != nil {
				return nil, err
			}
			// 传入右列表达式, 右列值;
			rv, err := EvaluateExpr(equal.Right, rcols, rrows, lcols, lrows)
			if err != nil {
				return nil, err
			}
			// 进行值的具体比较;
			return OperationCompareValue(lv, rv, expr.OperationVal)
		case *OperationGreaterThan:
			greaterThan := expr.OperationVal.(*OperationGreaterThan)
			lv, err := EvaluateExpr(greaterThan.Left, lcols, lrows, rcols, rrows)
			if err != nil {
				return nil, err
			}
			rv, err := EvaluateExpr(greaterThan.Right, rcols, rrows, lcols, lrows)
			if err != nil {
				return nil, err
			}
			return OperationCompareValue(lv, rv, expr.OperationVal)
		case *OperationLessThan:
			lessThan := expr.OperationVal.(*OperationLessThan)
			lv, err := EvaluateExpr(lessThan.Left, lcols, lrows, rcols, rrows)
			if err != nil {
				return nil, err
			}
			rv, err := EvaluateExpr(lessThan.Right, rcols, rrows, lcols, lrows)
			if err != nil {
				return nil, err
			}
			return OperationCompareValue(lv, rv, expr.OperationVal)
		}
		return nil, util.Error("#EvaluateExpr: not support operation")
	}
	return nil, nil
}

func OperationCompareValue(lv, rv Value, operation Operation) (Value, error) {
	switch operation.(type) {
	case *OperationEqual:
		// lv 小返回-1, lv大返回1, 相等返回0; 不能比较的返回错误;
		if allow, compare := lv.PartialCmp(rv); allow {
			if compare == 0 {
				return &ConstBool{Value: true}, nil
			}
		} else {
			return nil, util.Error("#OperationCompareValue OperationEqual can not compare value")
		}
		return &ConstBool{Value: false}, nil
	case *OperationGreaterThan:
		if allow, compare := lv.PartialCmp(rv); allow {
			if compare == 1 {
				return &ConstBool{Value: true}, nil
			}
		} else {
			return nil, util.Error("#OperationCompareValue OperationGreaterThan can not compare value")
		}
		return &ConstBool{Value: false}, nil
	case *OperationLessThan:
		if allow, compare := lv.PartialCmp(rv); allow {
			if compare == -1 {
				return &ConstBool{Value: true}, nil
			}
		} else {
			return nil, util.Error("#OperationCompareValue OperationLessThan can not compare value")
		}
		return &ConstBool{Value: false}, nil
	}
	return nil, nil
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
	PartialCmp(c Const) (bool, int)
	Hash() uint32
}

type ConstInt struct {
	Value int64
}

func NewConstInt(value int64) *ConstInt {
	return &ConstInt{Value: value}
}

func (i *ConstInt) Hash() uint32 {
	return util.Hash(i.Bytes())
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
func (i *ConstInt) PartialCmp(c Const) (bool, int) {
	switch c.(type) {
	case *ConstInt:
		if i.Value == c.(*ConstInt).Value {
			return true, 0
		}
		if i.Value < c.(*ConstInt).Value {
			return true, -1
		}
		return true, 1
	case *ConstFloat:
		if float64(i.Value) == c.(*ConstFloat).Value {
			return true, 0
		}
		if float64(i.Value) < c.(*ConstFloat).Value {
			return true, -1
		}
		return true, 1
	case *ConstNull:
		// 任何数据类型 vs null 类型, 结果都比null值大;
		return true, 1
	default:
		// 其他类型, 则不允许比较;
		return false, 0
	}
}

type ConstFloat struct {
	Value float64
}

func NewConstFloat(value float64) *ConstFloat {
	return &ConstFloat{Value: value}
}

func (f *ConstFloat) Hash() uint32 {
	return util.Hash(f.Bytes())
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
func (f *ConstFloat) PartialCmp(c Const) (bool, int) {
	switch c.(type) {
	case *ConstInt:
		if f.Value == float64(c.(*ConstInt).Value) {
			return true, 0
		}
		if f.Value < float64(c.(*ConstInt).Value) {
			return true, -1
		}
		return true, 1
	case *ConstFloat:
		if f.Value == c.(*ConstFloat).Value {
			return true, 0
		}
		if f.Value < c.(*ConstFloat).Value {
			return true, -1
		}
		return true, 1
	case *ConstNull:
		return true, 1
	default:
		return false, 0
	}
}

type ConstString struct {
	Value string
}

func NewConstString(value string) *ConstString {
	return &ConstString{Value: value}
}

func (s *ConstString) Hash() uint32 {
	return util.Hash(s.Bytes())
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

func (s *ConstString) PartialCmp(c Const) (bool, int) {
	switch c.(type) {
	case *ConstString:
		return true, strings.Compare(s.Value, c.(*ConstString).Value)
	case *ConstNull:
		return true, 1
	default:
		return false, 0
	}
}

type ConstBool struct {
	Value bool
}

func (b *ConstBool) Hash() uint32 {
	return util.Hash(b.Bytes())
}

func NewConstBool(value bool) *ConstBool {
	return &ConstBool{Value: value}
}
func (b *ConstBool) Bytes() []byte {
	if b.Value { // // 布尔值转换为单个字节
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
func (b *ConstBool) PartialCmp(c Const) (bool, int) {
	switch c.(type) {
	case *ConstBool:
		if b.Value == c.(*ConstBool).Value {
			return true, 0
		}
		if b.Value {
			return true, 1
		}
		return true, -1
	case *ConstNull:
		return true, 1
	default:
		return false, 0
	}
}

type ConstNull struct {
}

func NewConstNull() *ConstNull {
	return &ConstNull{}
}

func (n *ConstNull) Hash() uint32 {
	return util.Hash(n.Bytes())
}
func (n *ConstNull) Bytes() []byte {
	return []byte("null")
}
func (n *ConstNull) Into() interface{} {
	return []byte("null")
}
func (n *ConstNull) DateType() DataType {
	return Null
}
func (n *ConstNull) PartialCmp(c Const) (bool, int) {
	switch c.(type) {
	case *ConstNull:
		return true, 0
	default:
		// null 和 除了null值, 其他比较的结果都比null值小;
		return true, -1
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

// FormatScanResult 格式化扫描结果
func FormatScanResult(columns []string, rows []Row) string {
	if len(rows) == 0 {
		return fmt.Sprintf("%s\n%s\n(0 rows)", formatColumns(columns, make([]int, len(columns))),
			formatSeparator(make([]int, len(columns))))
	}

	// 找到每一列最大的长度
	maxLen := make([]int, len(columns))
	for i, col := range columns {
		maxLen[i] = len(col)
	}
	// 遍历所有行，更新最大长度
	for _, row := range rows {
		for i, value := range row {
			if len(value.Bytes()) > maxLen[i] {
				maxLen[i] = len(value.Bytes())
			}
		}
	}

	// 格式化列名
	formattedColumns := formatColumns(columns, maxLen)

	// 生成分隔线
	separator := formatSeparator(maxLen)

	// 格式化行数据
	formattedRows := formatRows(rows, maxLen)

	// 组合最终结果
	return fmt.Sprintf("%s\n%s\n%s\n(%d rows)",
		formattedColumns, separator, formattedRows, len(rows))
}

// formatColumns 格式化列名
func formatColumns(columns []string, maxLen []int) string {
	formatted := make([]string, len(columns))
	for i, col := range columns {
		// 使用左对齐，宽度为 maxLen[i]
		formatted[i] = fmt.Sprintf("%-*s", maxLen[i], col)
	}
	return strings.Join(formatted, " |")
}

// formatSeparator 生成分隔线
func formatSeparator(maxLen []int) string {
	separators := make([]string, len(maxLen))
	for i, length := range maxLen {
		// 生成 length+1 个横线
		separators[i] = strings.Repeat("-", length+1)
	}
	return strings.Join(separators, "+")
}

// formatRows 格式化行数据
func formatRows(rows []Row, maxLen []int) string {
	formattedRows := make([]string, len(rows))
	for rowIdx, row := range rows {
		cells := make([]string, len(row))
		for i, value := range row {
			strValue := string(value.Bytes())
			// 使用左对齐，宽度为 maxLen[i]
			cells[i] = fmt.Sprintf("%-*s", maxLen[i], strValue)
		}
		formattedRows[rowIdx] = strings.Join(cells, " |")
	}
	return strings.Join(formattedRows, "\n")
}

type ScanTableResult struct {
	TableName string
	Columns   []string
	Rows      []Row
}

func RowsToString(row Row) string {
	var buf strings.Builder
	for _, value := range row {
		buf.WriteString(string(value.Bytes()))
		buf.WriteString(" |")
	}
	return buf.String()
}

func (s *ScanTableResult) ToString() string {
	return FormatScanResult(s.Columns, s.Rows)
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

type ShowTableResult struct {
	TableInfo string
}

func (s *ShowTableResult) ToString() string {
	return s.TableInfo
}

type ShowDataBaseResult struct {
	TablesInfo string
}

func (s *ShowDataBaseResult) ToString() string {
	return s.TablesInfo
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
func Remove(source []Value, value Value) []Value {
	for i, v := range source {
		if allow, compare := v.PartialCmp(value); allow {
			if compare == 0 {
				return append(source[:i], source[i+1:]...)
			}
		}
	}
	return source
}
