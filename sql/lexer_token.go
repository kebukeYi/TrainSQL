package sql

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

type TokenType int

const (
	KEYWORD     TokenType = iota
	IDENT                 // 其他类型的字符串Token，比如表名、列名
	STRING                // 字符串类型的数据
	NUMBER                // 数字
	OPENPAREN             // 左括号 (
	CLOSEPAREN            // 右括号 )
	COMMA                 // 逗号
	SEMICOLON             // 分号 ;
	ASTERISK              // 星号 *
	PLUS                  // 加号 +
	MINUS                 // 减号 -
	SLASH                 // 斜杠 /
	EQUAL                 // 等号 =
	GREATERTHAN           // 大于
	LESSTHAN              // 小于
)

type TokenValue string

const (
	Create  TokenValue = "CREATE"
	Table   TokenValue = "TABLE"
	Index   TokenValue = "INDEX"
	Int     TokenValue = "INT"
	Integer TokenValue = "INTEGER"
	Boolean TokenValue = "BOOLEAN"
	Bool    TokenValue = "BOOL"
	String  TokenValue = "STRING"
	Text    TokenValue = "TEXT"
	Varchar TokenValue = "VARCHAR"
	Char    TokenValue = "CHAR"
	Float   TokenValue = "FLOAT"
	Double  TokenValue = "DOUBLE"
	Select  TokenValue = "SELECT"
	From    TokenValue = "FROM"
	Insert  TokenValue = "INSERT"
	Into    TokenValue = "INTO"
	Values  TokenValue = "VALUES"
	True    TokenValue = "TRUE"
	False   TokenValue = "FALSE"
	Default TokenValue = "DEFAULT"
	Not     TokenValue = "NOT"
	Null    TokenValue = "NULL"
	Primary TokenValue = "PRIMARY"
	Key     TokenValue = "KEY"
	Update  TokenValue = "UPDATE"
	Set     TokenValue = "SET"
	Where   TokenValue = "WHERE"
	Delete  TokenValue = "DELETE"
	Drop    TokenValue = "DROP"
	On      TokenValue = "ON"
	Asc     TokenValue = "ASC"
	As      TokenValue = "AS"
	Desc    TokenValue = "DESC"
	Limit   TokenValue = "LIMIT"
	Offset  TokenValue = "OFFSET"
	Group   TokenValue = "GROUP"
	By      TokenValue = "BY"
	Having  TokenValue = "HAVING"
	Order   TokenValue = "ORDER"

	Cross TokenValue = "CROSS"
	Join  TokenValue = "JOIN"
	Left  TokenValue = "LEFT"
	Right TokenValue = "RIGHT"

	Begin    TokenValue = "BEGIN"
	Commit   TokenValue = "COMMIT"
	Rollback TokenValue = "ROLLBACK"
	Explain  TokenValue = "EXPLAIN"

	OpenPar     TokenValue = "("
	ClosePar    TokenValue = ")"
	Comma       TokenValue = ","
	Semicolon   TokenValue = ";"
	Plus        TokenValue = "+"
	Minus       TokenValue = "-"
	Slash       TokenValue = "/"
	Asterisk    TokenValue = "*"
	Equal       TokenValue = "="
	GreaterThan TokenValue = ">"
	LessThan    TokenValue = "<"
)

type Token struct {
	Type  TokenType  // 不同的Token类型
	Value TokenValue // 当前值;
}

func NewToken(t TokenType, v TokenValue) *Token {
	return &Token{
		Type:  t,
		Value: v,
	}
}

func (t *Token) isOperator() bool {
	if t.Type == MINUS || t.Type == PLUS || t.Type == ASTERISK || t.Type == SLASH {
		return true
	}
	return false
}

func (t *Token) precedence() int32 {
	if t.Type == MINUS || t.Type == PLUS {
		return 1
	}
	if t.Type == ASTERISK || t.Type == SLASH {
		return 2
	}
	return 0
}

func (t *Token) computeExpr(l, r *types.Expression) *types.Expression {
	var val float64
	var con types.Const
	if l.ConstVal != nil && r.ConstVal != nil {
		val = t.compute(l.ConstVal.Into().(float64), r.ConstVal.Into().(float64))
	} else {
		util.Error("[computeExpr] Unexpected operator: %s\n", t.Value)
	}
	con = &types.ConstFloat{Value: val}
	return types.NewExpression(con)
}

func (t *Token) compute(l, r float64) float64 {
	if t.Type == MINUS {
		return l - r
	}
	if t.Type == ASTERISK {
		return l * r
	}
	if t.Type == SLASH {
		return l / r
	}
	if t.Type == PLUS {
		return l + r
	}
	util.Error("Unexpected operator: %s\n", t.Value)
	return -1
}
func (t *Token) equal(s *Token) bool {
	return t.Type == s.Type && t.Value == s.Value
}

func (t *Token) ToString() string {
	return fmt.Sprintf("Token{Type:%d, Vaule:%s}", t.Type, t.Value)
}

func InitWord() map[string]*Token {
	return map[string]*Token{
		"CREATE": NewToken(KEYWORD, Create),
		"TABLE":  NewToken(KEYWORD, Table),

		"PRIMARY": NewToken(KEYWORD, Primary),
		"KEY":     NewToken(KEYWORD, Key),

		"INSERT": NewToken(KEYWORD, Insert),
		"INTO":   NewToken(KEYWORD, Into),
		"VALUES": NewToken(KEYWORD, Values),
		"SELECT": NewToken(KEYWORD, Select),
		"FROM":   NewToken(KEYWORD, From),
		"UPDATE": NewToken(KEYWORD, Update),
		"SET":    NewToken(KEYWORD, Set),
		"WHERE":  NewToken(KEYWORD, Where),
		"DELETE": NewToken(KEYWORD, Delete),
		"DROP":   NewToken(KEYWORD, Drop),

		"INT":     NewToken(KEYWORD, Int),
		"BOOL":    NewToken(KEYWORD, Bool),
		"INTEGER": NewToken(KEYWORD, Integer),
		"BOOLEAN": NewToken(KEYWORD, Boolean),
		"VARCHAR": NewToken(KEYWORD, Varchar),
		"CHAR":    NewToken(KEYWORD, Char),
		"FLOAT":   NewToken(KEYWORD, Float),
		"DOUBLE":  NewToken(KEYWORD, Double),
		"STRING":  NewToken(KEYWORD, String),
		"TEXT":    NewToken(KEYWORD, Text),

		"NULL":    NewToken(KEYWORD, Null),
		"NOT":     NewToken(KEYWORD, Not),
		"DEFAULT": NewToken(KEYWORD, Default),
		"TRUE":    NewToken(KEYWORD, True),
		"FALSE":   NewToken(KEYWORD, False),

		"BEGIN":    NewToken(KEYWORD, Begin),
		"COMMIT":   NewToken(KEYWORD, Commit),
		"ROLLBACK": NewToken(KEYWORD, Rollback),
		"EXPLAIN":  NewToken(KEYWORD, Explain),
		"INDEX":    NewToken(KEYWORD, Index),

		"ON":     NewToken(KEYWORD, On),
		"ASC":    NewToken(KEYWORD, Asc),
		"AS":     NewToken(KEYWORD, As),
		"DESC":   NewToken(KEYWORD, Desc),
		"LIMIT":  NewToken(KEYWORD, Limit),
		"OFFSET": NewToken(KEYWORD, Offset),
		"GROUP":  NewToken(KEYWORD, Group),
		"BY":     NewToken(KEYWORD, By),
		"HAVING": NewToken(KEYWORD, Having),
		"ORDER":  NewToken(KEYWORD, Order),
		"CROSS":  NewToken(KEYWORD, Cross),
		"JOIN":   NewToken(KEYWORD, Join),
		"LEFT":   NewToken(KEYWORD, Left),
		"RIGHT":  NewToken(KEYWORD, Right),
	}
}
