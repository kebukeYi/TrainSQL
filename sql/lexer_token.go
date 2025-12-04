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
	GREATERTHAN           // 大于 >
	LESSTHAN              // 小于 <
)

type TokenValue string

// KEYWORD 类别关键字如下:
const (
	Show     TokenValue = "SHOW"
	DataBase TokenValue = "DATABASE"
	Create   TokenValue = "CREATE"
	Table    TokenValue = "TABLE"
	Index    TokenValue = "INDEX"
	Int      TokenValue = "INT"
	Integer  TokenValue = "INTEGER"
	Boolean  TokenValue = "BOOLEAN"
	Bool     TokenValue = "BOOL"
	String   TokenValue = "STRING"
	Text     TokenValue = "TEXT"
	Varchar  TokenValue = "VARCHAR"
	Char     TokenValue = "CHAR"
	Float    TokenValue = "FLOAT"
	Double   TokenValue = "DOUBLE"
	Select   TokenValue = "SELECT"
	From     TokenValue = "FROM"
	Insert   TokenValue = "INSERT"
	Into     TokenValue = "INTO"
	Values   TokenValue = "VALUES"
	True     TokenValue = "TRUE"
	False    TokenValue = "FALSE"
	Default  TokenValue = "DEFAULT"
	Not      TokenValue = "NOT"
	Null     TokenValue = "NULL"
	Primary  TokenValue = "PRIMARY"
	Key      TokenValue = "KEY"
	Update   TokenValue = "UPDATE"
	Set      TokenValue = "SET"
	Where    TokenValue = "WHERE"
	Delete   TokenValue = "DELETE"
	Drop     TokenValue = "DROP"
	On       TokenValue = "ON"
	Asc      TokenValue = "ASC"
	As       TokenValue = "AS"
	Desc     TokenValue = "DESC"
	Limit    TokenValue = "LIMIT"
	Offset   TokenValue = "OFFSET"
	Group    TokenValue = "GROUP"
	By       TokenValue = "BY"
	Having   TokenValue = "HAVING"
	Order    TokenValue = "ORDER"

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

func (t *Token) computeExpr(l, r *types.Expression) (*types.Expression, error) {
	var val float64
	var err error
	var con types.Const
	if l.ConstVal != nil && r.ConstVal != nil {
		left, ok := l.ConstVal.Into().(float64)
		if !ok {
			return nil, util.Error("#computeExpr cannot compute the left Expression %s", l.ConstVal.Into())
		}
		right, ok := r.ConstVal.Into().(float64)
		if !ok {
			return nil, util.Error("#computeExpr cannot compute the right Expression %s", r.ConstVal.Into())
		}
		val, err = t.compute(left, right)
		if err != nil {
			return nil, err
		}
	} else {
		// 存在一方的值为空;
		return nil, util.Error("#computeExpr cannot compute the left right Expression")
	}
	con = &types.ConstFloat{Value: val}
	return types.NewExpression(con), nil
}

func (t *Token) compute(l, r float64) (float64, error) {
	if t.Type == MINUS {
		return l - r, nil
	}
	if t.Type == ASTERISK {
		return l * r, nil
	}
	if t.Type == SLASH {
		return l / r, nil
	}
	if t.Type == PLUS {
		return l + r, nil
	}
	return -1, util.Error("#compute Unexpected operator: %s", t.ToString())
}
func (t *Token) equal(s *Token) bool {
	return t.Type == s.Type && t.Value == s.Value
}

func (t *Token) ToString() string {
	return fmt.Sprintf("Token{Type:%d, Value:%s}", t.Type, t.Value)
}

func InitWord() map[string]*Token {
	return map[string]*Token{
		"SHOW":     NewToken(KEYWORD, Show),
		"DATABASE": NewToken(KEYWORD, DataBase),

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
