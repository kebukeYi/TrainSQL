package parser

import "fmt"

type TokenType int

const (
	KEYWORD    TokenType = iota
	IDENT                // 其他类型的字符串Token，比如表名、列名
	STRING               // 字符串类型的数据
	NUMBER               // 数字
	OPENPAREN            // 左括号 (
	CLOSEPAREN           // 右括号 )
	COMMA                // 逗号
	SEMICOLON            // 分号 ;
	ASTERISK             // 星号 *
	PLUS                 // 加号 +
	MINUS                // 减号 -
	SLASH                // 斜杠 /
)

type TokenValue string

const (
	Create  TokenValue = "CREATE"
	Table   TokenValue = "TABLE"
	Index   TokenValue = "INDEX"
	Int     TokenValue = "INT"
	Integer TokenValue = "INTEGER"
	Boolean TokenValue = "BOOL"
	Bool    TokenValue = "BOOLEAN"
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
	Delete  TokenValue = "DELETE"

	OpenPar   TokenValue = "("
	ClosePar  TokenValue = ")"
	Comma     TokenValue = ","
	Semicolon TokenValue = ";"
	Plus      TokenValue = "+"
	Minus     TokenValue = "-"
	Slash     TokenValue = "/"
	Asterisk  TokenValue = "*"
)

type Token struct {
	Type  TokenType  // 不同的Token类型
	Value TokenValue // 当前值;
}

func (t *Token) equal(s *Token) bool {
	return t.Type == s.Type && t.Value == s.Value
}

func (t *Token) ToString() string {
	return fmt.Sprintf("Token{Type:%d, Vaule:%s}", t.Type, t.Value)
}

func NewToken(t TokenType, v TokenValue) *Token {
	return &Token{
		Type:  t,
		Value: v,
	}
}

func InitWord() map[string]*Token {
	return map[string]*Token{
		"CREATE":  NewToken(KEYWORD, "CREATE"),
		"TABLE":   NewToken(KEYWORD, "TABLE"),
		"KEY":     NewToken(KEYWORD, "KEY"),
		"PRIMARY": NewToken(KEYWORD, "PRIMARY"),
		"INSERT":  NewToken(KEYWORD, "INSERT"),
		"INTO":    NewToken(KEYWORD, "INTO"),
		"VALUES":  NewToken(KEYWORD, "VALUES"),
		"SELECT":  NewToken(KEYWORD, "SELECT"),
		"FROM":    NewToken(KEYWORD, "FROM"),
		"UPDATE":  NewToken(KEYWORD, "UPDATE"),
		"SET":     NewToken(KEYWORD, "SET"),
		"WHERE":   NewToken(KEYWORD, "WHERE"),
		"DELETE":  NewToken(KEYWORD, "DELETE"),
		"DROP":    NewToken(KEYWORD, "DROP"),
		"INT":     NewToken(KEYWORD, "INT"),
		"BOOL":    NewToken(KEYWORD, "BOOL"),
		"VARCHAR": NewToken(KEYWORD, "VARCHAR"),
		"FLOAT":   NewToken(KEYWORD, "FLOAT"),
		"DOUBLE":  NewToken(KEYWORD, "DOUBLE"),
		"NULL":    NewToken(KEYWORD, "NULL"),
		"NOT":     NewToken(KEYWORD, "NOT"),
		"DEFAULT": NewToken(KEYWORD, "DEFAULT"),
		"TRUE":    NewToken(KEYWORD, "TRUE"),
		"FALSE":   NewToken(KEYWORD, "FALSE"),
	}
}
