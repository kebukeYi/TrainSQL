package lexer

type Tag uint32

const (
	// AND 对应SQL关键字
	AND Tag = iota + 256
	EQ
	FALSE
	GE
	ID

	LE

	FLOAT
	MINUS
	PLUS
	NE
	NUM
	REAL             // float double 类型
	LEFT_BRACE       // "{"
	RIGHT_BRACE      // "}"
	LEFT_BRACKET     //"("
	RIGHT_BRACKET    //")"
	AND_OPERATOR     //   &
	OR_OPERATOR      // |
	ASSIGN_OPERATOR  // =
	NEGATE_OPERATOR  // !
	LESS_OPERATOR    // <
	GREATER_OPERATOR // >

	SEMICOLON // ;

	// 新增SQL对应关键字
	SELECT
	FROM
	WHERE
	INSERT
	INTO
	VALUES
	DELETE
	UPDATE
	SET
	CREATE
	TABLE
	INT
	VARCHAR
	VIEW
	AS
	INDEX
	ON
	OR
	COMMA
	STRING
	EOF
	ERROR
)

var Token_map = make(map[Tag]string)

func init() {
	// 初始化SQL关键字对应字符串
	Token_map[AND] = "AND"
	Token_map[SELECT] = "SELECT"
	Token_map[WHERE] = "where"
	Token_map[INSERT] = "INSERT"
	Token_map[INTO] = "INTO"
	Token_map[VALUES] = "VALUES"
	Token_map[DELETE] = "DELETE"
	Token_map[UPDATE] = "UPDATE"
	Token_map[SET] = "SET"
	Token_map[CREATE] = "CREATE"
	Token_map[TABLE] = "TABLE"
	Token_map[INT] = "INT"
	Token_map[VARCHAR] = "VARCHAR"
	Token_map[VIEW] = "VIEW"
	Token_map[AS] = "AS"
	Token_map[INDEX] = "INDEX"
	Token_map[ON] = "ON"
	Token_map[COMMA] = ","

	Token_map[EQ] = "EQ"
	Token_map[FALSE] = "FALSE"
	Token_map[GE] = "GE"
	Token_map[ID] = "ID"
	Token_map[INT] = "int"
	Token_map[FLOAT] = "float"

	Token_map[LE] = "<="
	Token_map[MINUS] = "-"
	Token_map[PLUS] = "+"
	Token_map[NE] = "!="
	Token_map[NUM] = "NUM"
	Token_map[OR] = "OR"
	Token_map[REAL] = "REAL"
	Token_map[AND_OPERATOR] = "&"
	Token_map[OR_OPERATOR] = "|"
	Token_map[ASSIGN_OPERATOR] = "="
	Token_map[NEGATE_OPERATOR] = "!"
	Token_map[LESS_OPERATOR] = "<"
	Token_map[GREATER_OPERATOR] = ">"
	Token_map[LEFT_BRACE] = "{"
	Token_map[RIGHT_BRACE] = "}"
	Token_map[LEFT_BRACKET] = "("
	Token_map[RIGHT_BRACKET] = ")"
	Token_map[SEMICOLON] = ";"
	Token_map[EOF] = "EOF"
	Token_map[ERROR] = "ERROR"
	Token_map[SEMICOLON] = ";"
}

type Token struct {
	lexeme string
	Tag    Tag
}

func (t *Token) ToString() string {
	if t.lexeme == "" {
		return Token_map[t.Tag]
	}
	return t.lexeme
}

func NewToken(tag Tag) Token {
	return Token{
		lexeme: "",
		Tag:    tag,
	}
}

func NewTokenWithString(tag Tag, lexeme string) Token {
	return Token{
		lexeme: lexeme,
		Tag:    tag,
	}
}
