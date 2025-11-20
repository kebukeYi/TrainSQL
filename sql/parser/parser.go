package parser

import (
	"practiceSQL/sql/types"
	"practiceSQL/sql/util"
	"strconv"
)

type Parser struct {
	lexer *Lexer
}

func NewParser(inputSQL string) *Parser {
	return &Parser{lexer: NewLexer(inputSQL)}
}

func (p *Parser) Parse() Statement {
	statement := p.parseStatement()
	p.nextExpect(&Token{Type: SEMICOLON, Value: Semicolon})
	token := p.peek()
	if token != nil {
		util.Error("Unexpected token: %s\n", token.ToString())
	}
	return statement
}
func (p *Parser) peek() *Token {
	return p.lexer.peekScan()
}

func (p *Parser) next() *Token {
	return p.lexer.next()
}

// 强制要求下一个 token 必须是标识符, 无论是否匹配，都会消耗一个 token;
// 如果不是标识符，返回 Err 中止解析;
// 搜寻表名时使用;
func (p *Parser) nextIdent() string {
	token := p.next()
	if token == nil || token.Type != IDENT {
		util.Error("Unexpected next token is ident, but is not;\n")
	}
	return string(token.Value)
}

// 如果下一个 Token 是关键字类型, 则取出; 否则不取出,返回none;
// 条件性关键字检查, 检查 字段属性中是否 含有 NULL 关键字;
func (p *Parser) nextIfKeyWord() *Token {
	return p.nextIf(func(token *Token) bool {
		return token.Type == KEYWORD
	})
}

// nextExpect: 强制消费, 要求下一个 token 必须匹配预期值,否则报错;
func (p *Parser) nextExpect(expect *Token) {
	token := p.next()
	if token == nil {
		util.Error("Expect %s, but got nil\n", expect.ToString())
	}
	if token != nil && !token.equal(expect) {
		util.Error("Expect %s, but got %s\n", expect.ToString(), token.ToString())
	}
}

// nextIfToken: 仅在匹配事消费token,不匹配时返回 None,不报错,继续解析;
func (p *Parser) nextIfToken(token *Token) *Token {
	next := p.next()
	if next != nil && !next.equal(token) {
		//util.Error("Expect %s, but got %s\n", token.ToString(), next.ToString())
		p.lexer.ReverseScan()
		return nil
	}
	return next
}

// 如果满足条件, 则取出当前 Token, 否则不取出, 并返回 None;
func (p *Parser) nextIf(fc func(*Token) bool) *Token {
	token := p.peek()
	if fc(token) {
		return p.lexer.next()
	}
	return nil
}

func (p *Parser) parseStatement() Statement {
	if token := p.peek(); token == nil {
		util.Error("[Parser] #parseStatement: Unexpected end of input.")
	} else {
		switch token.Type {
		case KEYWORD:
			switch token.Value {
			case Create:
				return p.parseDdl()
			case Insert:
				return p.parseInsert()
			case Select:
				return p.parseSelect()
			case Update:
				return p.parseUpdate()
			case Delete:
				return p.parseDelete()
			}
		default:
			panic("unhandled default case")
		}
	}
	return nil
}
func (p *Parser) parseDdl() Statement {
	if token := p.next(); token == nil {
		panic("token is not create token")
	}
	if token2 := p.next(); token2 == nil {
		panic("table Name is not create")
	} else {
		if token2.Value == Table {
			return p.parseDdlCreateTable()
		} else if token2.Value == Index {
			return p.parseDdlCreateIndex()
		}
	}
	return nil
}

func (p *Parser) parseDdlCreateTable() Statement {
	tableName := p.nextIdent()
	p.nextExpect(&Token{Type: OPENPAREN, Value: OpenPar})
	var columns []*types.Column
	// CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);
	for {
		column := p.parseDdlColumn()
		columns = append(columns, column)
		if token := p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token == nil {
			break
		}
	}
	p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
	creatTableData := &CreatTableData{
		TableName: tableName,
		Columns:   columns,
	}
	return creatTableData
}

func (p *Parser) parseDdlCreateIndex() Statement {
	return nil
}
func (p *Parser) parseExpression() *types.Expression {
	token := p.next()
	var con types.Const
	switch token.Type {
	case NUMBER:
		if util.IsIntegerStrict(string(token.Value)) {
			v, _ := strconv.ParseInt(string(token.Value), 10, 64)
			con = &types.ConstInt{
				Value: v,
			}
		} else {
			v, _ := strconv.ParseFloat(string(token.Value), 64)
			con = &types.ConstFloat{
				Value: v,
			}
		}
	case STRING:
		con = &types.ConstString{
			Value: string(token.Value),
		}
	case KEYWORD:
		switch token.Value {
		case Null:
			con = &types.ConstNull{}
		case True:
			con = &types.ConstBool{
				Value: true,
			}
		case False:
			con = &types.ConstBool{
				Value: false,
			}
		default:
			panic("keyword is not support")
		}
	default:
		panic("unhandled default case")
	}
	return types.NewExpression(con)
}

// parseDdlColumn
func (p *Parser) parseDdlColumn() *types.Column {
	// CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);
	filedName := p.nextIdent()
	dataTypeToken := p.next()
	if dataTypeToken == nil || dataTypeToken.Type != KEYWORD {
		panic("dataType is not nil")
	}
	dataType := p.parserDataType(dataTypeToken)
	column := &types.Column{
		Name:     filedName,
		DateType: dataType,
		Nullable: true,
	}
	for {
		// column_name INT NOT NULL DEFAULT 0
		if token := p.nextIfKeyWord(); token != nil {
			switch token.Value {
			case Null:
				column.Nullable = true
			case Not:
				p.nextExpect(&Token{Type: KEYWORD, Value: Null})
				column.Nullable = false
			case Default:
				column.DefaultValue = p.parseExpression()
			default:
				util.Error("[Parser] Unexpected keyword: %s\n", token.Value)
			}
		} else {
			break
		}
	}
	return column
}

func (p *Parser) parserDataType(token *Token) types.DataType {
	switch token.Value {
	case Int:
		return types.Integer
	case Integer:
		return types.Integer
	case String:
		return types.String
	case Varchar:
		return types.String
	case Text:
		return types.String
	case Char:
		return types.String
	case Bool:
		return types.Boolean
	case Boolean:
		return types.Boolean
	case Float:
		return types.Float
	case Double:
		return types.Float
	default:
		panic("dataType is not support")
	}
}
func (p *Parser) parseInsert() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Insert})
	p.nextExpect(&Token{Type: KEYWORD, Value: Into})
	tableName := p.nextIdent()
	var columns []string
	if ifToken := p.nextIfToken(&Token{Type: OPENPAREN, Value: OpenPar}); ifToken != nil {
		var cols []string
		// insert into tbl (a, b, c) Values (1, 2, 3),(4, 5, 6);
		for {
			ident := p.nextIdent()
			cols = append(cols, ident)
			if next := p.next(); next.Type == COMMA {
				continue
			} else if next.Type == CLOSEPAREN {
				break
			} else {
				util.Error("Unexpected token: %s\n", next.ToString())
			}
		}
		columns = cols
	}
	p.nextExpect(&Token{Type: KEYWORD, Value: Values})
	// insert into tbl (a, b, c) Values (1, 2, 3),(4, 5, 6);
	var values [][]*types.Expression
	for {
		p.nextExpect(&Token{Type: OPENPAREN, Value: OpenPar})
		var exprs []*types.Expression
		for {
			expression := p.parseExpression()
			exprs = append(exprs, expression)
			if next := p.next(); next.Type == COMMA {
				continue
			} else if next.Type == CLOSEPAREN {
				break
			} else {
				util.Error("Unexpected token: %s\n", next.ToString())
			}
		}
		values = append(values, exprs)
		// insert into tbl(a, b, c) Values (1, 2, 3),(4, 5, 6);
		if p.nextIfToken(&Token{Type: COMMA, Value: Comma}) == nil {
			break
		}
	}
	return &InsertData{
		TableName: tableName,
		Values:    values,
		Columns:   columns,
	}
}
func (p *Parser) parseSelect() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Select})
	p.nextExpect(&Token{Type: ASTERISK, Value: Asterisk})
	p.nextExpect(&Token{Type: KEYWORD, Value: From})
	tableName := p.nextIdent()
	// p.nextExpect(&Token{Type: KEYWORD, Value: Where})
	return &SelectData{
		TableName: tableName,
	}
}
func (p *Parser) parseDelete() Statement {
	return &DeleteData{}
}
func (p *Parser) parseUpdate() Statement {
	return &UpdateData{}
}
