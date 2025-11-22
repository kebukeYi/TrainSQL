package parser

import (
	"github.com/kebukeYi/TrainSQL/sql/plan"
	"github.com/kebukeYi/TrainSQL/sql/types"
	"github.com/kebukeYi/TrainSQL/sql/util"
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
		if token.Type == KEYWORD {
			switch token.Value {
			case Create:
				return p.parseDdl()
			case Drop:
				return p.parseDdl()
			case Insert:
				return p.parseInsert()
			case Select:
				return p.parseSelect()
			case Update:
				return p.parseUpdate()
			case Delete:
				return p.parseDelete()
			case Begin:
				return p.parseTransaction()
			case Commit:
				return p.parseTransaction()
			case Rollback:
				return p.parseTransaction()
			default:
				panic("unhandled default case")
			}
			return nil
		}
		util.Error("[Parser] #parseStatement: Unexpected token: %s\n", token.ToString())
	}
	return nil
}
func (p *Parser) parseDdl() Statement {
	token := p.next()
	if token == nil {
		panic("token is not create drop token;")
	}
	value := token.Value
	if value == Create {
		if token2 := p.next(); token2 == nil {
			panic("table Execute is not null")
		} else {
			if token2.Value == Table {
				return p.parseDdlCreateTable()
			} else if token2.Value == Index {
				return p.parseDdlCreateIndex()
			} else {
				util.Error("[Parser] #parseDdl: Unhandled default case: %s\n;", token2.Value)
			}
		}
	} else if value == Drop {
		return p.parseDdlDropTable()
	} else {
		panic("unhandled parseDdl default case")
	}
	return nil
}
func (p *Parser) parseDdlCreateTable() Statement {
	if token := p.next(); token == nil {
		panic("table Execute is not create")
	} else if token.Value != Table {
		util.Error("")
	}
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
func (p *Parser) parseDdlDropTable() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Table})
	dropTableData := &DropTableData{
		TableName: p.nextIdent(),
	}
	return dropTableData
}
func (p *Parser) parseDdlCreateIndex() Statement {
	return nil
}
func (p *Parser) parseExpression() *types.Expression {
	token := p.next()
	var con types.Const
	switch token.Type {
	case IDENT:
		// 函数
		// count(col_name)
		if p.nextIfToken(&Token{Type: OPENPAREN, Value: OpenPar}) != nil {
			colName := p.nextIdent()
			p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
			return &types.Expression{Function: types.Function{L: string(token.Value), R: colName}}
		} else {
			return &types.Expression{Field: string(token.Value)}
		}
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
	case OPENPAREN:
		expression := p.computeMathOperator(1)
		p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
		return expression
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
		Name:         filedName,
		DateType:     dataType,
		Nullable:     true,
		DefaultValue: nil,
		PrimaryKey:   false,
		IsIndex:      false,
	}
	// 解析列的默认值，以及是否可以为空;
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
			case Primary:
				p.nextExpect(&Token{Type: KEYWORD, Value: Key})
				column.PrimaryKey = true
			case Index:
				column.IsIndex = true
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
	// 表名
	tableName := p.nextIdent()
	var columns []string
	// 查看是否给指定的列进行 insert
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
	selectData := &SelectData{}
	selectData.SelectCol = p.parseSelectClause()
	selectData.From = p.parseFromClause()
	selectData.WhereClause = p.parseWhereClause()
	selectData.GroupBy = p.parseGroupByClause()
	selectData.Having = p.parseHavingClause()
	selectData.OrderBy = p.parseOrderByClause()
	selectData.Limit = p.parseLimitClause()
	selectData.Offset = p.parseOffsetClause()
	return selectData
}

func (p *Parser) parseSelectClause() map[*types.Expression]string {
	p.nextExpect(&Token{Type: KEYWORD, Value: Select})
	selectCol := make(map[*types.Expression]string)
	if token := p.nextIfToken(&Token{Type: ASTERISK, Value: Asterisk}); token != nil {
		return selectCol
	}
	for {
		expression := p.parseExpression()
		if token := p.nextIfToken(&Token{Type: KEYWORD, Value: As}); token != nil {
			selectCol[expression] = p.nextIdent()
		} else {
			selectCol[expression] = ""
		}
		if token := p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token != nil {
			continue
		} else {
			break
		}
	}
	return selectCol
}
func (p *Parser) parseFromClause() FromItem {
	// From 关键字
	p.nextExpect(&Token{Type: KEYWORD, Value: From})
	// 第一个表名
	item := p.parseFromTableClause()
	// 是否有 Join
	for {
		if joinType := p.parseFromJoinClause(); joinType != -1 {
			left := item
			right := p.parseFromTableClause()
			if right == nil {
				return nil
			}
			var predicate *types.Expression
			if joinType == CrossType {
				predicate = nil
			} else {
				p.nextExpect(&Token{Type: KEYWORD, Value: On})
				l := p.parseExpression()
				p.nextExpect(&Token{Type: KEYWORD, Value: Equal})
				r := p.parseExpression()
				if joinType == RightType {
					l, r = r, l
				}
				com := &types.OperationEqual{
					Left:  l,
					Right: r,
				}
				predicate = &types.Expression{OperationVal: com}
			}
			item = &JoinItem{
				Left:      left,
				Right:     right,
				JoinType:  joinType,
				Predicate: predicate,
			}
		} else {
			return item
		}
	}
}
func (p *Parser) parseFromTableClause() FromItem {
	return &TableItem{
		TableName: p.nextIdent(),
	}
}
func (p *Parser) parseFromJoinClause() JoinType {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Cross}); token != nil {
		p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		return CrossType
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Join}); token != nil {
		return InnerType
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Left}); token != nil {
		p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		return LeftType
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Right}); token != nil {
		p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		return RightType
	}
	return -1
}
func (p *Parser) parseGroupByClause() *types.Expression {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Group}); token != nil {
		p.nextExpect(&Token{Type: KEYWORD, Value: By})
		return p.parseExpression()
	}
	return nil
}
func (p *Parser) parseHavingClause() *types.Expression {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Having}); token != nil {
		return p.parseOperationExpr()
	}
	return nil

}
func (p *Parser) parseOrderByClause() map[string]plan.OrderDirection {
	orders := make(map[string]plan.OrderDirection)
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Order}); token == nil {
		return orders
	}
	p.nextExpect(&Token{Type: KEYWORD, Value: By})
	for {
		col := p.nextIdent()
		token := p.nextIf(func(token *Token) bool {
			return token.equal(&Token{Type: KEYWORD, Value: Asc}) || token.equal(&Token{Type: KEYWORD, Value: Desc})
		})
		if token != nil {
			orders[col] = plan.Asc
			if token.equal(&Token{Type: KEYWORD, Value: Asc}) {
				orders[col] = plan.Asc
			} else {
				orders[col] = plan.Desc
			}
		}
		if token := p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token != nil {
			continue
		} else {
			break
		}
	}
	return orders

}
func (p *Parser) parseLimitClause() *types.Expression {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Limit}); token != nil {
		return p.parseExpression()
	}
	return nil
}

func (p *Parser) parseOffsetClause() *types.Expression {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Offset}); token != nil {
		return p.parseExpression()
	}
	return nil
}
func (p *Parser) parseDelete() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Delete})
	p.nextExpect(&Token{Type: KEYWORD, Value: From})
	tableName := p.nextIdent()
	return &DeleteData{
		TableName: tableName,
	}

}
func (p *Parser) parseUpdate() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Update})
	tableName := p.nextIdent()
	p.nextExpect(&Token{Type: KEYWORD, Value: Set})
	columns := make(map[string]*types.Expression)
	for {
		// update user set name = 'tom', age = 18 where id = 1;
		// update user set age = age + 1  where id = 1;
		// update user set age = 12  where id = 1;
		// update user set age = 12+12  where id = 1;
		colName := p.nextIdent()
		p.nextExpect(&Token{Type: EQUAL, Value: Equal})
		value := p.parseExpression()
		if _, ok := columns[colName]; ok {
			util.Error("column is already exists")
		}
		columns[colName] = value
		if p.nextIfToken(&Token{Type: COMMA, Value: Comma}) == nil {
			break
		}
	}
	whereClause := p.parseWhereClause()
	return &UpdateData{
		TableName:   tableName,
		WhereClause: whereClause,
		Columns:     columns,
	}
}

func (p *Parser) parseWhereClause() *types.Expression {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Where}); token != nil {
		return nil
	}
	return p.parseOperationExpr()
}

func (p *Parser) parseOperationExpr() *types.Expression {
	left := p.parseExpression()
	if next := p.next(); next != nil {
		switch next.Type {
		case EQUAL:
			return &types.Expression{
				OperationVal: &types.OperationEqual{
					Left:  left,
					Right: p.computeMathOperator(1),
				},
			}
		case GREATERTHAN:
			return &types.Expression{
				OperationVal: &types.OperationGreaterThan{
					Left:  left,
					Right: p.computeMathOperator(1),
				},
			}
		case LESSTHAN:
			return &types.Expression{
				OperationVal: &types.OperationLessThan{
					Left:  left,
					Right: p.computeMathOperator(1),
				},
			}
		default:
			util.Error("unhandled default case %s\n;", next.ToString())
		}
	} else {
		panic("unhandled default case")
	}
	return nil
}
func (p *Parser) parseTransaction() Statement {
	if next := p.next(); next != nil {
		if next.Value == Begin {
			return &BeginData{}
		} else if next.Value == Commit {
			return &CommitData{}
		} else if next.Value == Rollback {
			return &RollbackData{}
		} else {
			util.Error("[parseTransaction] unhandled default case %s\n;", next.ToString())
		}
	}
	return nil
}

func (p *Parser) parseExplain() Statement {
	p.nextExpect(&Token{Type: KEYWORD, Value: Explain})
	if next := p.peek(); next != nil {
		if next.Value == Explain {
			util.Error("can not nest explain statement")
		}
	}
	statement := p.parseStatement()
	return &ExplainData{
		Statement: statement,
	}
}

func (p *Parser) parseDdlCreateView() Statement {
	return nil
}
