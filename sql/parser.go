package sql

import (
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

func (p *Parser) Parse() (Statement, error) {
	statement, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	if err = p.nextExpect(&Token{Type: SEMICOLON, Value: Semicolon}); err != nil {
		return nil, err
	}
	token, err := p.peek()
	if err != nil {
		return nil, err
	}
	if token != nil {
		return nil, util.Error("Unexpected token: %s", token.ToString())
	}
	return statement, nil
}
func (p *Parser) peek() (*Token, error) {
	return p.lexer.peekScan()
}
func (p *Parser) next() (*Token, error) {
	return p.lexer.next()
}

// 强制要求下一个 token 必须是标识符类型;
// 无论是否匹配, 都会消耗一个 token;
// 如果不是标识符, 返回 Err 中止解析;
// 解析表名时使用;
func (p *Parser) nextIdent() (string, error) {
	token, err := p.next()
	if token == nil || token.Type != IDENT {
		if err != nil {
			return "", err
		} else {
			return "", util.Error("Unexpected next token is ident, but is not")
		}
	}
	return string(token.Value), nil
}

// 如果下一个 Token 是关键字类型, 则取出; 否则不取出, 返回nil;
// 条件性关键字检查, 检查 数据库字段 的属性中是否 含有 NULL 关键字;
func (p *Parser) nextIfKeyWord() (*Token, error) {
	return p.nextIf(func(token *Token) bool {
		return token.Type == KEYWORD
	})
}

// nextExpect: 强制消费, 要求下一个 token 必须匹配预期值,否则报错;
func (p *Parser) nextExpect(expect *Token) error {
	token, err := p.next()
	if token == nil {
		if err != nil {
			return err
		} else {
			return util.Error("Expect %s, but got nil", expect.ToString())
		}
	}
	if !token.equal(expect) {
		return util.Error("Expect %s, but got %s", expect.ToString(), token.ToString())
	}
	return nil
}

// nextIfToken: 仅在匹配时,消费掉token; 不匹配时返回 nil,不报错,继续解析;
func (p *Parser) nextIfToken(token *Token) *Token {
	next, _ := p.next()
	if next != nil && !next.equal(token) {
		p.lexer.ReverseScan()
		return nil
	}
	return next
}

// nextIf: 如果满足条件, 则取出当前 Token, 否则不取出, 并返回 nil;
func (p *Parser) nextIf(fc func(*Token) bool) (*Token, error) {
	token, err := p.peek()
	if err != nil {
		return nil, err
	}
	if fc(token) {
		return p.lexer.next()
	}
	return nil, nil
}

func (p *Parser) parseStatement() (Statement, error) {
	if token, err := p.peek(); token == nil {
		if err != nil {
			return nil, err
		} else {
			return nil, util.Error("#parseStatement: Unexpected end of input")
		}
	} else {
		if token.Type == KEYWORD {
			switch token.Value {
			case Show:
				return p.parseShow()
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
			case Explain:
				return p.parseExplain()
			default:
				return nil, util.Error("#parseStatement: Unhandled default case: %s", token.ToString())
			}
		}
		return nil, util.Error("#parseStatement: Expected KEYWORD token, but get: %s", token.ToString())
	}
}
func (p *Parser) parseDdl() (Statement, error) {
	token, _ := p.next()
	if token == nil {
		return nil, util.Error("#parseDdl: Unexpected end of input")
	}
	value := token.Value
	if value == Create {
		if token2, _ := p.peek(); token2 == nil {
			return nil, util.Error("#parseDdl: table Execute is not null")
		} else {
			if token2.Value == Table {
				return p.parseDdlCreateTable()
			} else if token2.Value == Index {
				return p.parseDdlCreateIndex()
			} else {
				return nil, util.Error("#parseDdl: Unhandled default case: %s", token2.ToString())
			}
		}
	} else if value == Drop {
		return p.parseDdlDropTable()
	} else {
		return nil, util.Error("#parseDdl: unhandled parseDdl default case")
	}
}
func (p *Parser) parseDdlCreateTable() (Statement, error) {
	if token, _ := p.next(); token == nil {
		return nil, util.Error("#parseDdlCreateTable: table is not null")
	} else if token.Value != Table {
		return nil, util.Error("#parseDdlCreateTable: table is not null")
	}
	tableName, _ := p.nextIdent()
	err := p.nextExpect(&Token{Type: OPENPAREN, Value: OpenPar})
	if err != nil {
		return nil, err
	}
	var columns []*types.Column
	// CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);
	for {
		column, err := p.parseDdlColumn()
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
		if token := p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token == nil {
			break
		}
	}
	err = p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
	if err != nil {
		return nil, err
	}
	creatTableData := &CreatTableData{
		TableName: tableName,
		Columns:   columns,
	}
	return creatTableData, nil
}
func (p *Parser) parseDdlDropTable() (Statement, error) {
	err := p.nextExpect(&Token{Type: KEYWORD, Value: Table})
	if err != nil {
		return nil, err
	}
	tableName, _ := p.nextIdent()
	dropTableData := &DropTableData{
		TableName: tableName,
	}
	return dropTableData, nil
}
func (p *Parser) parseDdlCreateIndex() (Statement, error) {
	return nil, nil
}
func (p *Parser) parseExpression() (*types.Expression, error) {
	token, _ := p.next()
	var con types.Const
	switch token.Type {
	case IDENT:
		// 函数
		// count(col_name)
		if p.nextIfToken(&Token{Type: OPENPAREN, Value: OpenPar}) != nil {
			colName, _ := p.nextIdent()
			err := p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
			if err != nil {
				return nil, err
			}
			return &types.Expression{Function: &types.Function{FuncName: string(token.Value), ColName: colName}}, nil
		} else {
			return &types.Expression{Field: string(token.Value)}, nil
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
		expression, err := p.computeMathOperator(1)
		if err != nil {
			return nil, err
		}
		err = p.nextExpect(&Token{Type: CLOSEPAREN, Value: ClosePar})
		if err != nil {
			return nil, err
		}
		return expression, nil
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
			return nil, util.Error("#parseExpression: Unhandled default case: %s", token.ToString())
		}
	default:
		return nil, util.Error("#parseExpression: Unhandled default case: %s", token.ToString())
	}
	return types.NewExpression(con), nil
}

// parseDdlColumn 解析列定义;
func (p *Parser) parseDdlColumn() (*types.Column, error) {
	// CREATE TABLE user (id INT, name VARCHAR NOT NULL, age INT DEFAULT 0);
	filedName, err := p.nextIdent()
	if err != nil {
		return nil, err
	}
	dataTypeToken, _ := p.next()
	if dataTypeToken == nil || dataTypeToken.Type != KEYWORD {
		return nil, util.Error("#parseDdlColumn: Expect data type, but got err type or nil")
	}
	dataType, err := p.parserDataType(dataTypeToken)
	if err != nil {
		return nil, err
	}
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
		if token, _ := p.nextIfKeyWord(); token != nil {
			switch token.Value {
			case Null:
				column.Nullable = true
			case Not:
				err = p.nextExpect(&Token{Type: KEYWORD, Value: Null})
				if err != nil {
					return nil, err
				}
				column.Nullable = false
			case Default:
				expr, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				column.DefaultValue = expr
			case Primary:
				err = p.nextExpect(&Token{Type: KEYWORD, Value: Key})
				if err != nil {
					return nil, err
				}
				column.PrimaryKey = true
			case Index:
				column.IsIndex = true
			default:
				return nil, util.Error("#parseDdlColumn: Unexpected keyword: %s", token.ToString())
			}
		} else {
			break
		}
	}
	return column, nil
}
func (p *Parser) parserDataType(token *Token) (types.DataType, error) {
	// todo 根据输入字符串来定义数据类型;
	switch token.Value {
	case Int:
		return types.Integer, nil
	case Integer:
		return types.Integer, nil
	case String:
		return types.String, nil
	case Varchar:
		return types.String, nil
	case Text:
		return types.String, nil
	case Char:
		return types.String, nil
	case Bool:
		return types.Boolean, nil
	case Boolean:
		return types.Boolean, nil
	case Float:
		return types.Float, nil
	case Double:
		return types.Float, nil
	default:
		return -1, util.Error("#parserDataType: token.dataType[%s] is not support", token.ToString())
	}
}
func (p *Parser) parseInsert() (Statement, error) {
	var err error
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Insert})
	if err != nil {
		return nil, err
	}
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Into})
	if err != nil {
		return nil, err
	}
	// 表名
	tableName, _ := p.nextIdent()
	var columns []string
	// 查看是否给指定的列进行 insert
	if ifToken := p.nextIfToken(&Token{Type: OPENPAREN, Value: OpenPar}); ifToken != nil {
		var cols []string
		// insert into tbl (a, b, c) Values (1, 2, 3),(4, 5, 6);
		for {
			ident, _ := p.nextIdent()
			cols = append(cols, ident)
			if next, _ := p.next(); next.Type == COMMA {
				continue
			} else if next.Type == CLOSEPAREN {
				break
			} else {
				return nil, util.Error("Unexpected token: %s\n", next.ToString())
			}
		}
		columns = cols
	}
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Values})
	if err != nil {
		return nil, err
	}
	// insert into tbl (a, b, c) Values (1, 2, 3),(4, 5, 6);
	var values [][]*types.Expression
	for {
		err = p.nextExpect(&Token{Type: OPENPAREN, Value: OpenPar})
		if err != nil {
			return nil, err
		}
		var exprs []*types.Expression
		for {
			expression, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			exprs = append(exprs, expression)
			if next, _ := p.next(); next.Type == COMMA {
				continue
			} else if next.Type == CLOSEPAREN {
				break
			} else {
				return nil, util.Error("Unexpected token: %s\n", next.ToString())
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
	}, nil
}
func (p *Parser) parseSelect() (Statement, error) {
	selectData := &SelectData{}
	var err error
	selectData.SelectCols, err = p.parseSelectClause()
	if err != nil {
		return nil, err
	}
	selectData.From, err = p.parseFromClause()
	if err != nil {
		return nil, err
	}
	selectData.WhereClause, err = p.parseWhereClause()
	if err != nil {
		return nil, err
	}
	selectData.GroupBy, err = p.parseGroupByClause()
	if err != nil {
		return nil, err
	}
	selectData.Having, err = p.parseHavingClause()
	if err != nil {
		return nil, err
	}
	selectData.OrderBy, err = p.parseOrderByClause()
	if err != nil {
		return nil, err
	}
	selectData.Limit, err = p.parseLimitClause()
	if err != nil {
		return nil, err
	}
	selectData.Offset, err = p.parseOffsetClause()
	if err != nil {
		return nil, err
	}
	return selectData, nil
}

func (p *Parser) parseSelectClause() ([]*SelectCol, error) {
	var err error
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Select})
	if err != nil {
		return nil, err
	}
	SeqSelectCol := make([]*SelectCol, 0)
	if token := p.nextIfToken(&Token{Type: ASTERISK, Value: Asterisk}); token != nil {
		return SeqSelectCol, nil
	}
	for {
		expression, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		selectCol := &SelectCol{Expr: expression}
		if token := p.nextIfToken(&Token{Type: KEYWORD, Value: As}); token != nil {
			selectCol.Alis, _ = p.nextIdent()
		} else {
			selectCol.Alis = ""
		}
		SeqSelectCol = append(SeqSelectCol, selectCol)
		if token := p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token != nil {
			continue
		} else {
			break
		}
	}
	return SeqSelectCol, nil
}
func (p *Parser) parseFromClause() (FromItem, error) {
	// From 关键字
	err := p.nextExpect(&Token{Type: KEYWORD, Value: From})
	if err != nil {
		return nil, err
	}
	// 第一个表名
	item, err := p.parseFromTableClause()
	if err != nil {
		return nil, err
	}
	// 是否有 Join (嵌套进行);
	for {
		if joinType, err := p.parseFromJoinClause(); joinType != -1 {
			left := item
			right, err := p.parseFromTableClause()
			if err != nil {
				return nil, err
			}
			if right == nil {
				return nil, nil
			}
			var predicateOn *types.Expression
			if joinType == CrossType {
				predicateOn = nil
			} else {
				// 解析 on 后面表达式;
				if err = p.nextExpect(&Token{Type: KEYWORD, Value: On}); err != nil {
					return nil, err
				}
				onLeft, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				if err = p.nextExpect(&Token{Type: EQUAL, Value: Equal}); err != nil {
					return nil, err
				}
				onRight, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				// 右连接, 左右条件, 表达式进行交换;
				if joinType == RightType {
					onLeft, onRight = onRight, onLeft
				}
				com := &types.OperationEqual{
					Left:  onLeft,
					Right: onRight,
				}
				predicateOn = &types.Expression{OperationVal: com}
			}
			item = &JoinItem{
				Left:      left,
				Right:     right,
				JoinType:  joinType,
				Predicate: predicateOn,
			}
		} else {
			if err != nil {
				return nil, err
			} else {
				return item, nil
			}
		}
	}
}
func (p *Parser) parseFromTableClause() (FromItem, error) {
	tableName, err := p.nextIdent()
	if err != nil {
		return nil, err
	}
	return &TableItem{
		TableName: tableName,
	}, nil
}
func (p *Parser) parseFromJoinClause() (JoinType, error) {
	var err error
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Cross}); token != nil {
		err = p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		if err != nil {
			return -1, err
		}
		return CrossType, nil
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Join}); token != nil {
		return InnerType, nil
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Left}); token != nil {
		err = p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		if err != nil {
			return -1, err
		}
		return LeftType, nil
	} else if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Right}); token != nil {
		err = p.nextExpect(&Token{Type: KEYWORD, Value: Join})
		if err != nil {
			return -1, err
		}
		return RightType, nil
	}
	return -1, nil
}
func (p *Parser) parseGroupByClause() (*types.Expression, error) {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Group}); token != nil {
		err := p.nextExpect(&Token{Type: KEYWORD, Value: By})
		if err != nil {
			return nil, err
		}
		return p.parseExpression()
	}
	return nil, nil
}
func (p *Parser) parseHavingClause() (*types.Expression, error) {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Having}); token != nil {
		return p.parseOperationExpr()
	}
	return nil, nil

}
func (p *Parser) parseOrderByClause() ([]*OrderDirection, error) {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Order}); token == nil {
		return nil, nil
	}
	orders := make([]*OrderDirection, 0)
	err := p.nextExpect(&Token{Type: KEYWORD, Value: By})
	if err != nil {
		return nil, err
	}
	for {
		col, _ := p.nextIdent()
		token, _ := p.nextIf(func(token *Token) bool {
			return token.equal(&Token{Type: KEYWORD, Value: Asc}) || token.equal(&Token{Type: KEYWORD, Value: Desc})
		})
		orderDirection := &OrderDirection{colName: col}
		if token != nil {
			if token.equal(&Token{Type: KEYWORD, Value: Asc}) {
				orderDirection.direction = OrderAsc
			} else {
				orderDirection.direction = OrderDesc
			}
		} else {
			orderDirection.direction = OrderAsc
		}
		orders = append(orders, orderDirection)
		if token = p.nextIfToken(&Token{Type: COMMA, Value: Comma}); token != nil {
			continue
		} else {
			break
		}
	}
	return orders, nil

}
func (p *Parser) parseLimitClause() (*types.Expression, error) {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Limit}); token != nil {
		return p.parseExpression()
	}
	return nil, nil
}
func (p *Parser) parseOffsetClause() (*types.Expression, error) {
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Offset}); token != nil {
		return p.parseExpression()
	}
	return nil, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	var err error
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Delete})
	if err != nil {
		return nil, err
	}
	err = p.nextExpect(&Token{Type: KEYWORD, Value: From})
	if err != nil {
		return nil, err
	}
	tableName, _ := p.nextIdent()
	whereClause, _ := p.parseWhereClause()
	return &DeleteData{
		TableName:   tableName,
		WhereClause: whereClause,
	}, nil

}
func (p *Parser) parseUpdate() (Statement, error) {
	var err error
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Update})
	if err != nil {
		return nil, err
	}
	tableName, _ := p.nextIdent()
	err = p.nextExpect(&Token{Type: KEYWORD, Value: Set})
	if err != nil {
		return nil, err
	}
	columns := make(map[string]*types.Expression)
	for {
		// update user set name = 'tom', age = 18 where id = 1;
		// update user set age = age + 1  where id = 1;
		// update user set age = 12  where id = 1;
		// update user set age = 12+12  where id = 1;
		colName, _ := p.nextIdent()
		err = p.nextExpect(&Token{Type: EQUAL, Value: Equal})
		if err != nil {
			return nil, err
		}
		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, ok := columns[colName]; ok {
			return nil, util.Error("#parseUpdate: column[%s] is already exists", colName)
		}
		columns[colName] = value
		if p.nextIfToken(&Token{Type: COMMA, Value: Comma}) == nil {
			break
		}
	}
	whereClause, _ := p.parseWhereClause()
	return &UpdateData{
		TableName:   tableName,
		WhereClause: whereClause,
		Columns:     columns,
	}, nil
}
func (p *Parser) parseWhereClause() (*types.Expression, error) {
	// 检测后面是否有 where 关键字, 没有也可以;
	if token := p.nextIfToken(&Token{Type: KEYWORD, Value: Where}); token == nil {
		return nil, nil
	}
	return p.parseOperationExpr()
}

func (p *Parser) parseOperationExpr() (*types.Expression, error) {
	left, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if next, err := p.next(); next != nil {
		right, err := p.computeMathOperator(1)
		if err != nil {
			return nil, err
		}
		switch next.Type {
		case EQUAL:
			return &types.Expression{
				OperationVal: &types.OperationEqual{
					Left:  left,
					Right: right,
				},
			}, nil
		case GREATERTHAN:
			return &types.Expression{
				OperationVal: &types.OperationGreaterThan{
					Left:  left,
					Right: right,
				},
			}, nil
		case LESSTHAN:
			return &types.Expression{
				OperationVal: &types.OperationLessThan{
					Left:  left,
					Right: right,
				},
			}, nil
		default:
			return nil, util.Error("#parseOperationExpr unhandled default case %s", next.ToString())
		}
	} else {
		return nil, err
	}
}
func (p *Parser) parseTransaction() (Statement, error) {
	if next, _ := p.next(); next != nil {
		if next.Value == Begin {
			return &BeginData{}, nil
		} else if next.Value == Commit {
			return &CommitData{}, nil
		} else if next.Value == Rollback {
			return &RollbackData{}, nil
		} else {
			return nil, util.Error("#parseTransaction unhandled default case %s\n;", next.ToString())
		}
	}
	return nil, nil
}
func (p *Parser) parseExplain() (Statement, error) {
	err := p.nextExpect(&Token{Type: KEYWORD, Value: Explain})
	if err != nil {
		return nil, err
	}
	if next, _ := p.peek(); next != nil {
		if next.Value == Explain {
			return nil, util.Error("#parseExplain can not nest explain statement")
		}
	}
	// source Node
	statement, err := p.parseStatement()
	if err != nil {
		return nil, err
	}
	return &ExplainData{
		Statements: statement,
	}, nil
}

func (p *Parser) parseDdlCreateView() Statement {
	// todo 有待实现
	return nil
}

func (p *Parser) parseShow() (Statement, error) {
	err := p.nextExpect(&Token{Type: KEYWORD, Value: Show})
	if err != nil {
		return nil, err
	}
	if token, _ := p.next(); token != nil {
		if token.Value == Table {
			tableName, _ := p.nextIdent()
			return &ShowTableData{
				// 展示指定表信息
				TableName: tableName,
			}, nil
		} else if token.Value == DataBase {
			// 展示全部表;
			return &ShowTDataBaseData{}, nil
		} else {
			return nil, util.Error("#parseShow unhandled default case %s", token.ToString())
		}
	}
	return nil, nil
}
