package parser

import (
	"fmt"
	"github.com/kebukeYi/TrainSQL/lexer"
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/record_manager"
	"strconv"
	"strings"
)

type SQLParser struct {
	sqlLexer lexer.Lexer // 词法解析器;
}

func NewSQLParser(s string) *SQLParser {
	return &SQLParser{
		sqlLexer: lexer.NewLexer(s),
	}
}

func (p *SQLParser) ParseCommand() interface{} {
	// 先扫描出第一个词, 判断是什么关键字;
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag == lexer.INSERT {
		p.sqlLexer.ReverseScan() // 重置读取位移;
		return p.Insert()
	} else if tok.Tag == lexer.DELETE {
		p.sqlLexer.ReverseScan()
		return p.Delete()
	} else if tok.Tag == lexer.UPDATE {
		p.sqlLexer.ReverseScan()
		return p.Modify()
	} else {
		p.sqlLexer.ReverseScan()
		return p.Create()
	}
}

func (p *SQLParser) checkWordTag(wordTag lexer.Tag) {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != wordTag {
		panic("token is not match")
	}
}

func (p *SQLParser) checkWordToken(token lexer.Token) {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != token.Tag {
		panic("token is not match")
	}
	if tok.ToString() != token.ToString() {
		fmt.Printf("token is not match, expect: %s, but get: %s \n", token.ToString(), tok.ToString())
		panic(err)
	}
}

func (p *SQLParser) isMatchTag(wordTag lexer.Tag) bool {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag == wordTag {
		return true
	} else {
		p.sqlLexer.ReverseScan()
		return false
	}
}

func (p *SQLParser) Create() interface{} {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.CREATE {
		panic("token is not create")
	}

	tok, err = p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.TABLE {
		return p.CreateTable()
	} else if tok.Tag == lexer.VIEW {
		return p.CreateView()
	} else if tok.Tag == lexer.INDEX {
		return p.CreateIndex()
	}

	panic("sql string with create should not end here")
}

/*
CreateView
-- 创建可更新的用户地址视图（仅包含用户ID和地址，支持通过视图更新地址）
CREATE VIEW v_user_address AS
SELECT

	user_id,
	province,
	city,
	detail_address

FROM user_addresses WHERE is_deleted = 0; -- 过滤已删除用户
WITH CHECK OPTION; -- 确保通过视图插入/更新的数据符合原表约束（如非空、格式等）
*/
func (p *SQLParser) CreateView() interface{} {
	p.checkWordTag(lexer.ID)
	viewName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.AS)
	qd := p.Select()

	vd := NewViewData(viewName, qd)
	p.checkWordTag(lexer.SEMICOLON)
	fmt.Sprintf("vd def: %s \n", vd.ToString())
	return vd
}

func (p *SQLParser) CreateIndex() interface{} {
	// CREATE INDEX indexName ON tableName(email);
	p.checkWordTag(lexer.ID) // indexName
	idexName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.ON)
	p.checkWordTag(lexer.ID)
	tableName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.LEFT_BRACKET)
	_, fldName := p.Field()
	p.checkWordTag(lexer.RIGHT_BRACKET)

	idxData := NewIndexData(idexName, tableName, fldName)
	// fmt.Printf("create index result: %s", idxData.ToString())
	p.checkWordTag(lexer.SEMICOLON)
	return idxData
}

/*
CreateTable  样例

	 CREATE TABLE products (
		-- 主键：自增ID，唯一标识产品
		product_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '产品唯一ID',
		-- 产品名称：非空，最长100字符，不重复
		product_name VARCHAR(100) NOT NULL UNIQUE COMMENT '产品名称',
		-- 分类ID：关联分类表（外键示例），允许为NULL（未分类）
		category_id INT NULL COMMENT '所属分类ID',
		-- 价格：decimal类型（精确小数），保留2位小数，大于0
		price DECIMAL(10, 2) NOT NULL CHECK (price > 0) COMMENT '产品售价',
		-- 库存：非负整数，默认值为0
		stock INT NOT NULL DEFAULT 0 CHECK (stock >= 0) COMMENT '库存数量')
*/
func (p *SQLParser) CreateTable() interface{} {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	// 是否是 属性值 类型;
	if tok.Tag != lexer.ID {
		panic("token should be ID(string) for table name")
	}

	tblName := p.sqlLexer.Lexeme
	tok, err = p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.LEFT_BRACKET {
		panic("missing left bracket")
	}
	// 获取字段定义;
	sch := p.FieldDefs()
	tok, err = p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	// ) 结尾;
	if tok.Tag != lexer.RIGHT_BRACKET {
		panic("missing right bracket")
	}
	p.checkWordTag(lexer.SEMICOLON)
	return NewCreateTableData(tblName, sch)
}

func (p *SQLParser) FieldDefs() *record_manager.Schema {
	schema := p.FieldDef()
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag == lexer.COMMA { // ,
		schema2 := p.FieldDefs()
		schema.AddAll(schema2)
	} else {
		p.sqlLexer.ReverseScan()
	}
	return schema
}

func (p *SQLParser) FieldDef() *record_manager.Schema {
	_, fldName := p.Field()
	return p.FieldType(fldName)
}

func (p *SQLParser) FieldType(fldName string) *record_manager.Schema {
	schema := record_manager.NewSchema()
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.INT {
		schema.AddIntField(fldName)
	} else if tok.Tag == lexer.VARCHAR {
		tok, err := p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.LEFT_BRACKET {
			panic("missing left bracket")
		}

		tok, err = p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}

		if tok.Tag != lexer.NUM {
			panic("it is not a number for varchar")
		}

		num := p.sqlLexer.Lexeme
		fldLen, err := strconv.Atoi(num)
		if err != nil {
			panic(err)
		}
		schema.AddStringField(fldName, fldLen)

		tok, err = p.sqlLexer.Scan()
		if err != nil {
			panic(err)
		}
		if tok.Tag != lexer.RIGHT_BRACKET {
			panic("missing right bracket")
		}
	} else {
		panic("unknown data type;")
	}

	return schema
}

func (p *SQLParser) fieldList() []string {
	L := make([]string, 0)
	_, field := p.Field()
	L = append(L, field)
	if p.isMatchTag(lexer.COMMA) {
		fields := p.fieldList()
		L = append(L, fields...)
	}
	return L
}

func (p *SQLParser) constList() []*query.Constant {
	L := make([]*query.Constant, 0)
	L = append(L, p.Constant())
	if p.isMatchTag(lexer.COMMA) {
		consts := p.constList()
		L = append(L, consts...)
	}
	return L
}

/*
根据语法规则:
INSERT INTO ID(tableName) (LEFT_PARAS FieldList RIGHT_PARAS) VALUES (LEFT_PARS ConstList RIGHT_PARAS)
我们首先要匹配四个关键字, 分别为 insert, into, id, 左括号,
然后就是一系列由逗号隔开的field,
接着就是右括号，然后是关键字values
接着是常量序列，最后以右括号结尾
*/
func (p *SQLParser) Insert() interface{} {
	p.checkWordTag(lexer.INSERT)
	p.checkWordTag(lexer.INTO)
	p.checkWordTag(lexer.ID)
	tblName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.LEFT_BRACKET)
	flds := p.fieldList() // 字段列表
	p.checkWordTag(lexer.RIGHT_BRACKET)
	p.checkWordTag(lexer.VALUES)
	p.checkWordTag(lexer.LEFT_BRACKET)
	vals := p.constList() // 真实值列表
	p.checkWordTag(lexer.RIGHT_BRACKET)
	p.checkWordTag(lexer.SEMICOLON)
	return NewInsertData(tblName, flds, vals)
}

/*
	    第一个关键字 delete,第二个关键字必须 from
		delete from tableName where a = 1 predicate
*/
func (p *SQLParser) Delete() interface{} {
	p.checkWordTag(lexer.DELETE)
	p.checkWordTag(lexer.FROM)
	p.checkWordTag(lexer.ID)
	tblName := p.sqlLexer.Lexeme
	pred := query.NewPredicate()
	if p.isMatchTag(lexer.WHERE) {
		pred = p.Predicate()
	}
	p.checkWordTag(lexer.SEMICOLON)
	return NewDeleteData(tblName, pred)
}

// Modify update tableName set fieldName = newVal [where predicate]
func (p *SQLParser) Modify() interface{} {
	p.checkWordTag(lexer.UPDATE)
	p.checkWordTag(lexer.ID)
	// 获得表名
	tblName := p.sqlLexer.Lexeme
	p.checkWordTag(lexer.SET)
	_, fldName := p.Field()
	p.checkWordTag(lexer.ASSIGN_OPERATOR) // =
	newVal := p.Expression()              // 解析表达式
	pred := query.NewPredicate()
	if p.isMatchTag(lexer.WHERE) {
		pred = p.Predicate()
	}
	p.checkWordTag(lexer.SEMICOLON)
	return NewModifyData(tblName, fldName, newVal, pred)
}

func (p *SQLParser) Field() (lexer.Token, string) {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.ID {
		panic("Tag of FIELD is no ID")
	}
	return tok, p.sqlLexer.Lexeme
}

func (p *SQLParser) Constant() *query.Constant {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	switch tok.Tag {
	case lexer.STRING:
		s := strings.Clone(p.sqlLexer.Lexeme)
		return query.NewConstantWithString(&s)
		break
	case lexer.NUM:
		v, err := strconv.Atoi(p.sqlLexer.Lexeme)
		if err != nil {
			panic("string is not a number")
		}
		return query.NewConstantWithInt(&v)
		break
	default:
		panic("token is not string or num when parsing constant")
	}
	return nil
}

func (p *SQLParser) Expression() *query.Expression {
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	// id类型
	if tok.Tag == lexer.ID {
		// 回退
		p.sqlLexer.ReverseScan()
		_, str := p.Field()
		return query.NewExpressionWithString(str)
	} else {
		// 值 类型;
		p.sqlLexer.ReverseScan()
		constant := p.Constant()
		return query.NewExpressionWithConstant(constant)
	}
}

func (p *SQLParser) Term() *query.Term {
	lhs := p.Expression()
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	// = 号
	if tok.Tag != lexer.ASSIGN_OPERATOR {
		panic("should have = in middle of term")
	}
	rhs := p.Expression()
	return query.NewTerm(lhs, rhs)
}

func (p *SQLParser) Predicate() *query.Predicate {
	// predicate 对应 where 语句后面的判断部分, 例如 where a > b and c < b
	// 这里的a > b and c < b 就是 predicate
	term := p.Term()
	pred := query.NewPredicateWithTerms(term)
	tok, err := p.sqlLexer.Scan()
	// 如果语句已经读取完则直接返回;
	if err != nil && tok.Tag != lexer.EOF {
		panic(err)
	}
	if tok.Tag == lexer.AND {
		pred.ConjoinWith(p.Predicate())
	} else {
		p.sqlLexer.ReverseScan()
	}
	return pred
}

func (p *SQLParser) Select() *QueryData {
	// query 解析 select 语句,首个 token;
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.SELECT {
		panic("token is not select")
	}

	// 获得 select 列表;
	fields := p.SelectList()

	// 获得 from 关键字;
	tok, err = p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag != lexer.FROM {
		panic("token is not from")
	}

	// 获取 select 语句作用的表名;
	tables := p.TableList()

	// 判断 select 语句是否有where子句; 注意:有可能没有 where 关键字;
	tok, err = p.sqlLexer.Scan()
	if err != nil {
		if tok.Tag == lexer.ERROR {
			panic(err)
		}
	}

	pred := query.NewPredicate()
	if tok.Tag == lexer.WHERE {
		pred = p.Predicate()
	} else {
		p.sqlLexer.ReverseScan()
	}
	p.checkWordTag(lexer.SEMICOLON)
	return NewQueryData(fields, tables, pred)
}

func (p *SQLParser) SelectList() []string {
	// SELECT_LIST 对应 select 关键字后面的列名称
	l := make([]string, 0)
	_, field := p.Field()
	l = append(l, field)

	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}

	if tok.Tag == lexer.COMMA { // ,
		// select 多个列，每个列由逗号隔开
		selectList := p.SelectList()
		l = append(l, selectList...)
	} else {
		p.sqlLexer.ReverseScan()
	}

	return l
}

func (p *SQLParser) TableList() []string {
	// TBALE_LSIT 对应from后面的表名
	l := make([]string, 0)
	tok, err := p.sqlLexer.Scan()
	if err != nil {
		panic(err)
	}
	if tok.Tag != lexer.ID {
		panic("token is not id")
	}

	l = append(l, p.sqlLexer.Lexeme)
	tok, err = p.sqlLexer.Scan()
	if err != nil {
		if tok.Tag == lexer.EOF {
			return l
		}
		panic(err)
	}
	if tok.Tag == lexer.COMMA { // ,
		tableList := p.TableList()
		l = append(l, tableList...)
	} else {
		p.sqlLexer.ReverseScan()
	}
	return l
}
