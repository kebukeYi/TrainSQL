package sql

import (
	"bufio"
	"bytes"
	"errors"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"io"
	"strings"
	"unicode"
)

type Lexer struct {
	reader     *bufio.Reader
	keyWords   map[string]*Token // 关键词字典
	readOffset int
	tokenStack []*Token
}

func NewLexer(sql string) *Lexer {
	return &Lexer{
		reader:     bufio.NewReader(strings.NewReader(sql)),
		keyWords:   InitWord(),
		tokenStack: []*Token{},
		readOffset: 0,
	}
}
func (le *Lexer) peek(n int) ([]byte, error) {
	if readCh, err := le.reader.Peek(n); err != nil {
		// 如果是读到末尾了, 那么就直接正常结束;
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	} else {
		return readCh, nil
	}
}
func (le *Lexer) readCh() ([]byte, error) {
	readByte, err := le.reader.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	return []byte{readByte}, nil
}
func (le *Lexer) unReadCh() error {
	err := le.reader.UnreadByte()
	if err != nil {
		return err
	}
	return nil
}

// 将 sql 中的空格字符,一一取出,随后丢弃掉;
func (le *Lexer) eraseWithSpace() error {
	if _, err := le.nextWhile(func(r byte) bool {
		return r == ' ' || r == '\t' || r == '\n'
	}); err != nil {
		return err
	}
	return nil
}

// 如果当前字符满足fc(), 则消费并返回当前字符;
func (le *Lexer) nextIf(fc func(r byte) bool) ([]byte, error) {
	if readCh, err := le.peek(1); err != nil {
		// 未知错误
		return nil, err
	} else if readCh != nil {
		if fc(readCh[0]) {
			return le.readCh()
		} else {
			return nil, util.Mismatch
		}
	}
	return nil, nil
}

// 判断当前字符是否满足fc(), 如果满足则消费保存, 并读取下一个,判断下一个字符是否满足;不满足则返回;
func (le *Lexer) nextWhile(fc func(r byte) bool) ([]byte, error) {
	var result []byte
	for {
		if nextIf, err := le.nextIf(fc); nextIf != nil {
			result = append(result, nextIf[0])
		} else {
			// 匹配失败, 跳出循环;
			if errors.Is(err, util.Mismatch) {
				break
			}
			// 错误(未知错误,则抛出)
			return nil, err
		}
	}
	// 如果是空格字节数组,不保存, 返回nil;
	if len(bytes.TrimSpace(result)) == 0 {
		return nil, nil
	}
	return result, nil
}
func (le *Lexer) nextIfToken(fc func(r byte) *Token) (*Token, error) {
	peek, err := le.peek(1)
	if peek != nil {
		token := fc(peek[0])
		if token != nil {
			le.readCh()
			return token, nil
		}
	} else {
		return nil, err
	}
	return nil, nil
}
func (le *Lexer) next() (*Token, error) {
	token, err := le.Scan()
	if err != nil {
		return nil, util.Error("#next Scan error: %s\n", err)
	}
	if token == nil {
		// 查看是否是 非法字符;
		if peek, _ := le.peek(1); peek != nil {
			return nil, util.Error("#next Unexpected character: %s\n", peek)
		}
		return nil, nil
	}
	return token, nil
}
func (le *Lexer) Scan() (*Token, error) {
	var token *Token
	var err error
	var peek []byte
	// 进入这个分支说明之前读过一个token, 但是又回退了;
	// 因此下次读取时, 直接从栈中读取即可; 不用再解析;
	if le.readOffset < len(le.tokenStack) {
		token = le.tokenStack[le.readOffset]
		le.readOffset += 1
		return token, nil
	} else {
		le.readOffset += 1
	}
	if err = le.eraseWithSpace(); err != nil {
		return nil, err
	}
	if peek, err = le.peek(1); peek == nil {
		return nil, err
	} else {
		if unicode.IsDigit(rune(peek[0])) {
			token, err = le.scanNumber()
		} else if unicode.IsLetter(rune(peek[0])) {
			token, err = le.scanIdent()
		} else if peek[0] == '"' || peek[0] == '\'' {
			token, err = le.scanString()
		} else {
			token, err = le.scanSymbol()
		}
	}
	if err != nil {
		return nil, err
	}
	le.tokenStack = append(le.tokenStack, token)
	return token, nil
}
func (le *Lexer) peekScan() (*Token, error) {
	token, err := le.next()
	if err != nil {
		return nil, err
	}
	le.ReverseScan()
	return token, nil
}
func (le *Lexer) ReverseScan() {
	if le.readOffset > 0 {
		le.readOffset -= 1
	}
}
func (le *Lexer) scanString() (*Token, error) {
	isOver := true
	nextIf, err := le.nextIf(func(r byte) bool {
		return r == '"' || r == '\''
	})
	if err != nil {
		return nil, err
	}
	// 如果下一个字符 不是 字符串的起始 标记, 那么就返回;
	if nextIf == nil {
		return nil, nil
	}
	isOver = !isOver
	var result []byte
	var ch []byte
	for {
		if ch, _ = le.readCh(); ch == nil {
			if !isOver {
				return nil, util.Error("#scanString Mismatch ' ")
			}
			break
		}
		if ch[0] == '"' || ch[0] == '\'' {
			isOver = !isOver
			break
		} else {
			result = append(result, ch...)
		}
	}
	if result == nil || len(result) == 0 || string(result) == "" || string(result) == "''" {
		return nil, util.Error("not  ''  ")
	}
	return &Token{Type: STRING, Value: TokenValue(result)}, nil
}
func (le *Lexer) scanNumber() (*Token, error) {
	// 数字可能包含小数点,或者只有一个数字,或者只是一个整数;
	// 1.先扫描出前面一部分数字;
	num, _ := le.nextWhile(func(r byte) bool {
		return unicode.IsDigit(rune(r))
	})
	// 2.如果后面跟着小数点,则继续扫描小数点后面的数字;
	if nextIf, _ := le.nextIf(func(r byte) bool {
		return r == '.'
	}); nextIf != nil {
		num = append(num, nextIf...)
		num2, _ := le.nextWhile(func(r byte) bool {
			return unicode.IsDigit(rune(r))
		})
		if num2 == nil || len(num2) == 0 || string(num2) == "" {
			return nil, util.Error("#scanNumber error input number: %s\n", string(num))
		}
		num = append(num, num2...)
	}
	// 后面不是小数点:
	// 1. 说明只读一个整数,返回即可;
	// 2. 说明读到EOF末尾, 返回即可;
	return &Token{Type: NUMBER, Value: TokenValue(num)}, nil
}
func (le *Lexer) scanIdent() (*Token, error) {
	token := &Token{}
	var err error
	var idnet []byte
	var idnet2 []byte
	idnet, err = le.nextIf(func(r byte) bool {
		return unicode.IsLetter(rune(r))
	})
	if err != nil {
		return nil, err
	}
	idnet2, err = le.nextWhile(func(r byte) bool {
		return isAlphanumeric(rune(r)) || r == '_'
	})
	if err != nil {
		return nil, err
	}
	idnet = append(idnet, idnet2...)
	value := strings.ToUpper(string(idnet))
	// 如果是关键字, 则转为大写; 否则原样返回;
	if tokenValue, ok := le.keyWords[value]; ok {
		token.Type = tokenValue.Type
		token.Value = tokenValue.Value
	} else {
		token.Value = TokenValue(idnet)
		token.Type = IDENT
	}
	return token, nil
}
func (le *Lexer) scanSymbol() (*Token, error) {
	fc := func(c byte) *Token {
		token := &Token{}
		switch c {
		case '*':
			token.Type = ASTERISK
			token.Value = Asterisk
		case '(':
			token.Type = OPENPAREN
			token.Value = OpenPar
		case ')':
			token.Type = CLOSEPAREN
			token.Value = ClosePar
		case ',':
			token.Type = COMMA
			token.Value = Comma
		case ';':
			token.Type = SEMICOLON
			token.Value = Semicolon
		case '+':
			token.Type = PLUS
			token.Value = Plus
		case '-':
			token.Type = MINUS
			token.Value = Minus
		case '/':
			token.Type = SLASH
			token.Value = Slash
		case '=':
			token.Type = EQUAL
			token.Value = Equal
		case '>':
			token.Type = GREATERTHAN
			token.Value = GreaterThan
		case '<':
			token.Type = LESSTHAN
			token.Value = LessThan
		default:
			return nil
		}
		return token
	}
	token, err := le.nextIfToken(fc)
	return token, err
}

// 检查单个字符是否为字母或数字;
func isAlphanumeric(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}
