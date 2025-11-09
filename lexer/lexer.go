package lexer

import (
	"bufio"
	"io"
	"strconv"
	"strings"
	"unicode"
)

type Lexer struct {
	Lexeme       string   // 当前词
	lexemeStack  []string // 所有词栈, 包含符号
	tokenStack   []Token  // 所有关键词栈
	peek         byte     // 当前字符
	Line         uint32   // 第几行
	reader       *bufio.Reader
	read_pointer int              // 当前读取lexemeStack 的词栈位移
	key_words    map[string]Token // 关键词字典
}

func NewLexer(source string) Lexer {
	str := strings.NewReader(source)
	sourceReader := bufio.NewReaderSize(str, len(source))
	lexer := Lexer{
		Line:      uint32(1),
		reader:    sourceReader,
		key_words: make(map[string]Token),
	}
	// 生成 token, < wordString, tag > 字典表;
	lexer.initToken()
	return lexer
}
func (l *Lexer) initToken() {
	key_words := GetKeyWords()
	for _, key_word := range key_words {
		l.key_words[key_word.ToString()] = key_word.Tag
	}
}

func (l *Lexer) ReverseScan() {
	/*
		back_len := len(l.Lexeme)
		只能un read 一个字节
		for i := 0; i < back_len; i++ {
			l.reader.UnreadByte()
		}
	*/
	// 回退一词;
	if l.read_pointer > 0 {
		l.read_pointer = l.read_pointer - 1
	}
}

func (l *Lexer) Reach() error {
	char, err := l.reader.ReadByte() // 读取下一个字节
	l.peek = char
	return err
}

// ReadCharacter 判断下一个字符是否为指定字符;
func (l *Lexer) ReadCharacter(c byte) (bool, error) {
	chars, err := l.reader.Peek(1)
	if err != nil {
		return false, err
	}

	peekChar := chars[0]
	if peekChar != c {
		return false, nil
	}

	l.Reach() // 越过当前peek的字符
	return true, nil
}

func (l *Lexer) UnRead() error {
	// 回退一字节;
	return l.reader.UnreadByte()
}

// Scan  读取一个 词;
func (l *Lexer) Scan() (Token, error) {
	// 从预存的词法分析结果中顺序读取下一个token;
	if l.read_pointer < len(l.lexemeStack) {
		l.Lexeme = l.lexemeStack[l.read_pointer]
		token := l.tokenStack[l.read_pointer]
		l.read_pointer = l.read_pointer + 1
		return token, nil
	} else {
		// 继续往后读取;
		l.read_pointer = l.read_pointer + 1
	}
	// 是否已经遇到过引号字符 "
	haveSeenQuote := false
	for {
		err := l.Reach()
		if err != nil {
			if err == io.EOF {
				// 正常读取结束;
				return NewToken(EOF), nil
			}
			return NewToken(ERROR), err
		}
		// 空格或者回车,继续当前行读取;
		if l.peek == ' ' || l.peek == '\t' {
			continue
		} else if l.peek == '\n' {
			l.Line = l.Line + 1 // 换行,继续循环读取;
		} else {
			// 有效字符,跳出循环,到词法分析;
			break
		}
	}

	l.Lexeme = ""

	switch l.peek {
	case ',':
		l.Lexeme = ","
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(COMMA)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '{':
		l.Lexeme = "{"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(LEFT_BRACE)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '}':
		l.Lexeme = "}"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(RIGHT_BRACE)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '+':
		l.Lexeme = "+"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(PLUS)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '-':
		l.Lexeme = "-"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(MINUS)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '(':
		l.Lexeme = "("
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(LEFT_BRACKET)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case ')':
		l.Lexeme = ")"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(RIGHT_BRACKET)
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case ';': // 分号
		l.Lexeme = ";"
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(SEMICOLON)
		//token := NewTokenWithString(SEMICOLON, ";")
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	case '&':
		l.Lexeme = "&"
		// 如果是 &&
		if ok, err := l.ReadCharacter('&'); ok {
			l.Lexeme = "&&"
			word := NewWordToken("&&", AND)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			// 否则是 &
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(AND_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}
	case '|':
		l.Lexeme = "|"
		if ok, err := l.ReadCharacter('|'); ok {
			l.Lexeme = "||"
			word := NewWordToken("||", OR)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(OR_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}

	case '=':
		l.Lexeme = "="
		if ok, err := l.ReadCharacter('='); ok {
			l.Lexeme = "=="
			word := NewWordToken("==", EQ)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(ASSIGN_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}

	case '!':
		l.Lexeme = "!"
		if ok, err := l.ReadCharacter('='); ok {
			l.Lexeme = "!="
			word := NewWordToken("!=", NE)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(NEGATE_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}

	case '<':
		l.Lexeme = "<"
		if ok, err := l.ReadCharacter('='); ok {
			l.Lexeme = "<="
			word := NewWordToken("<=", LE)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(LESS_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}

	case '>':
		l.Lexeme = ">"
		if ok, err := l.ReadCharacter('='); ok {
			l.Lexeme = ">="
			word := NewWordToken(">=", GE)
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, word.Tag)
			return word.Tag, err
		} else {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(GREATER_OPERATOR)
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}

	case '"':
		haveSeenQuote = true
		for {
			// 需要在当前连续读; 例如: "wangwu" 值;
			err := l.Reach()
			// 到最后一个引号了;
			if l.peek == '"' {
				haveSeenQuote = false
				l.lexemeStack = append(l.lexemeStack, l.Lexeme)
				token := NewToken(STRING)
				l.tokenStack = append(l.tokenStack, token)
				return token, nil
			}
			if err != nil {
				panic("string no end with quota")
			}
			l.Lexeme += string(l.peek)
		}
	}

	numberFunc := func() (Token, error) {
		var v int
		var err error
		for {
			num, err := strconv.Atoi(string(l.peek))
			if err != nil { // 转换错误;
				// != 不等于nil, 说明当前字符不是数字, 是有效字符;
				if l.peek != 0 { // l.peek == 0 意味着已经读完所有字符;
					l.UnRead() // 将字符放回以便下次扫描;
				}
				break
			}
			// 读一位, 乘10倍;
			v = 10*v + num
			l.Lexeme += string(l.peek)
			l.Reach()
		}

		if l.peek != '.' {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			token := NewToken(NUM)
			token.lexeme = l.Lexeme
			l.tokenStack = append(l.tokenStack, token)
			return token, err
		}
		l.Lexeme += string(l.peek)
		l.Reach() // 越过 "."

		x := float64(v)
		d := float64(10)
		for {
			l.Reach()
			num, err := strconv.Atoi(string(l.peek))
			if err != nil {
				if l.peek != 0 { //l.peek == 0 意味着已经读完所有字符
					l.UnRead() //将字符放回以便下次扫描
				}
				break
			}

			x = x + float64(num)/d
			d = d * 10
			l.Lexeme += string(l.peek)
		}

		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token := NewToken(REAL)
		token.lexeme = l.Lexeme
		l.tokenStack = append(l.tokenStack, token)
		return token, err
	}

	// 当前字符是 数字类型:
	if unicode.IsNumber(rune(l.peek)) {
		return numberFunc()
	}

	// 判断一个字符是不是字母, 随后再读取 整个字符串;
	if unicode.IsLetter(rune(l.peek)) {
		var buffer []byte
		for {
			buffer = append(buffer, l.peek)
			l.Lexeme += string(l.peek)

			l.Reach()
			if !unicode.IsLetter(rune(l.peek)) {
				if l.peek != 0 { //l.peek == 0 意味着已经读完所有字符
					l.UnRead() //将字符放回以便下次扫描
				}
				break
			}
		}

		s := string(buffer)
		token, ok := l.key_words[strings.ToUpper(s)]
		// 如果是关键字, 返回对应的Token;
		if ok {
			l.lexemeStack = append(l.lexemeStack, l.Lexeme)
			l.tokenStack = append(l.tokenStack, token)
			return token, nil
		}
		// 否则返回一个ID(string)类型, 字段名;
		l.lexemeStack = append(l.lexemeStack, l.Lexeme)
		token = NewToken(ID)
		token.lexeme = l.Lexeme
		l.tokenStack = append(l.tokenStack, token)
		return token, nil
	}

	if haveSeenQuote {
		panic("string without end quota")
	}

	return NewToken(EOF), nil
}
