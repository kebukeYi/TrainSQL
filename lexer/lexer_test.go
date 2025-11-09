package lexer

import (
	"fmt"
	"testing"
)

func TestSelectLexer(t *testing.T) {
	sqlLexer := NewLexer("select name , sex from student where age > 20;")
	var tokens []Token
	tokens = append(tokens, NewTokenWithString(SELECT, "select"))
	tokens = append(tokens, NewTokenWithString(ID, "name"))
	tokens = append(tokens, NewTokenWithString(COMMA, ","))
	tokens = append(tokens, NewTokenWithString(ID, "sex"))
	tokens = append(tokens, NewTokenWithString(FROM, "from"))
	tokens = append(tokens, NewTokenWithString(ID, "student"))
	tokens = append(tokens, NewTokenWithString(WHERE, "where"))
	tokens = append(tokens, NewTokenWithString(ID, "age"))
	tokens = append(tokens, NewTokenWithString(GREATER_OPERATOR, ">"))
	tokens = append(tokens, NewTokenWithString(NUM, "20"))
	tokens = append(tokens, NewTokenWithString(SEMICOLON, ";"))
	for _, tok := range tokens {
		sqlTok, err := sqlLexer.Scan()
		if err != nil {
			fmt.Println("lexer error")
			break
		}
		if sqlTok.Tag != tok.Tag {
			errText := fmt.Sprintf("token err, expect: %v, but got %v\n", tok, sqlTok)
			fmt.Println(errText)
			break
		}
	}
	fmt.Println("lexer testing pass.")
}
