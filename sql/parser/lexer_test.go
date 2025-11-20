package parser

import (
	"fmt"
	"testing"
)

func TestEraseWithSpace(t *testing.T) {
	// sql := "     select    into     from   user  ; "
	// sql := ""
	sql := "S"
	lexer := NewLexer(sql)
	for {
		token := lexer.next()
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}

func TestScanNumber(t *testing.T) {
	//sql := "     select    into     from   user  ; "
	//sql := ""
	//sql := "S"
	//sql := "   1"
	//sql := "   1   "
	//sql := "1"
	//sql := "12.12.1.1."
	//sql := "1234."
	//sql := "1234.234"
	//sql := "0.234"
	//sql := "00000.234"
	//sql := "00000.00234"
	//sql := "00000.0023400"
	//sql := "00000.0023400S"
	sql := "00000.0023400 23 S 'G'"
	lexer := NewLexer(sql)
	for {
		token := lexer.next()
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}

func TestScanString(t *testing.T) {
	//sql := "'"
	//sql := "'S"
	//sql := "S"
	sql := "'S'"
	//sql := "'S''"
	//sql := "''"
	//sql := "' '"
	//sql := "' 24 78 jdf 57 rw'"
	lexer := NewLexer(sql)
	for {
		token := lexer.next()
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}

func TestScanIdent(t *testing.T) {
	sql := "    o." // o
	//sql := "    op  " // op
	//sql := "    op _ 1 2 th 52 " // op
	//sql := "    op_12_th52 " // op_12_th52
	lexer := NewLexer(sql)
	for {
		token := lexer.next()
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}
