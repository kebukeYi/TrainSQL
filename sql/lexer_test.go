package sql

import (
	"fmt"
	"testing"
)

func TestEraseWithSpace(t *testing.T) {
	// sql := "     select    into     from   user  ; " // ok
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
	//sql := "     select    *     from   user  ; " // ok
	//sql := "" // nil
	//sql := "S" // ok S
	//sql := "   1" // ok 1
	//sql := "   1   "  // ok 1
	//sql := "1" // ok 1
	//sql := "12.12.1.1." // err 12.12; Unexpected character: .
	//sql := "1234." // error input number: 1234.
	//sql := "1234.234" // ok 1234.234
	//sql := "0.234" // ok 0.234
	//sql := "00000.234" // ok
	//sql := "00000.00234" // ok
	//sql := "00000.0023400" // ok
	sql := "00000.0023400S" // ok 00000.0023400  S
	//sql := "00000.0023400 23 S 'G'" // ok 00000.0023400  23 S G
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
	//sql := "'" // 报错 Mismatch '
	//sql := "'S" // 报错 Mismatch '
	//sql := "S"  // S
	//sql := "'S'" // S
	//sql := "'S''" // 报错 Mismatch '
	//sql := "''" // 报错
	//sql := "\"\"" // 报错
	//sql := "' '" //
	sql := "' 24 78 jdf 57 rw'" //  24 78 jdf 57 rw
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
	//sql := "    o." // o ; Unexpected character: .;
	//sql := "    op  " // op
	//sql := "    op _ 1 2 th 52 " // op; panic: Unexpected character: _
	sql := "    op_12_th52 " // op_12_th52
	lexer := NewLexer(sql)
	for {
		token := lexer.next()
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}
