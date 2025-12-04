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
		token, err := lexer.next()
		if err != nil {
			fmt.Println(err)
			return
		}
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
	sql := "1234." // error input number: 1234.
	//sql := "1234.234" // ok 1234.234
	//sql := "0.234" // ok 0.234
	//sql := "00000.234" // ok
	//sql := "00000.00234" // ok
	//sql := "00000.0023400" // ok
	//sql := "00000.0023400S" // ok 00000.0023400  S
	//sql := "00000.0023400 23 S 'G'" // ok 00000.0023400  23 S G
	lexer := NewLexer(sql)
	for {
		token, err := lexer.next()
		if err != nil {
			fmt.Println(err)
			return
		}
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}

func TestScanString(t *testing.T) {
	//sql := "'" // err #next Scan error: #scanString Mismatch '
	//sql := "'S" // err #next Scan error: #scanString Mismatch '
	//sql := "S" // ok Token{Type:1, Value:S}
	sql := "'S'" // ok Token{Type:2, Value:S}
	//sql := "'S''" // err #next Scan error: #scanString Mismatch '
	//sql := "''" // err #next Scan error: not  ''
	//sql := "\"\"" //err #next Scan error: not  ''
	//sql := "' '" // ok Token{Type:2, Value: }
	//sql := "' 24 78 jdf 57 rw'" // ok Token{Type:2, Value: 24 78 jdf 57 rw}
	lexer := NewLexer(sql)
	for {
		token, err := lexer.next()
		if err != nil {
			fmt.Println(err)
			return
		}
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}

func TestScanIdent(t *testing.T) {
	//sql := "    o." // err #next Unexpected character: _
	sql := "    op  " //  ok op
	//sql := "    op _ 1 2 th 52 " // err #next Unexpected character: _
	//sql := "    op_12_th52 " // ok op_12_th52
	lexer := NewLexer(sql)
	for {
		token, err := lexer.next()
		if err != nil {
			fmt.Println(err)
			return
		}
		if token == nil {
			break
		}
		fmt.Println(token.ToString())
	}
}
