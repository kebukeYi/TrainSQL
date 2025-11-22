package parser

import "github.com/kebukeYi/TrainSQL/sql/types"

// 计算数学表达式
// 5 + 2 + 1
// 5 + 2 * 1
func (p *Parser) computeMathOperator(minPrev int32) *types.Expression {
	left := p.parseExpression()
	for {
		// 当前 Token
		token := p.peek()
		if token == nil {
			break
		}
		// 当前 token 得是 运算符;
		if !token.isOperator() || token.precedence() < minPrev {
			break
		}
		nextPrecedence := token.precedence() + 1
		if next := p.next(); next == nil {
			return nil
		}
		// 递归计算右边的表达式
		right := p.computeMathOperator(nextPrecedence)
		// 计算 左右两方值
		left = token.computeExpr(left, right)
	}
	return left
}
