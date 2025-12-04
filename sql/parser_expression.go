package sql

import "github.com/kebukeYi/TrainSQL/sql/types"

// 计算数学表达式
// 5 + 2 + 1
// 5 + 2 * 1
func (p *Parser) computeMathOperator(minPrev int32) (*types.Expression, error) {
	var left *types.Expression
	var right *types.Expression
	var next *Token
	var err error
	left, err = p.parseExpression()
	if err != nil {
		return nil, err
	}
	for {
		// 当前 Token
		token, _ := p.peek()
		if token == nil {
			break
		}
		// 当前 token 得是 运算符;
		if !token.isOperator() || token.precedence() < minPrev {
			break
		}
		nextPrecedence := token.precedence() + 1
		if next, err = p.next(); next == nil {
			return nil, err
		}
		// 递归计算右边的表达式
		right, err = p.computeMathOperator(nextPrecedence)
		if err != nil {
			return nil, err
		}
		// 计算 左右两方值
		left, err = token.computeExpr(left, right)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}
