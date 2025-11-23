package sql

import "github.com/kebukeYi/TrainSQL/sql/types"

type Node interface {
	node()
}

type UpdateNode struct {
	TableName string
	Source    Node
	columns   map[string]*types.Expression
}

func (u *UpdateNode) node() {

}

type CreateTableNode struct {
	Schema *types.Table
}

func (c *CreateTableNode) node() {
}

type DropTableNode struct {
	TableName string
}

func (d *DropTableNode) node() {
}

type InsertNode struct {
	TableName string
	Columns   []string
	Values    [][]*types.Expression
}

func (i *InsertNode) node() {
}

type ScanNode struct {
	TableName string
	Filter    *types.Expression
}

func (s *ScanNode) node() {
}

type DeleteNode struct {
	TableName string
	Source    Node
}

func (d *DeleteNode) node() {
}

type OrderDirection int

var (
	OrderAsc  OrderDirection = 1
	OrderDesc OrderDirection = 2
)

type OrderNode struct {
	Source  Node
	OrderBy map[string]OrderDirection
}

func (o *OrderNode) node() {
}

type LimitNode struct {
	Source Node
	Limit  int
}

func (l *LimitNode) node() {}

type OffsetNode struct {
	Source Node
	Offset int
}

func (o *OffsetNode) node() {}

type ProjectNode struct {
	Source Node
	Exprs  map[*types.Expression]string
}

func (p *ProjectNode) node() {
}

type NestedLoopJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (n *NestedLoopJoinNode) node() {
}

type HashJoinNode struct {
	Left      Node
	Right     Node
	Predicate *types.Expression
	Outer     bool
}

func (h *HashJoinNode) node() {
}

type AggregateNode struct {
	Source  Node
	Exprs   map[*types.Expression]string
	GroupBy *types.Expression
}

func (a *AggregateNode) node() {
}

type FilterNode struct {
	Source    Node
	Predicate *types.Expression
}

func (f *FilterNode) node() {
}

type IndexScanNode struct {
	TableName string
	Filed     string
	Value     types.Value
}

func (i *IndexScanNode) node() {
}

type PrimaryKeyScanNode struct {
	TableName string
	Value     types.Value
}

func (p *PrimaryKeyScanNode) node() {
}

type FromItem interface {
	Item()
}

type TableItem struct {
	TableName string
}

func (t *TableItem) Item() {
}

type JoinType int

var (
	CrossType JoinType = 1
	InnerType JoinType = 2
	LeftType  JoinType = 3
	RightType JoinType = 4
)

type JoinItem struct {
	Left      FromItem
	Right     FromItem
	JoinType  JoinType
	Predicate *types.Expression
}

func (j *JoinItem) Item() {
}
