package query

import (
	"github.com/kebukeYi/TrainSQL/record_manager"
)

type Scan interface {
	BeforeFirst()
	Next() bool
	// GetInt 获取当前记录的指定字段对应的int值
	GetInt(fldName string) int
	GetString(fldName string) string
	// GetVal Constant对应本模块里面的Constant定义
	GetVal(fldName string) *Constant
	HasField(fldName string) bool
	Close()
}

type UpdateScan interface {
	GetScan() Scan
	SetInt(fldName string, val int)
	SetString(fldName string, val string)
	SetVal(fldName string, val *Constant)
	// Insert 当前记录后插入一条记录;
	Insert()
	Delete()
	GetRid() *record_manager.RID
	MoveToRid(rid *record_manager.RID)
}

// Plan 将 Planner 接口定义放在这里是为了防止循环引用;
type Plan interface {
	Open() interface{}
	BlocksAccessed() int               //对应 B(s)
	RecordsOutput() int                //对应 R(s)
	DistinctValues(fldName string) int //对应 V(s,F)
	Schema() record_manager.SchemaInterface
}
