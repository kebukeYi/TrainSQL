package record_manager

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
)

type SchemaInterface interface {
	AddField(field_name string, field_type FIELD_TYPE, length int)
	AddIntField(field_name string)
	AddStringField(field_name string, length int)
	Add(field_name string, sch SchemaInterface)
	AddAll(sch SchemaInterface)
	Fields() []string
	HasFields(field_name string) bool
	Type(field_name string) FIELD_TYPE
	Length(field_name string) int
}

type LayoutInterface interface {
	Schema() SchemaInterface
	// 具体字段在单条记录的偏移量;
	Offset(field_name string) int
	// 文档描述: https://blog.csdn.net/tyler_download/article/details/127326804
	SlotSize() int
}

type RecordManagerInterface interface {
	Block() *fm.BlockIndex                          // 返回记录所在页面对应的区块
	GetInt(slot int, fldName string) int            // 根据给定字段名取出其对应的int值
	SetInt(slot int, fldName string, val int)       // 设定指定字段名的int值
	GetString(slot int, fldName string) string      // 根据给定字段名获取其字符串内容
	SetString(slot int, fldName string, val string) // 设置给定字段名的字符串内容
	Format()                                        // 将所有插槽中的记录设定为默认值
	Delete(slot int)                                // 将给定插槽的占用标志位设置为0
	NextAfter(slot int) int                         // 查找给定插槽之后第一个占用标志位为1的记录
	InsertAfter(slot int) int                       // 查找给定插槽之后第一个占用标志位为0的记录
}

type RIDInterface interface {
	BlockNumber() int //记录所在的区块号
	Slot() int        //记录的插槽号
	Equals(other RIDInterface) bool
	ToString() string
}
