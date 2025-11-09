package record_manager

import (
	"github.com/kebukeYi/TrainSQL/file_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

const (
	BYTES_OF_INT = 8
)

type Layout struct {
	schema    SchemaInterface // 多个字段,名字,属性,长度;
	offsets   map[string]int  // 每个字段的偏移量;
	slot_size int             // 每条记录的长度;
}

func NewLayoutWithSchema(schema SchemaInterface) *Layout {
	layout := &Layout{
		schema:    schema,
		offsets:   make(map[string]int),
		slot_size: 0,
	}
	fields := schema.Fields()
	// pos = 8
	pos := tx.UINT64_LENGTH // 使用1个int64类型作为使用标志位,它占据8个字节;
	for i := 0; i < len(fields); i++ {
		// 从位移8处开始;
		layout.offsets[fields[i]] = pos
		pos += layout.lengthInBytes(fields[i])
	}
	layout.slot_size = pos // 每条记录的非定长记录;
	return layout
}

func NewLayout(schema SchemaInterface, offsets map[string]int, slot_size int) *Layout {
	return &Layout{
		schema:    schema,
		offsets:   offsets,
		slot_size: slot_size,
	}
}

func (l *Layout) Schema() SchemaInterface {
	return l.schema
}

func (l *Layout) SlotSize() int {
	return l.slot_size
}

func (l *Layout) Offset(field_name string) int {
	offset, ok := l.offsets[field_name]
	if !ok {
		return -1
	}
	return offset
}

func (l *Layout) lengthInBytes(field_name string) int {
	fld_type := l.schema.Type(field_name)
	p := file_manager.NewPageBySize(1)
	if fld_type == INTEGER {
		return BYTES_OF_INT // int 类型占用8个字节
	} else {
		// 先获取字段内容的长度
		field_len := l.schema.Length(field_name)
		/**
		因为是varchar类型，我们根据长度构造一个字符串，然后调用Page.MaxLengthForString
		获得写入页面时的数据长度，回忆一下在将字符串数据写入页面时，我们需要先写入8个字节用于记录
		写入字符串的长度
		*/
		dummy_str := string(make([]byte, field_len))
		return int(p.MaxLengthForString(dummy_str))
	}
}
