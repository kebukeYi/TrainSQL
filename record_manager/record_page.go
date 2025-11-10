package record_manager

import (
	"fmt"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type SLOT_FLAG int

const (
	EMPTY SLOT_FLAG = iota
	USED
)

type RecordPage struct {
	tx     *tx.Translation // 持久化更改记录的修改值;
	blk    *fm.BlockIndex  // 绑定的区块信息;
	layout LayoutInterface // 作用是获取字段的位移;
}

func NewRecordPage(tx *tx.Translation, blk *fm.BlockIndex,
	layout LayoutInterface) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
	// 1.bufferList 有空闲页,直接加载数据,返回;
	// 2.bufferList 被加载过,直接返回;
	// 3.bufferList 没有空闲页, 就死等;
	tx.Pin(blk)
	return rp
}

func (r *RecordPage) offset(slot int) uint64 {
	return uint64(slot * r.layout.SlotSize())
}

func (r *RecordPage) GetInt(slot int, field_name string) int {
	// 块内位移
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	// 第几块+块内位移;
	val, err := r.tx.GetInt(r.blk, field_pos)
	if err == nil {
		return int(val)
	}
	return -1
}

func (r *RecordPage) GetString(slot int, field_name string) string {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	val, _ := r.tx.GetString(r.blk, field_pos)
	return val
}

func (r *RecordPage) SetInt(slot int, field_name string, val int) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetInt(r.blk, field_pos, int64(val), true)
}

func (r *RecordPage) SetString(slot int, field_name string, val string) {
	field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
	r.tx.SetString(r.blk, field_pos, val, true)
}

func (r *RecordPage) Delete(slot int) {
	r.setFlag(slot, EMPTY)
}

func (r *RecordPage) Format() {
	slot := 0 // 第几个槽位;
	// 判断 此槽id是否 在一个块内;
	for r.isValidSlot(slot) {
		// 槽id的位移; 设置成 empty类型;
		r.tx.SetInt(r.blk, r.offset(slot), int64(EMPTY), false)
		sch := r.layout.Schema() // 元信息:字段列表;
		for _, field_name := range sch.Fields() {
			// 每个字段的起始位移;
			field_pos := r.offset(slot) + uint64(r.layout.Offset(field_name))
			if sch.Type(field_name) == INTEGER {
				r.tx.SetInt(r.blk, field_pos, 0, false)
			} else {
				r.tx.SetString(r.blk, field_pos, "", false)
			}
		}
		slot += 1
	}
}

func (r *RecordPage) NextAfter(slot int) int {
	return r.searchAfter(slot, USED)
}

func (r *RecordPage) InsertAfter(slot int) int {
	new_slot := r.searchAfter(slot, EMPTY)
	if new_slot >= 0 {
		r.setFlag(new_slot, USED)
	}
	return new_slot
}

func (r *RecordPage) Block() *fm.BlockIndex {
	return r.blk
}

func (r *RecordPage) setFlag(slot int, flag SLOT_FLAG) {
	r.tx.SetInt(r.blk, r.offset(slot), int64(flag), true)
}

func (r *RecordPage) searchAfter(slot int, flag SLOT_FLAG) int {
	slot += 1
	for r.isValidSlot(slot) {
		val, err := r.tx.GetInt(r.blk, r.offset(slot))
		if err != nil {
			fmt.Printf("SearchAfter has err %v\n", err)
			return -1
		}
		if SLOT_FLAG(val) == flag {
			return slot
		}
		slot += 1
	}
	return -1
}

func (r *RecordPage) isValidSlot(slot int) bool {
	// fmt.Printf("isValidSlot with slot num: %d, offset slot+1:%d:,
	// tx blockSize: %d\n,", slot, r.offset(slot+1), r.tx.BlockSize())
	return r.offset(slot+1) <= r.tx.BlockSize()
}
