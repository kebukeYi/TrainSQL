package query

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	"github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

// TableScan 不做筛选,只做遍历,以及修改数据;
type TableScan struct {
	tx           *tx.Translation
	layout       record_manager.LayoutInterface        // 表的布局信息;
	rp           record_manager.RecordManagerInterface // 当前记录,并有修改作用;
	file_name    string                                // 要访问的文件名字;
	current_slot int                                   // 槽位默认为-1;
}

func NewTableScan(tx *tx.Translation, table_name string, layout record_manager.LayoutInterface) *TableScan {
	table_scan := &TableScan{
		tx:        tx,
		layout:    layout,
		file_name: table_name + ".tbl",
	}
	// 获得这个文件的的最新区块数量
	size, err := tx.Size(table_scan.file_name)
	if err != nil {
		panic(err)
	}
	if size == 0 {
		//如果文件为空，那么增加一个区块
		table_scan.MoveToNewBlock()
	} else {
		// 先读取第一个区块;
		table_scan.MoveToBlock(0)
	}
	return table_scan
}

func (t *TableScan) GetScan() Scan {
	return t
}

func (t *TableScan) Close() {
	if t.rp != nil {
		t.tx.UnPin(t.rp.Block())
	}
}

func (t *TableScan) BeforeFirst() {
	t.MoveToBlock(0)
}

func (t *TableScan) Next() bool {
	/*
		如果在当前区块找不到给定有效记录则遍历后续区块，直到所有区块都遍历为止
	*/
	t.current_slot = t.rp.NextAfter(t.current_slot)
	for t.current_slot < 0 {
		if t.AtLastBlock() {
			// 直到最后一个区块都找不到给定插槽
			return false
		}
		t.MoveToBlock(int(t.rp.Block().ID() + 1))
		t.current_slot = t.rp.NextAfter(t.current_slot)
	}
	return true
}

func (t *TableScan) GetInt(field_name string) int {
	return t.rp.GetInt(t.current_slot, field_name)
}

func (t *TableScan) GetString(field_name string) string {
	return t.rp.GetString(t.current_slot, field_name)
}

func (t *TableScan) GetVal(field_name string) *Constant {
	if t.layout.Schema().Type(field_name) == record_manager.INTEGER {
		iVal := t.GetInt(field_name)
		return NewConstantWithInt(&iVal)
	}
	sVal := t.GetString(field_name)
	return NewConstantWithString(&sVal)
}

func (t *TableScan) HasField(field_name string) bool {
	return t.layout.Schema().HasFields(field_name)
}

func (t *TableScan) SetInt(field_name string, val int) {
	t.rp.SetInt(t.current_slot, field_name, val)
}

func (t *TableScan) SetString(field_name string, val string) {
	t.rp.SetString(t.current_slot, field_name, val)
}

func (t *TableScan) SetVal(field_name string, val *Constant) {
	if t.layout.Schema().Type(field_name) == record_manager.INTEGER {
		t.SetInt(field_name, val.AsInt())
	} else {
		t.SetString(field_name, val.AsString())
	}
}

func (t *TableScan) Insert() {
	/*
		将当前插槽号指向下一个可用插槽;
	*/
	t.current_slot = t.rp.InsertAfter(t.current_slot)
	for t.current_slot < 0 { //当前区块找不到可用插槽
		if t.AtLastBlock() {
			//如果当前处于最后一个区块，那么新增一个区块
			t.MoveToNewBlock()
		} else {
			t.MoveToBlock(int(t.rp.Block().ID() + 1))
		}
		t.current_slot = t.rp.InsertAfter(t.current_slot)
	}
}

func (t *TableScan) Delete() {
	t.rp.Delete(t.current_slot)
}

func (t *TableScan) MoveToRid(r *record_manager.RID) {
	t.Close()
	blk := fm.NewBlockIndex(t.file_name, uint64(r.BlockNumber()))
	t.rp = record_manager.NewRecordPage(t.tx, blk, t.layout)
	t.current_slot = r.Slot()
}

func (t *TableScan) GetRid() *record_manager.RID {
	return record_manager.NewRID(int(t.rp.Block().ID()), t.current_slot)
}

func (t *TableScan) MoveToBlock(blk_num int) {
	t.Close()
	blk := fm.NewBlockIndex(t.file_name, uint64(blk_num))
	t.rp = record_manager.NewRecordPage(t.tx, blk, t.layout)
	t.current_slot = -1
}

func (t *TableScan) MoveToNewBlock() {
	t.Close()
	blk, err := t.tx.Append(t.file_name)
	if err != nil {
		panic(err)
	}
	t.rp = record_manager.NewRecordPage(t.tx, blk, t.layout)
	t.rp.Format()
	t.current_slot = -1
}

func (t *TableScan) AtLastBlock() bool {
	size, err := t.tx.Size(t.file_name)
	if err != nil {
		panic(err)
	}

	return t.rp.Block().ID() == size-1
}
