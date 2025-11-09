package metadata_manager

import (
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type IndexInfo struct {
	idxName   string     // 索引列名字
	fldName   string     // 列名字
	tblSchema *rm.Schema // 索引表-元数据
	idxLayout *rm.Layout // 索引表-布局
	tx        *tx.Translation
	si        *StatInfo
}

func NewIndexInfo(idxName string, fldName string, tblSchema *rm.Schema,
	tx *tx.Translation, si *StatInfo) *IndexInfo {
	idxInfo := &IndexInfo{
		idxName:   idxName,
		fldName:   fldName,
		tx:        tx,
		tblSchema: tblSchema,
		si:        si,
		idxLayout: nil,
	}
	idxInfo.idxLayout = idxInfo.CreateIdxLayout()
	return idxInfo
}

func (i *IndexInfo) Open() Index {
	// 在这里 构建不同的哈希算法对象
	return NewHashIndex(i.tx, i.idxName, i.idxLayout)
}

func (i *IndexInfo) BlocksAccessed() int {
	// 每块大小 / 每条记录大小 => 每块多少条记录
	rpb := int(i.tx.BlockSize()) / i.idxLayout.SlotSize()
	// 所有记录 / 每块多少记录 => 索引表有多少块
	numBlocks := i.si.RecordsOutput() / rpb
	// 所有块 / 100 => 每个hash索引表有多少块
	return HashIndexSearchCost(numBlocks, rpb)
}

func (i *IndexInfo) RecordsOutput() int {
	// 总行数 / 索引列字段不同取值的数量
	return i.si.RecordsOutput() / i.si.DistinctValues(i.fldName)
}

func (i *IndexInfo) DistinctValues(fldName string) int {
	if i.fldName == fldName {
		return 1
	}
	return i.si.DistinctValues(fldName)
}

func (i *IndexInfo) CreateIdxLayout() *rm.Layout {
	sch := rm.NewSchema()
	sch.AddIntField("block") // blockId
	sch.AddIntField("id")    // offset
	if i.tblSchema.Type(i.fldName) == rm.INTEGER {
		sch.AddIntField("dataval")
	} else {
		fldLen := i.tblSchema.Length(i.fldName)
		sch.AddStringField("dataval", fldLen)
	}
	return rm.NewLayoutWithSchema(sch)
}
