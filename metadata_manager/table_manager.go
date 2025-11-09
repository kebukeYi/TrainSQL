package metadata_manager

import (
	"github.com/kebukeYi/TrainSQL/query"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

const (
	MAX_NAME = 16
)

// TableManager 管理所有数据表的元数据;
type TableManager struct {
	tcatLayout *rm.Layout
	fcatLayout *rm.Layout
}

func NewTableManager(isNew bool, tx *tx.Translation) *TableManager {
	tableMgr := &TableManager{}
	tcatSchema := rm.NewSchema()
	// 创建两个表专门用于存储新建数据库 表的元数据;
	tcatSchema.AddStringField("tblname", MAX_NAME)
	tcatSchema.AddIntField("slotsize")
	// 根据表的元数据: 有哪些列, 每列的类型是什么, 来得出每列的位移;
	tableMgr.tcatLayout = rm.NewLayoutWithSchema(tcatSchema)

	fcatSchema := rm.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME)
	fcatSchema.AddStringField("fldname", MAX_NAME)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	tableMgr.fcatLayout = rm.NewLayoutWithSchema(fcatSchema)

	if isNew {
		// 如果当前数据表是第一次创建，那么为这个表创建两个元数据表;
		tableMgr.CreateTable("tblcat", tcatSchema, tx)
		tableMgr.CreateTable("fldcat", fcatSchema, tx)
	}
	return tableMgr
}

func (t *TableManager) CreateTable(tblName string, sch *rm.Schema, tx *tx.Translation) {
	// 在创建数据表前先创建 tblcat, fldcat 两个元数据表;
	// 第一类元数据表数据;
	layout := rm.NewLayoutWithSchema(sch)
	tcat := query.NewTableScan(tx, "tblcat", t.tcatLayout)
	tcat.Insert()
	tcat.SetString("tblname", tblName)
	tcat.SetInt("slotsize", layout.SlotSize())
	tcat.Close()

	// 第二类元数据表的数据;
	fcat := query.NewTableScan(tx, "fldcat", t.fcatLayout)
	for _, fldName := range sch.Fields() {
		fcat.Insert()
		fcat.SetString("tblname", tblName)
		fcat.SetString("fldname", fldName)
		fcat.SetInt("type", int(sch.Type(fldName)))
		fcat.SetInt("length", sch.Length(fldName))
		fcat.SetInt("offset", layout.Offset(fldName))
	}
	fcat.Close()
}

func (t *TableManager) GetLayout(tblName string, tx *tx.Translation) *rm.Layout {
	// 获取给定表的layout结构;
	size := -1
	// 获得表的起始block块数据;
	tcat := query.NewTableScan(tx, "tblcat", t.tcatLayout)
	for tcat.Next() {
		// 找到给定表对应的元数据表;
		if tcat.GetString("tblname") == tblName {
			size = tcat.GetInt("slotsize")
			break
		}
	}
	tcat.Close()
	sch := rm.NewSchema()
	offsets := make(map[string]int)
	// 获取给定表对应的字段信息;
	fcat := query.NewTableScan(tx, "fldcat", t.fcatLayout)
	for fcat.Next() {
		if fcat.GetString("tblname") == tblName {
			fldName := fcat.GetString("fldname")
			fldType := fcat.GetInt("type")
			fldLen := fcat.GetInt("length")
			offset := fcat.GetInt("offset")
			offsets[fldName] = offset
			sch.AddField(fldName, rm.FIELD_TYPE(fldType), fldLen)
		}
	}
	fcat.Close()
	return rm.NewLayout(sch, offsets, size)
}
