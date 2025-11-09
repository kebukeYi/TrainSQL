package metadata_manager

//索引管理器需要使用到后面才讲到的SQL查询和索引算法知识，所以先放一放
import (
	"github.com/kebukeYi/TrainSQL/query"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type IndexManager struct {
	layout  *rm.Layout    // 索引元数据表的布局;
	tblMgr  *TableManager // 扫描表作用;
	statMgr *StatManager  // 统计信息管理器;
}

func NewIndexManager(isNew bool, tblMgr *TableManager, statMgr *StatManager, tx *tx.Translation) *IndexManager {
	if isNew {
		// 索引元数据表包含三个字段，索引名，对应的表名，被索引的字段名
		sch := rm.NewSchema()
		sch.AddStringField("indexname", MAX_NAME)
		sch.AddStringField("tablename", MAX_NAME)
		sch.AddStringField("fieldname", MAX_NAME)
		tblMgr.CreateTable("idxcat", sch, tx)
	}

	idxMgr := &IndexManager{
		tblMgr:  tblMgr,
		statMgr: statMgr,
		layout:  tblMgr.GetLayout("idxcat", tx),
	}

	return idxMgr
}

func (i *IndexManager) CreateIndex(idxName string, tblName string, fldName string, tx *tx.Translation) {
	//创建索引时就为其在 idxcat 索引元数据表中加入一条记录;
	ts := query.NewTableScan(tx, "idxcat", i.layout)
	ts.BeforeFirst()
	ts.Insert()
	ts.SetString("indexname", idxName)
	ts.SetString("tablename", tblName)
	ts.SetString("fieldname", fldName)

	ts.Close()
}

func (i *IndexManager) GetIndexInfo(tblName string, tx *tx.Translation) map[string]*IndexInfo {
	result := make(map[string]*IndexInfo)
	ts := query.NewTableScan(tx, "idxcat", i.layout)
	ts.BeforeFirst()
	for ts.Next() {
		if ts.GetString("tablename") == tblName {
			idxName := ts.GetString("indexname")
			fldName := ts.GetString("fieldname") // 获得索引列的名称;
			// 去找源表的元数据, 目的是 获得索引列的值类型;
			tblLayout := i.tblMgr.GetLayout(tblName, tx)
			tblSi := i.statMgr.GetStatInfo(tblName, tblLayout, tx)
			schema, ok := (tblLayout.Schema()).(*rm.Schema)
			if ok != true {
				panic("convert schema interface error")
			}
			// 建立索引结构信息;
			ii := NewIndexInfo(idxName, fldName, schema, tx, tblSi)
			result[fldName] = ii
		}
	}
	ts.Close()
	return result
}
