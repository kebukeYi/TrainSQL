package planner

import (
	"github.com/kebukeYi/TrainSQL/metadata_manager"
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type TablePlan struct {
	tx      *tx.Translation            // 目的是获得文件的具体信息;
	tblName string                     // 标的信息
	layout  *record_manager.Layout     // 表的信息
	si      *metadata_manager.StatInfo // 表的统计信息
}

func NewTablePlan(tx *tx.Translation, tblName string, md *metadata_manager.MetaDataManager) *TablePlan {
	tablePlanner := TablePlan{
		tx:      tx,
		tblName: tblName,
	}
	// 获得表的layout结构
	tablePlanner.layout = md.GetLayout(tablePlanner.tblName, tablePlanner.tx)
	// 为表创建统计信息
	tablePlanner.si = md.GetStatInfo(tblName, tablePlanner.layout, tx)
	return &tablePlanner
}

func (t *TablePlan) StartScan() interface{} {
	// 提供表扫描功能
	return query.NewTableScan(t.tx, t.tblName, t.layout)
}

func (t *TablePlan) RecordsOutput() int {
	return t.si.RecordsOutput()
}

func (t *TablePlan) BlocksAccessed() int {
	return t.si.BlocksAccessed()
}

func (t *TablePlan) DistinctValues(tblName string) int {
	return t.si.DistinctValues(tblName)
}

func (t *TablePlan) Schema() record_manager.SchemaInterface {
	return t.layout.Schema()
}
