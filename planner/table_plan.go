package planner

import (
	"github.com/kebukeYi/TrainSQL/metadata_manager"
	"github.com/kebukeYi/TrainSQL/query"
	"github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type TablePlan struct {
	tx      *tx.Translation
	tblName string
	layout  *record_manager.Layout
	si      *metadata_manager.StatInfo
}

func NewTablePlan(tx *tx.Translation, tblName string, md *metadata_manager.MetaDataManager) *TablePlan {
	tablePlanner := TablePlan{
		tx:      tx,
		tblName: tblName,
	}

	tablePlanner.layout = md.GetLayout(tablePlanner.tblName, tablePlanner.tx)
	tablePlanner.si = md.GetStatInfo(tblName, tablePlanner.layout, tx)

	return &tablePlanner
}

func (t *TablePlan) Open() interface{} {
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
