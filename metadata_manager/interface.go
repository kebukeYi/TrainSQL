package metadata_manager

import (
	"github.com/kebukeYi/TrainSQL/query"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type TableManagerInterface interface {
	CreateTable(tblName string, sch *rm.Schema, tx *tx.Translation)
	GetLayout(tblName string, tx *tx.Translation) *rm.Layout
}

type Index interface {
	// BeforeFirst 指向第一条满足查询条件的记录
	BeforeFirst(searchKey *query.Constant)
	Next() bool
	GetDataRID() *rm.RID
	Insert(dataval *query.Constant, datarid *rm.RID)
	Delete(dataval *query.Constant, datarid *rm.RID)
	Close()
}
