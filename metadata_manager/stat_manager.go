package metadata_manager

import (
	"github.com/kebukeYi/TrainSQL/query"
	rm "github.com/kebukeYi/TrainSQL/record_manager"
	"github.com/kebukeYi/TrainSQL/tx"
	"sync"
)

const (
	// 数据库表发生变化100次后更新统计数据
	REFRESH_STAT_INFO_COUNT = 100
)

type StatInfo struct {
	numBlocks int // 数据库表的区块数
	numRecs   int // 数据库表包含的记录数
}

func newStatInfo(numBlocks int, numRecs int) *StatInfo {
	return &StatInfo{
		numBlocks: numBlocks,
		numRecs:   numRecs,
	}
}

func (s *StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

func (s *StatInfo) RecordsOutput() int {
	return s.numRecs
}

func (s *StatInfo) DistinctValues(fldName string) int {
	// 某个字段, 包含多少行不同的值
	return 1 + (s.numRecs / 3) // 初步认为三分之一，后面再修改;
}

type StatManager struct {
	tblMgr     *TableManager
	tableStats map[string]*StatInfo // 每张表的统计信息,包括行数以及块数;
	numCalls   int                  // 统计数据调用次数
	lock       sync.Mutex
}

func NewStatManager(tblMgr *TableManager, tx *tx.Translation) *StatManager {
	statMgr := &StatManager{
		tblMgr:   tblMgr,
		numCalls: 0,
	}
	// 更新统计数据;
	statMgr.refreshStatistics(tx)
	return statMgr
}

func (s *StatManager) GetStatInfo(tblName string, layout *rm.Layout, tx *tx.Translation) *StatInfo {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.numCalls += 1
	if s.numCalls > REFRESH_STAT_INFO_COUNT {
		s.refreshStatistics(tx)
	}
	si := s.tableStats[tblName]
	if si == nil {
		// 为新数据库表创建统计对象
		si = s.calcTableStats(tblName, layout, tx)
		s.tableStats[tblName] = si
	}
	return si
}

func (s *StatManager) refreshStatistics(tx *tx.Translation) {
	s.tableStats = make(map[string]*StatInfo)
	s.numCalls = 0
	tcatLayout := s.tblMgr.GetLayout("tblcat", tx)
	tcat := query.NewTableScan(tx, "tblcat", tcatLayout)
	for tcat.Next() {
		tblName := tcat.GetString("tblname")
		layout := s.tblMgr.GetLayout(tblName, tx)
		// 计算数据库, 表有多少行记录, 以及涉及到的区块数量;
		si := s.calcTableStats(tblName, layout, tx)
		s.tableStats[tblName] = si
	}
	tcat.Close()
}

func (s *StatManager) calcTableStats(tblName string, layout *rm.Layout, tx *tx.Translation) *StatInfo {
	numRecs := 0
	numBlocks := 0
	ts := query.NewTableScan(tx, tblName, layout)
	for ts.Next() {
		numRecs += 1
		numBlocks = ts.GetRid().BlockNumber() + 1
	}
	ts.Close()
	return newStatInfo(numBlocks, numRecs)
}
