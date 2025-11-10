package query

import (
	"github.com/kebukeYi/TrainSQL/record_manager"
)

// SelectionScan 作用于: select name,age from student where age > 20;
type SelectionScan struct {
	updateScan UpdateScan // 默认是 tableScan
	pred       *Predicate // where 表达式
}

func NewSelectionScan(s UpdateScan, pred *Predicate) *SelectionScan {
	return &SelectionScan{
		updateScan: s,
		pred:       pred,
	}
}

func (s *SelectionScan) BeforeFirst() {
	s.updateScan.GetScan().BeforeFirst()
}

func (s *SelectionScan) Next() bool {
	for s.updateScan.GetScan().Next() {
		// 判断 是否满足 where age = 20;
		scan := s.updateScan.GetScan()
		if s.pred.IsSatisfied(scan) {
			return true
		} else {
			// 不匹配则跳过, 继续下一个 tableScan.next();
		}
	}
	return false
}

func (s *SelectionScan) GetInt(fldName string) int {
	return s.updateScan.GetScan().GetInt(fldName)
}

func (s *SelectionScan) GetString(fldName string) string {
	return s.updateScan.GetScan().GetString(fldName)
}

func (s *SelectionScan) GetVal(fldName string) *Constant {
	return s.updateScan.GetScan().GetVal(fldName)
}

func (s *SelectionScan) HasField(fldName string) bool {
	return s.updateScan.GetScan().HasField(fldName)
}

func (s *SelectionScan) Close() {
	s.updateScan.GetScan().Close()
}

func (s *SelectionScan) SetInt(fldName string, val int) {
	s.updateScan.SetInt(fldName, val)
}

func (s *SelectionScan) SetString(fldName string, val string) {
	s.updateScan.SetString(fldName, val)
}

func (s *SelectionScan) SetVal(fldName string, val *Constant) {
	s.updateScan.SetVal(fldName, val)
}

func (s *SelectionScan) Delete() {
	s.updateScan.Delete()
}

func (s *SelectionScan) Insert() {
	s.updateScan.Insert()
}

func (s *SelectionScan) GetRid() *record_manager.RID {
	return s.updateScan.GetRid()
}

func (s *SelectionScan) MoveToRID(rid *record_manager.RID) {
	s.updateScan.MoveToRid(rid)
}
