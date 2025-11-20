package executor

type ScanTableExecutor struct {
	TableName string
}

func NewScanTableExecutor(tableName string) *ScanTableExecutor {
	return &ScanTableExecutor{
		TableName: tableName,
	}
}
func (scan *ScanTableExecutor) Name() {
}
