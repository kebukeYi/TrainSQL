package storage

type RangeBounds struct {
	StartKey []byte
	EndKey   []byte
}

type ResultPair struct {
	Key   []byte
	Value []byte
}

func (receiver *ResultPair) ToString() string {
	return "[" + string(receiver.Key) + ":" + string(receiver.Value) + "]"
}

type Storage interface {
	Lock()
	UnLock()
	Get(key []byte) []byte
	Set(key []byte, value []byte)
	Delete(key []byte)
	//Scan [greaterOrEqual, lessThan)
	Scan(bounds *RangeBounds) []*ResultPair
	// ScanPrefix [^prefix]
	ScanPrefix(keyPrefix []byte, isValue bool) []*ResultPair
	Close() error
}
