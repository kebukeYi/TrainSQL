package storage

import (
	"encoding/binary"
	"github.com/kebukeYi/TrainSQL/sql/util"
	"math"
)

type TransactionManager struct {
	storage Storage
}

func NewTransactionManager(storage Storage) *TransactionManager {
	return &TransactionManager{
		storage: storage,
	}
}

func (m *TransactionManager) Begin() *Transaction {
	t := NewTransaction(m.storage)
	t.begin()
	return t
}

type Version uint64

type TransactionState struct {
	Version        Version
	activeVersions []Version
}

func (s *TransactionState) isVisible(version Version) bool {
	for _, v := range s.activeVersions {
		if v == (version) {
			return false
		}
	}
	return (version) <= (s.Version)
}

func (s *TransactionState) getMinVersion() Version {
	version := Version(math.MaxInt64)
	for _, activeVersion := range s.activeVersions {
		if activeVersion < version {
			version = activeVersion
		}
	}
	return version
}

type Transaction struct {
	storage          Storage
	transactionState *TransactionState
}

func (t *Transaction) Version() Version {
	return t.transactionState.Version
}

func NewTransaction(storage Storage) *Transaction {
	return &Transaction{
		storage: storage,
	}
}

func (t *Transaction) begin() *Transaction {
	t.storage.Lock()
	defer t.storage.UnLock()
	// nextVersionKey : NextVersion_
	nextVersionKey := GetNextVersionKey()
	version := t.storage.Get(nextVersionKey)
	nextVersion := Version(0)
	if version == nil {
		nextVersion += 1
	} else {
		v := binary.LittleEndian.Uint64(version)
		nextVersion = Version(v)
	}
	t.storage.Set(nextVersionKey, binary.LittleEndian.AppendUint64(nil, uint64(nextVersion+1)))
	activeVersions := t.ScanActive()
	// key: TenActive_version(8字节)
	key := GetTenActiveKey(nextVersion)
	t.storage.Set(key, nil)
	t.transactionState = &TransactionState{
		Version:        nextVersion,
		activeVersions: activeVersions,
	}
	return t
}

func (t *Transaction) ScanActive() []Version {
	versions := make([]Version, 0)
	// key: TenActive_ ; 仅仅前缀扫描所有满足的元素;
	key := GetPrefixTenActiveKey()
	// 直接在存储引擎进行扫描; 不需要value;
	pairs := t.storage.ScanPrefix(key, false)
	for _, pair := range pairs {
		// pair.key: TenActive_version(8字节)
		// 获得 pair.key 的后8字节 version;
		version := GetTenActiveKeyVersion(pair.Key)
		versions = append(versions, version)
	}
	return versions
}

func (t *Transaction) Commit() {
	t.storage.Lock()
	defer t.storage.UnLock()
	deleteKeys := make([][]byte, 0)
	// writeKey: TxnWrite_version(8字节)
	writeKey := GetPrefixTxnWriteKey(t.transactionState.Version)
	// 前缀扫描 获得当前事务 写入的所有操作记录;
	// 因为不知道当前事务写了哪些具体的数据,因此需要扫描匹配;
	// writeKey: TxnWrite_version(8字节)_key
	pairs := t.storage.ScanPrefix(writeKey, false)
	for _, pair := range pairs {
		deleteKeys = append(deleteKeys, pair.Key)
	}
	// 所有具体的操作记录, 原封不动的进行删除;
	for _, key := range deleteKeys {
		t.storage.Delete(key)
	}
	// key: TenActive_version(8字节); 删除掉当前事务,不再是活跃事务;
	key := GetTenActiveKey(t.transactionState.Version)
	t.storage.Delete(key)
}

func (t *Transaction) Rollback() {
	t.storage.Lock()
	defer t.storage.UnLock()
	deleteKeys := make([][]byte, 0)
	// key: TxnWrite_version(8字节)
	key := GetPrefixTxnWriteKey(t.transactionState.Version)
	// 前缀扫描 获得当前事务 写入的所有操作记录;
	pairs := t.storage.ScanPrefix(key, false)
	for _, pair := range pairs {
		// pair.Key: TxnWrite_version_key
		// txnWriteKeyValue: 获得涉及具体key;
		txnWriteKeyValue := GetTxnWriteKeyValue(pair.Key)
		// versionKey: KeyVersion_key_version(8字节), 定向删除具体的记录;
		versionKey := GetKeyVersionKey(txnWriteKeyValue, t.transactionState.Version)
		deleteKeys = append(deleteKeys, versionKey)
	}
	for _, key := range deleteKeys {
		// 将事务写入的row记录进行删除;
		t.storage.Delete(key)
	}
	// tenActiveKey: TenActive_version(8字节); 删除掉当前事务,不再是活跃事务;
	tenActiveKey := GetTenActiveKey(t.transactionState.Version)
	t.storage.Delete(tenActiveKey)
}

func (t *Transaction) Set(key []byte, value []byte) error {
	t.storage.Lock()
	defer t.storage.UnLock()
	return t.writeInner(key, value)
}
func (t *Transaction) Delete(key []byte) error {
	t.storage.Lock()
	defer t.storage.UnLock()
	return t.writeInner(key, nil)
}
func (t *Transaction) writeInner(key []byte, value []byte) error {
	var checkVersion Version
	// 活跃事务为空, 说明当前事务为第一个启动事务; 但是之后存在哪些事务不清楚;
	if t.transactionState.activeVersions == nil || len(t.transactionState.activeVersions) == 0 {
		checkVersion = t.transactionState.Version + 1
	} else {
		// 从已知的最小活跃事务开始, 范围要包含全部, 寻找可能和当前事务发生冲突的记录;
		checkVersion = t.transactionState.getMinVersion()
	}
	// from: KeyVersion_key_checkVersion;
	from := GetKeyVersionKey(key, checkVersion)
	// to: KeyVersion_key_maxInt64;
	to := GetKeyVersionKey(key, math.MaxInt64)
	rangeBounds := &RangeBounds{
		StartKey: from,
		EndKey:   to,
	}
	// 当前活跃事务列表 3 4 5
	// 当前事务 6
	// 只需要判断key的最后一个版本号:
	// 1. key 按照顺序排列, 扫描出的结果是从小到大的;
	// 2. 假如有新的的事务修改了这个 key,  比如 10, 修改之后 10 提交了, 那么 6 再修改这个 key 就是冲突的;
	// 3. 如果是当前活跃事务修改了这个 key, 比如 4, 那么新事务 5 就不可能修改这个 key;
	// [from,to)
	resultPairs := t.storage.Scan(rangeBounds)
	if resultPairs != nil {
		lastKeyVersionPair := resultPairs[len(resultPairs)-1]
		// keyVersion: KeyVersion_row_user_version
		keyVersion := SplitKeyVersion(lastKeyVersionPair.Key)
		if !t.transactionState.isVisible(keyVersion) {
			return util.WriteConflict
		}
	}
	// writeKey: TxnWrite_version(8字节)_key; 当前事务的操作记录; 可供当前事务事后的前缀扫描查询;
	writeKey := GetTxnWriteKey(t.transactionState.Version, key)
	// 记录 当前事务(version) 写入了哪些 key 记录, 用于回滚事务;
	t.storage.Set(writeKey, nil)
	// versionKey: KeyVersion_key_version(8字节), value
	versionKey := GetKeyVersionKey(key, t.transactionState.Version)
	t.storage.Set(versionKey, value)
	return nil
}

func (t *Transaction) Get(key []byte) []byte {
	t.storage.Lock()
	defer t.storage.UnLock()
	// version: 9
	// 扫描的 version 的范围应该是 0-9
	// KeyVersion_key_version0 - KeyVersion_key_version9
	from := GetKeyVersionKey(key, 0)
	to := GetKeyVersionKey(key, t.transactionState.Version+1)
	rangeBounds := &RangeBounds{
		StartKey: from,
		EndKey:   to,
	}
	// [from,to) 扫扫描的是 左开右闭区间;
	resultPairs := t.storage.Scan(rangeBounds)
	length := len(resultPairs)
	for i := length - 1; i >= 0; i-- {
		// KeyVersion_row_user_version
		version := SplitKeyVersion(resultPairs[i].Key)
		if t.transactionState.isVisible(version) {
			if resultPairs[i].Value == nil || len(resultPairs[i].Value) == 0 {
				return nil
			}
			return resultPairs[i].Value
		}
	}
	return nil
}

func (t *Transaction) ScanPrefix(keyPrefix []byte, needValue bool) []ResultPair {
	// keyPrefix: key1  原生key性质, 需要进一步的组装;
	// keyVersionKey: KeyVersion_key1
	keyVersionKey := GetPrefixKeyVersionKey(keyPrefix)
	// pair: [KeyVersion_row_user_version, value]
	resultPairs := t.storage.ScanPrefix(keyVersionKey, needValue)
	newResultPairs := make([]ResultPair, 0)
	for _, pair := range resultPairs {
		// 将有删除标记的记录过滤掉;
		if pair.Value == nil || len(pair.Value) == 0 {
			continue
		}
		version := SplitKeyVersion(pair.Key)
		if t.transactionState.isVisible(version) {
			// pair.Key: KeyVersion_Row_user_version(8字节)
			// rawKey: key1
			rawKey := GetRawKeyFromKeyVersion(pair.Key)
			r := ResultPair{
				Key:   rawKey,
				Value: pair.Value,
			}
			newResultPairs = append(newResultPairs, r)
		}
	}
	return newResultPairs
}
