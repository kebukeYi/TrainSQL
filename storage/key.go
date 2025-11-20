package storage

import (
	"encoding/binary"
	"github.com/kebukeYi/TrainSQL/sql/util"
)

var (
	NextVersion = "NextVersion_"
	TenActive   = "TenActive_"
	TxnWrite    = "TxnWrite_"
	KeyVersion  = "KeyVersion_"
)

func GetNextVersionKey() []byte {
	return []byte(NextVersion)
}

func GetTenActiveKey(version Version) []byte {
	buffer := []byte(TenActive)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(version))
	buffer = append(buffer, buf...)
	return buffer
}
func GetPrefixTenActiveKey() []byte {
	return []byte(TenActive)
}

func GetTenActiveKeyVersion(key []byte) Version {
	return Version(binary.BigEndian.Uint64(key[len(TenActive):]))
}
func GetTxnWriteKey(version Version, key []byte) []byte {
	buffer := []byte(TxnWrite)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(version))
	buffer = append(buffer, buf...)
	buffer = append(buffer, key...)
	return buffer
}
func GetPrefixTxnWriteKey(version Version) []byte {
	buffer := []byte(TxnWrite)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(version))
	buffer = append(buffer, buf...)
	return buffer
}

// GetTxnWriteKeyValue txnWriteKey: TxnWrite_version(8B)_key
// return  key
func GetTxnWriteKeyValue(txnWriteKey []byte) []byte {
	if len(txnWriteKey) < len(TxnWrite)+8 {
		return nil
	}
	return txnWriteKey[len(TxnWrite)+8:]
}
func GetKeyVersionKey(key []byte, version Version) []byte {
	buffer := []byte(KeyVersion)
	buffer = append(buffer, key...)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(version))
	buffer = append(buffer, buf...)
	return buffer
}

func GetPrefixKeyVersionKey(key []byte) []byte {
	buffer := []byte(KeyVersion)
	// buffer : KeyVersion_user_id
	buffer = append(buffer, key...)
	return buffer
}

// SplitKeyVersion key: KeyVersion_row_user_version
func SplitKeyVersion(key []byte) Version {
	if len(key) < len(KeyVersion)+8 {
		util.Error("SplitKeyVersion: key length error")
	}
	return Version(binary.BigEndian.Uint64(key[len(key)-8:]))
}

func GetRawKeyFromKeyVersion(keyVersion []byte) []byte {
	if len(keyVersion) < len(KeyVersion)+8 {
		util.Error("SplitKeyVersion: key length error")
	}
	// key: KeyVersion_key1_version(8字节)
	key := keyVersion[len(KeyVersion) : len(keyVersion)-8]
	return key
}
