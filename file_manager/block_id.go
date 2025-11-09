package file_manager

import (
	"crypto/sha256"
	"fmt"
)

// BlockIndex 可定位到文件空间的具体位移, 也叫区块索引符;
type BlockIndex struct {
	file_name string // 区块所在文件名
	blk_id    uint64 // 区块的标号, 区块大小初始化时被设置;
}

func NewBlockIndex(file_name string, blkId uint64) *BlockIndex {
	return &BlockIndex{
		file_name: file_name,
		blk_id:    blkId,
	}
}

func (b *BlockIndex) FileName() string {
	return b.file_name
}

func (b *BlockIndex) ID() uint64 {
	return b.blk_id
}

func (b *BlockIndex) Equals(other *BlockIndex) bool {
	return b.file_name == other.file_name && b.blk_id == other.blk_id
}
func (b *BlockIndex) HashCode() string {
	return asSha256(*b)
}
func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))

	return fmt.Sprintf("%x", h.Sum(nil))
}
