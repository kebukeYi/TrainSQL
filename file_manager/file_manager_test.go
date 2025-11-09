package file_manager

import (
	"github.com/kebukeYi/TrainSQL/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFileManager(t *testing.T) {
	// 0. 清理文件
	util.ClearDir(util.FileManageTestDirectory)
	// 1. 创建文件管理器的目录;
	fm, _ := NewFileManager(util.FileManageTestDirectory, 400)

	// 2. 创建文件的索引块;
	blk := NewBlockIndex("test_file", 2)

	// 3. 创建索引块的内存page页;
	p1 := NewPageBySize(fm.BlockSize())

	pos1 := uint64(88)

	s := "abcdefghijklm"
	p1.SetString(pos1, s)
	size := p1.MaxLengthForString(s)

	pos2 := pos1 + size
	val := uint64(345)
	p1.SetInt(pos2, val)

	fm.Write(blk, p1)

	// 默认按照 blockSize 读取数据内容;
	p2 := NewPageBySize(fm.BlockSize())
	fm.Read(blk, p2)

	require.Equal(t, val, p2.GetInt(pos2))

	require.Equal(t, s, p2.GetString(pos1))
}
