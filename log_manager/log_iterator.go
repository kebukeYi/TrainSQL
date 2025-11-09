package log_manager

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
)

/*
LogIterator 用于遍历给定区块内的记录, 由于记录从page页的底部往前写，因此记录 1,2,3,4 写入后,
在区块的排列为 块1:[2,1], 块2:[4,3]; LogIterator会从文件末尾往前读块区, 然后在块内是从前往后遍历记录,
 于是得到的记录就是 4,3,2,1;
*/

type LogIterator struct {
	FileManager    *fm.FileManager // 文件管理器(LogManager);
	blk            *fm.BlockIndex  // 磁盘块
	p              *fm.Page        // 缓存页;
	currentOffset  uint64          // 当前块内可读的偏移量,会每次被递增更新;
	logStartOffset uint64          // 获得可读日志的起始地址,固定值;
}

func NewLogIterator(file_manager *fm.FileManager, blk *fm.BlockIndex) *LogIterator {
	it := LogIterator{
		FileManager: file_manager,
		blk:         blk,
	}
	// 新建一个缓存页;
	it.p = fm.NewPageBySize(file_manager.BlockSize())
	// 将给定区块的数据读入;
	err := it.moveToBlock(blk)
	if err != nil {
		return nil
	}
	return &it
}

// 迭代器移动到给定索引号区块处;
func (l *LogIterator) moveToBlock(blk *fm.BlockIndex) error {
	// 打开存储日志数据的文件，遍历到给定区块，将数据读入内存page中;
	_, err := l.FileManager.Read(blk, l.p)
	if err != nil {
		return err
	}
	// 获得日志的起始地址;
	l.logStartOffset = l.p.GetInt(0)
	l.currentOffset = l.logStartOffset
	return nil
}

func (l *LogIterator) Next() []byte {
	// 块内正序读取; 块间逆序读取;
	// 先读取最新日志, 也就是编号大的, 然后依次读取编号小的;
	if l.currentOffset == l.FileManager.BlockSize() {
		// 说明当前块中没有数据了, 需要移动到前一个区块中;
		l.blk = fm.NewBlockIndex(l.blk.FileName(), l.blk.ID()-1)
		l.moveToBlock(l.blk)
	}
	// 指定偏移量读取字节数据;
	record := l.p.GetBytes(l.currentOffset)
	l.currentOffset += UINT64_LEN + uint64(len(record))

	return record
}

func (l *LogIterator) HasNext() bool {
	// 如果当前偏移位置 < 默认区块大小 => 那么说明还有数据可以从当前区块读取;
	// 如果当前区块数据已经全部读完, 但是区块号不为0，那么可以读取前面区块获得老的日志数据;
	return l.currentOffset < l.FileManager.BlockSize() || l.blk.ID() > 0
}
