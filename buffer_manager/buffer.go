package buffer_manager

import (
	fmgr "github.com/kebukeYi/TrainSQL/file_manager"
	log "github.com/kebukeYi/TrainSQL/log_manager"
)

// Buffer 将磁盘上的区块数据, 复制一份到内存中, 在内存中加锁crud,最后再进行刷盘;
type Buffer struct {
	fm       *fmgr.FileManager // 作用: 被修改过后的持久化;
	lm       *log.LogManager   // 作用: 记录redoLog 日志
	contents *fmgr.Page        // 用于存储磁盘数据的缓存页面
	blk      *fmgr.BlockIndex  // 1 对 1 的绑定关系;
	pins     uint32            // 当前buffer 被使用引用计数
	txnum    int32             // 交易事务号
	lsn      uint64            // 对应日志管理器返回的序列号
}

func NewBuffer(file_mgr *fmgr.FileManager, log_mgr *log.LogManager) *Buffer {
	return &Buffer{
		fm:       file_mgr,
		lm:       log_mgr,
		txnum:    -1,
		contents: fmgr.NewPageBySize(file_mgr.BlockSize()),
	}
}

func (b *Buffer) Contents() *fmgr.Page {
	return b.contents
}

func (b *Buffer) Block() *fmgr.BlockIndex {
	return b.blk
}

func (b *Buffer) SetModified(txnum int32, lsn uint64) {
	// 如果客户修改了页面数据, 必须调用该接口通知Buffer;
	b.txnum = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.txnum
}

func (b *Buffer) AssignToBlock(block *fmgr.BlockIndex) {
	// 使用当前缓存页之前, 尝试将当前页面数据刷盘;
	b.Flush() // 当页面分发给新数据时需要判断当前页面数据是否需要写入磁盘;
	b.blk = block
	b.fm.Read(b.blk, b.Contents()) // 将对应数据从磁盘读取到缓存页面;
	b.pins = 0
}

func (b *Buffer) Flush() {
	// 当前页面数据已经被修改过，需要写入磁盘;
	if b.txnum >= 0 {
		b.lm.FlushByLSN(b.lsn)          // 先将修改操作对应的日志写入磁盘;
		b.fm.Write(b.blk, b.Contents()) // 再将数据写入磁盘;
		b.txnum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins = b.pins + 1
}

func (b *Buffer) Unpin() {
	b.pins = b.pins - 1
}
