package buffer_manager

import (
	"errors"
	"fmt"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"sync"
	"time"
)

const (
	MAX_TIME = 10 //分配页面时, 最多等待5秒
)

type BufferManager struct {
	buffer_pool   []*Buffer  // 多个buffer元素, 每个元素都1对1 blockID 区块,;
	num_available uint32     // 当前可用缓存页面数量
	mu            sync.Mutex // 锁,并发安全;
	cond          *sync.Cond // 通知变量;
}

func NewBufferManager(fm *fm.FileManager, lm *lm.LogManager, num_buffers uint32) *BufferManager {
	buffer_manager := &BufferManager{
		num_available: num_buffers,
	}
	for i := uint32(0); i < num_buffers; i++ {
		buffer := NewBuffer(fm, lm)
		buffer_manager.buffer_pool = append(buffer_manager.buffer_pool, buffer)
	}
	buffer_manager.cond = sync.NewCond(&buffer_manager.mu)
	return buffer_manager
}

// Available 返回当前可用缓存页面数量;
func (b *BufferManager) Available() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.num_available
}

func (b *BufferManager) BufferCap() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.buffer_pool)
}

func (b *BufferManager) FlushAllByTxNum(txNum int32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	// 将给定事务号所涉及的读写数据全部写入磁盘;
	// 可能一次事务涉及到多个区块数据;
	for _, buff := range b.buffer_pool {
		if buff.ModifyingTx() == txNum {
			// buffer 中的最新数据刷盘;
			buff.Flush()
		}
	}
}

func (b *BufferManager) Pin(blk *fm.BlockIndex) (*Buffer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	// 尝试获得 buffer 缓存页;
	buff := b.tryPin(blk)
	// 没有空余的 buffer 页, 尝试等待;
	for buff == nil && b.waitingTooLong(start) == false {
		// 如果无法获得缓存页面, 那么让调用者释放锁, 陷入等待状态;
		fmt.Printf("waiting for buffer for blk:%d;\n", blk.ID())
		b.cond.Wait()
		// 被唤醒后, 再次尝试, 重新尝试获得缓存页面;
		buff = b.tryPin(blk)
		if buff != nil {
			fmt.Printf("got buffer for blk:%d;\n", blk.ID())
			return buff, nil
		}
		fmt.Printf("coroutine to wait for buffer for blk:%d;\n", blk.ID())
		// 如果被唤醒后, 依然得不到缓存页面, 并且也已经超过最大时间, 那么就返回错误;
		// 否则继续 wait()等待;
	}
	if buff == nil {
		return nil, errors.New("no buffer available, careful for dead lock")
	}
	return buff, nil
}

func (b *BufferManager) Unpin(buff *Buffer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if buff == nil {
		return
	}
	buff.Unpin()
	// 可能存在其他事务也 pin 住 当前blk块;
	if !buff.IsPinned() {
		b.num_available = b.num_available + 1
		// 唤醒所有等待它的协程;
		b.cond.Broadcast()
	} else {
		// 不用唤醒所有等待它的协程;
	}
}

func (b *BufferManager) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_TIME {
		return true
	}
	return false
}

func (b *BufferManager) tryPin(blk *fm.BlockIndex) *Buffer {
	// 首先看给定的区块是否已经被读入某个缓存页;
	buff := b.findExistingBuffer(blk)
	// 说明没有被加载过;
	if buff == nil {
		// 查看是否还有可用缓存页, 然后将区块数据载入;
		buff = b.chooseUnpinBuffer()
		// 没有空闲的 缓存页;
		if buff == nil {
			// buffer 还没进行刷盘,也就是还没被事务所释放, 因此不能提前强制刷盘供其他事务使用;
			return nil
		}
		// 没有被加载过 && 存在空闲的缓存页;
		buff.AssignToBlock(blk)
	}

	// 被加载过 , 判断是否 pinned 过;
	if buff.IsPinned() == false {
		b.num_available = b.num_available - 1
	}
	buff.Pin()
	return buff
}

func (b *BufferManager) findExistingBuffer(blk *fm.BlockIndex) *Buffer {
	// 查看当前请求的区块, 是否已经被加载到了某个缓存页;
	// 如果是, 那么直接返回即可;否则返回nil;
	for _, buffer := range b.buffer_pool {
		block := buffer.Block()
		if block != nil && block.Equals(blk) {
			return buffer
		}
	}
	return nil
}

func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	// 选取一个没有被使用的缓存页;
	for _, buffer := range b.buffer_pool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}
