package tx

import (
	bm "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
)

type BufferList struct {
	buffer_mgr *bm.BufferManager            // 与其他事务公用缓存管理器;
	buffers    map[fm.BlockIndex]*bm.Buffer // 管理本地事务 pin buffer
	pins       []fm.BlockIndex              // 当前事务所涉及到的缓存页
}

func NewBufferList(buffer_mgr *bm.BufferManager) *BufferList {
	buffer_list := &BufferList{
		buffer_mgr: buffer_mgr,
		buffers:    make(map[fm.BlockIndex]*bm.Buffer),
		pins:       make([]fm.BlockIndex, 0),
	}
	return buffer_list
}

func (b *BufferList) get_buffer(blk *fm.BlockIndex) *bm.Buffer {
	buff, _ := b.buffers[*blk]
	return buff
}

func (b *BufferList) Pin(blk *fm.BlockIndex) error {
	// 如果说 一个事务所涉及到的区块数据已经达到缓存池容量, 则返回错误;
	if len(b.pins) == b.buffer_mgr.BufferCap() {
		panic("buffer_list: no more buffers available")
	}
	// 一旦一个 blockId 被pin后, 将其加入map进行追踪管理;
	// 可能会阻塞....
	buff, err := b.buffer_mgr.Pin(blk)
	if err != nil {
		return err
	}
	// 为了方便访问, 将其加入pins中;
	b.buffers[*blk] = buff
	b.pins = append(b.pins, *blk)
	return nil
}

func (b *BufferList) Unpin(blk *fm.BlockIndex) {
	buff, ok := b.buffers[*blk]
	if !ok {
		return
	}

	b.buffer_mgr.Unpin(buff)

	for idx, pinned_blk := range b.pins {
		if pinned_blk == *blk {
			b.pins = append(b.pins[:idx], b.pins[idx+1:]...)
			break
		}
	}
	// debug index
	// delete(b.buffers, *blk)
}

func (b *BufferList) UnpinAll() {
	for _, blk := range b.pins {
		buffer := b.buffers[blk]
		// 当前事务把占据的缓存页释放掉;
		b.buffer_mgr.Unpin(buffer)
	}
	b.buffers = make(map[fm.BlockIndex]*bm.Buffer)
	b.pins = make([]fm.BlockIndex, 0)
}
