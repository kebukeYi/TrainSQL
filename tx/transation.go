package tx

import (
	"errors"
	"fmt"
	bm "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lg "github.com/kebukeYi/TrainSQL/log_manager"
	"sync"
)

var tx_num_mu sync.Mutex
var next_tx_num = int32(0)

func nextTxNum() int32 {
	tx_num_mu.Lock()
	defer tx_num_mu.Unlock()
	next_tx_num = next_tx_num + 1
	return next_tx_num
}

type Translation struct {
	file_manager   *fm.FileManager     // 真实数据读写器;
	log_manager    *lg.LogManager      // redolog undolog 日志读写器;
	buffer_manager *bm.BufferManager   // 读写缓存管理器;
	my_buffers     *BufferList         // 管理pin, unpin
	concur_mgr     *ConCurrencyManager // 并发读写,管理器;
	recovery_mgr   *RecoveryManager    // 重启恢复, 管理器;
	tx_num         int32               // 唯一事务id;
}

func NewTransation(file_manager *fm.FileManager, log_manager *lg.LogManager,
	buffer_manager *bm.BufferManager) *Translation {
	tx_num := nextTxNum()
	tx := &Translation{
		file_manager:   file_manager,
		log_manager:    log_manager,
		buffer_manager: buffer_manager,
		my_buffers:     NewBufferList(buffer_manager),
		tx_num:         tx_num,
	}

	// 创建同步管理器
	tx.concur_mgr = NewConcurrencyManager()
	// 创建恢复管理器
	tx.recovery_mgr = NewRecoveryManager(tx, tx_num, log_manager, buffer_manager)
	return tx
}

// 事务1 对 blk1 写
// 事务2 对 blk1 写

// 事务1 对 blk1 读
// 事务2 对 blk1 写

// 事务1 对 blk1 写
// 事务2 对 blk1 读

// 事务1 对 blk1 读
// 事务2 对 blk1 读

func (t *Translation) Commit() {
	// 1.写 redoLog.commit 标记;
	// 2.将buffer中的新值写入磁盘中;
	t.recovery_mgr.Commit()
	// 3.通知其他在当前blk上读写阻塞的协程;
	t.concur_mgr.Release(t.tx_num)
	// fmt.Printf("transation %d  committed\n ", t.tx_num)
	// 4.释放本地 buffer.unPin();此时阻塞在tryPin()的协程可能被唤醒,也可能依然没有被唤醒;
	t.my_buffers.UnpinAll()
}

func (t *Translation) Close() {
}

func (t *Translation) Start() {
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, uint64(t.tx_num))
	start_record := NewStartRecord(p, t.log_manager)
	start_record.WriteToLog()
}

func (t *Translation) RollBack() {
	// 调用恢复管理器rollback;
	t.recovery_mgr.Rollback()
	//
	t.concur_mgr.Release(t.tx_num)
	fmt.Printf("transation %d roll back", t.tx_num)
	// 释放同步管理器
	t.my_buffers.UnpinAll()
}

func (t *Translation) Recover() {
	// 调用回复管理器的recover接口
	t.recovery_mgr.Recover()
}

func (t *Translation) Pin(blk *fm.BlockIndex) {
	t.my_buffers.Pin(blk)
}

func (t *Translation) UnPin(blk *fm.BlockIndex) {
	t.my_buffers.Unpin(blk)
}

func (t *Translation) buffer_no_exist(blk *fm.BlockIndex) error {
	err_s := fmt.Sprintf("No buffer found for given blk : %d with file name: %s\n",
		blk.ID(), blk.FileName())
	err := errors.New(err_s)
	return err
}

func (t *Translation) GetInt(blk *fm.BlockIndex, offset uint64) (int64, error) {
	// 调用同步管理器加s锁
	err := t.concur_mgr.SLock(blk, t.tx_num)
	if err != nil {
		return -1, err
	}
	buff := t.my_buffers.get_buffer(blk)
	if buff == nil {
		return -1, t.buffer_no_exist(blk)
	}

	return int64(buff.Contents().GetInt(offset)), nil
}

func (t *Translation) GetString(blk *fm.BlockIndex, offset uint64) (string, error) {
	//调用同步管理器加s锁
	err := t.concur_mgr.SLock(blk, t.tx_num)
	if err != nil {
		return "", err
	}

	buff := t.my_buffers.get_buffer(blk)
	if buff == nil {
		return "", t.buffer_no_exist(blk)
	}

	return buff.Contents().GetString(offset), nil
}

func (t *Translation) SetInt(blk *fm.BlockIndex, offset uint64, val int64, okToLog bool) error {
	// 调用同步管理器加x锁;
	err := t.concur_mgr.XLock(blk, t.tx_num)
	if err != nil {
		return err
	}

	buff := t.my_buffers.get_buffer(blk)
	if buff == nil {
		return t.buffer_no_exist(blk)
	}

	var lsn uint64
	if okToLog {
		// 调用恢复管理器的SetInt方法,目的是 记录操作日志;
		// 1.修改buffer中的值(重复操作);
		// 2.并将旧值记录到redoLog中! 非新值;
		// 3.新值要在 tx.commit()时,才会被flush到磁盘上;
		lsn, err = t.recovery_mgr.SetInt(buff, offset, val)
		if err != nil {
			return err
		}
	}
	// 修改buffer中的值;
	p := buff.Contents()
	// 真正修改数据页, 要等待事务commit()时,才刷盘;
	p.SetInt(offset, uint64(val))
	// 修改buffer的状态;
	buff.SetModified(t.tx_num, lsn)
	return nil
}

func (t *Translation) SetString(blk *fm.BlockIndex, offset uint64, val string, okToLog bool) error {
	//使用同步管理器加x锁
	err := t.concur_mgr.XLock(blk, t.tx_num)
	if err != nil {
		return err
	}

	buff := t.my_buffers.get_buffer(blk)
	if buff == nil {
		return t.buffer_no_exist(blk)
	}

	var lsn uint64

	if okToLog {
		//调用恢复管理器SetString方法
		lsn, err = t.recovery_mgr.SetString(buff, offset, val)
		if err != nil {
			return err
		}
	}

	p := buff.Contents()
	p.SetString(offset, val)
	buff.SetModified(t.tx_num, lsn)
	return nil
}

func (t *Translation) Size(file_name string) (uint64, error) {
	// 调用同步管理器加锁, 空块加锁, 禁止再申请新块;
	dummy_blk := fm.NewBlockIndex(file_name, uint64(END_OF_FILE))
	err := t.concur_mgr.SLock(dummy_blk, t.tx_num)
	if err != nil {
		return 0, err
	}
	s, _ := t.file_manager.BlockNums(file_name)
	return s, nil
}

func (t *Translation) Append(file_name string) (*fm.BlockIndex, error) {
	// 调用同步管理器加锁
	dummy_blk := fm.NewBlockIndex(file_name, END_OF_FILE)
	err := t.concur_mgr.XLock(dummy_blk, t.tx_num)
	if err != nil {
		return nil, err
	}
	blk, err := t.file_manager.CreatBlockIndex(file_name)
	if err != nil {
		return nil, err
	}

	return &blk, nil
}

func (t *Translation) BlockSize() uint64 {
	return t.file_manager.BlockSize()
}

func (t *Translation) AvailableBuffers() uint64 {
	return uint64(t.buffer_manager.Available())
}
