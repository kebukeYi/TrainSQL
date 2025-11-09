package tx

import (
	bm "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lg "github.com/kebukeYi/TrainSQL/log_manager"
)

type RecoveryManager struct {
	log_manager    *lg.LogManager    // 日志管理器;
	buffer_manager *bm.BufferManager // 为的是将最新的buffer数据刷盘;
	tx             *Translation      // 数据库重启时, undoLog, 重写更改blk作用;
	tx_num         int32
}

func NewRecoveryManager(tx *Translation, tx_num int32, log_manager *lg.LogManager,
	buffer_manager *bm.BufferManager) *RecoveryManager {

	recovery_mgr := &RecoveryManager{
		tx:             tx,
		tx_num:         tx_num,
		log_manager:    log_manager,
		buffer_manager: buffer_manager,
	}
	return recovery_mgr
}

func (r *RecoveryManager) Commit() error {
	// 1. 数据修改 log 刷盘;
	// 2. 将修改的 buffer 数据都被刷入磁盘;
	// 3. 此时buffer中的数据并没有值为空, 也就是意味着下一个事务, 能读到已提交的最新数据;
	r.buffer_manager.FlushAllByTxNum(r.tx_num)
	// 4. 追加commit结束标志;
	lsn, err := WriteCommitRecordLog(r.log_manager, uint64(r.tx_num))
	if err != nil {
		return err
	}
	// 5.确保commit日志被刷入磁盘;
	r.log_manager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) Rollback() error {
	// 遍历已经存在的日志,并找到源磁盘地址,读取到内存中, 进行加锁, 逆向操作;
	r.doRollback()
	// 回滚的数据, 重新回到buffer后,重新刷盘;
	r.buffer_manager.FlushAllByTxNum(r.tx_num)
	// 记录 rollback 日志;
	lsn, err := WriteRollBackLog(r.log_manager, uint64(r.tx_num))
	if err != nil {
		return err
	}
	// rollback日志刷盘;
	r.log_manager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) Recover() error {
	r.doRecover()
	r.buffer_manager.FlushAllByTxNum(r.tx_num)
	// 每次重启时, 恢复完毕后, 及时打上 快照点, 加速以后数据恢复;
	lsn, err := WriteCheckPointToLog(r.log_manager)
	if err != nil {
		return err
	}
	// 强制刷盘一下;
	r.log_manager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) SetInt(buffer *bm.Buffer, offset uint64, new_val int64) (uint64, error) {
	// page 处拿到旧值
	old_val := buffer.Contents().GetInt(offset)
	// page 处写入新值(后面会再重新写一遍的)
	buffer.Contents().SetInt(offset, uint64(new_val))
	blk := buffer.Block() // 得到 block 所指向的文件位移, 目的是将旧值写入磁盘
	return WriteSetIntLog(r.log_manager, uint64(r.tx_num), blk, offset, old_val)
}

func (r *RecoveryManager) SetString(buffer *bm.Buffer, offset uint64, new_val string) (uint64, error) {
	old_val := buffer.Contents().GetString(offset)
	blk := buffer.Block()
	buffer.Contents().SetString(offset, new_val)
	return WriteSetStringLog(r.log_manager, uint64(r.tx_num), blk, offset, old_val)
}

func (r *RecoveryManager) CreateLogRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes)
	switch RECORD_TYPE(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckPointRecord()
	case START:
		return NewStartRecord(p, r.log_manager)
	case COMMIT:
		return NewCommitkRecordRecord(p)
	case ROLLBACK:
		return NewRollBackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("Unknow log interface")
	}
}

func (r *RecoveryManager) doRollback() {
	// 遍历已经存在的日志,并找到源磁盘地址,读取到内存中, 进行加锁, 逆向操作;
	iter := r.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_record := r.CreateLogRecord(rec)
		// 日志匹配, 指定回滚的事务号;
		if log_record.TxNumber() == uint64(r.tx_num) {
			if log_record.Op() == START {
				return
			}
			// 就是把磁盘上的旧值重新赋值到内存中, 因为内存中有旧值, 就会进行回滚;
			log_record.Undo(r.tx)
		}
		// 不相同的事务, 不需要进行回滚;
	}
}

func (r *RecoveryManager) doRecover() {
	finishedTxs := make(map[uint64]bool)
	iter := r.log_manager.Iterator()
	for iter.HasNext() {
		rec := iter.Next()
		log_record := r.CreateLogRecord(rec)
		// 遇到快照点,直接退出, 不再继续扫描日志;
		if log_record.Op() == CHECKPOINT {
			return
		}
		// 遇到提交或者回滚, 添加到已经完成事务列表中, 已完成的事务, 不再继续执行;
		if log_record.Op() == COMMIT || log_record.Op() == ROLLBACK {
			finishedTxs[log_record.TxNumber()] = true
		}
		// 没有提交或者, 回滚标记的, 都需要进行回滚操作;
		existed, ok := finishedTxs[log_record.TxNumber()]
		if !ok || !existed {
			log_record.Undo(r.tx)
		}
	}
}
