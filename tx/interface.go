package tx

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	"math"
)

type TransationInterface interface {
	Commit()
	RollBack()
	Recover()
	Pin(blk *fm.BlockIndex)
	UnPin(blk *fm.BlockIndex)
	GetInt(blk *fm.BlockIndex, offset uint64) (int64, error)
	GetString(blk *fm.BlockIndex, offset uint64) (string, error)
	SetInt(blk *fm.BlockIndex, offset uint64, val int64, okToLog bool) error
	SetString(blk *fm.BlockIndex, offset uint64, val string, okToLog bool) error
	AvailableBuffers() uint64
	Size(filename string) (uint64, error)
	Append(filename string) (*fm.BlockIndex, error)
	BlockSize() uint64
}

const (
	UINT64_LENGTH = 8
	END_OF_FILE   = math.MaxUint64
)

type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

type LogRecordInterface interface {
	Op() RECORD_TYPE             //返回记录的类别
	TxNumber() uint64            //对应交易的号码
	Undo(tx TransationInterface) //回滚操作
	ToString() string            //获得记录的字符串内容
}

type LockTableInterface interface {
	SLock(blk *fm.BlockIndex, txNum int32) error //增加共享锁
	XLock(blk *fm.BlockIndex, txNum int32) error //增加互斥锁
	UnLock(blk *fm.BlockIndex, txNum int32)      //解除锁
}
