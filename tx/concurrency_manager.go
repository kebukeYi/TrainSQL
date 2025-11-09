package tx

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
)

type ConCurrencyManager struct {
	lock_table *LockTable               // 单例,所有事务公用; 包含 S锁 和 X锁;
	lock_map   map[fm.BlockIndex]string // 每个事务单独一份;
}

func NewConcurrencyManager() *ConCurrencyManager {
	concurrency_mgr := &ConCurrencyManager{
		lock_table: GetLockTableInstance(),         // 是全局唯一的;
		lock_map:   make(map[fm.BlockIndex]string), // 都单独有的一份;
	}
	return concurrency_mgr
}

func (c *ConCurrencyManager) SLock(blk *fm.BlockIndex, txNum int32) error {
	// 先查看自己是否已经加过锁了, 无论是 S X 锁;
	_, ok := c.lock_map[*blk]
	if !ok {
		// 没有加过锁, 那么才去全局申请加S锁;
		err := c.lock_table.SLock(blk, txNum)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "S"
	}
	return nil
}

func (c *ConCurrencyManager) XLock(blk *fm.BlockIndex, txNum int32) error {
	// 先查看自己是否已经加过锁了, 无论是 S X 锁;
	_, ok := c.lock_map[*blk]
	if !ok {
		// 判断区块是否已经被加上共享锁，如果别人已经获得共享锁那么就会挂起;
		err := c.lock_table.XLock(blk, txNum)
		if err != nil {
			return err
		}
		c.lock_map[*blk] = "X"
	}
	return nil
}

func (c *ConCurrencyManager) Release(txNum int32) {
	// 自己记录了自己拥有的锁, 方便后续释放锁;
	for key, _ := range c.lock_map {
		// 全局锁释放;
		c.lock_table.UnLock(&key, txNum)
	}
}

func (c *ConCurrencyManager) hasXLock(blk *fm.BlockIndex) bool {
	lock_type, ok := c.lock_map[*blk]
	return ok && lock_type == "X"
}
