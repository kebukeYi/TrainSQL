package tx

import (
	"errors"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	"sync"
	"time"
)

const (
	//MAX_WAITING_TIME = 10 // 3秒用于测试，在正式使用时设置为 10秒
	MAX_WAITING_TIME = 3 // 3秒用于测试，在正式使用时设置为 10秒
)

type LockTable struct {
	lock_map    map[fm.BlockIndex]int64           // 将锁和区块对应起来
	notify_chan map[fm.BlockIndex]chan struct{}   // 用于实现超时回退的管道
	notify_wg   map[fm.BlockIndex]*sync.WaitGroup // 用于实现唤醒通知
	method_lock sync.Mutex                        // 实现方法调用的线程安全, 全局互斥锁
}

var lock_table_instance *LockTable
var lock = &sync.Mutex{}

func GetLockTableInstance() *LockTable {
	lock.Lock()
	defer lock.Unlock()
	if lock_table_instance == nil {
		lock_table_instance = NewLockTable()
	}
	return lock_table_instance
}

func (l *LockTable) waitGivenTimeOut(blk *fm.BlockIndex, txNum int32) {
	wg, ok := l.notify_wg[*blk]
	if !ok {
		var new_wg sync.WaitGroup
		l.notify_wg[*blk] = &new_wg
		wg = &new_wg
	}

	wg.Add(1)
	defer wg.Done()
	l.method_lock.Unlock() // 挂起前, 释放方法锁;

	select {
	case <-time.After(MAX_WAITING_TIME * time.Second):
		// fmt.Printf("waitGivenTimeOut() txNum:%d need blk_%d routine wake up for timeout\n", txNum, blk.ID())
	case <-l.notify_chan[*blk]:
		// fmt.Printf("waitGivenTimeOut() txNum:%d need blk_%d routine wake up by notify close channel\n", txNum, blk.ID())
	}
	// 1. 超时自动唤醒, 加锁
	// 2. 主动唤醒, 加锁
	// 只有一个线程会加锁成功并返回, 其余则阻塞;
	// 事务 B
	l.method_lock.Lock() // 唤起后加上方法锁;
	// 获取锁成功, defer wg.Done() 一下, 有可能 wg 还未 == 0, 因为外层可能会for循环调用;
}

func (l *LockTable) notifyAll(blk *fm.BlockIndex, txNum int32) {
	// fmt.Printf("notifyAll() txNum:%d will close channle for blk :%v \n", txNum, *blk)
	channel, ok := l.notify_chan[*blk]
	if ok {
		// 唤醒 所有在等待这个 block 的协程; 但是只有一个协程会获得锁并返回;
		close(channel)
		delete(l.notify_chan, *blk)
		// mark := rand.Intn(10000)
		// fmt.Printf("delete blk:%v and launch rotinue to create it, mark: %d\n", *blk, mark)

		// 另外再起一个协程, 在当前blk有关的所有协程返回后, 再为blk创建一个新管道;
		//go func(blk_unlock fm.BlockIndex, ran_num int) {
		//	// 等待所有线程返回后再重新设置 channel,
		//	// 注意这个线程不一定得到及时调度, 因此可能不能及时创建channel对象从而导致close closed channel panic
		//	fmt.Printf("txNum:%d wait group for blk: %v, with mark:%d\n", txNum, blk_unlock, ran_num)
		//	// 可能会一直阻塞等待;
		//	l.notify_wg[blk_unlock].Wait()
		//	// 访问内部数据时需要加锁
		//	l.method_lock.Lock()
		//	l.notify_chan[blk_unlock] = make(chan struct{})
		//	l.method_lock.Unlock()
		//	fmt.Printf("txNum:%d create notify channel for %v\n", txNum, blk_unlock)
		//}(*blk, mark)
	} else {
		// fmt.Printf("channel for %v is already closed,  txNum:%d exit;\n", *blk, txNum)
	}
}

func NewLockTable() *LockTable {
	/*
		如果给定blk对应的值为-1，表明有互斥锁,如果大于0表明有相应数量的共享锁加在对应区块上，
		如果是0则表示没有锁
	*/
	lock_table := &LockTable{
		lock_map:    make(map[fm.BlockIndex]int64),
		notify_chan: make(map[fm.BlockIndex]chan struct{}),
		notify_wg:   make(map[fm.BlockIndex]*sync.WaitGroup),
	}

	return lock_table
}

func (l *LockTable) initWaitingOnBlk(blk *fm.BlockIndex) {
	_, ok := l.notify_chan[*blk]
	if !ok {
		l.notify_chan[*blk] = make(chan struct{})
	}

	_, ok = l.notify_wg[*blk]
	if !ok {
		l.notify_wg[*blk] = &sync.WaitGroup{}
	}
}

// S S 直接走了
// S X(阻塞)
// X S(阻塞)
// X X

func (l *LockTable) SLock(blk *fm.BlockIndex, txNum int32) error {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()

	l.initWaitingOnBlk(blk) // 初始化等待给定区块的管道, 以及group;

	start := time.Now()

	// 循环等待-获取-锁
	// 如果当前blk块被加上X锁 && 锁等待时间没有达到最大时长 => 继续循环等待其 X锁 被释放;
	for l.hasXlock(blk) && !l.waitingTooLong(start) {
		// 阻塞 等待 获得锁;
		// fmt.Printf("txNum:%d need blk_%d, get slock fail and sleep...\n", txNum, blk.ID())
		l.waitGivenTimeOut(blk, txNum)
		// waitGivenTimeOut() 返回时会 l.method_lock.Lock(), 和第2行解锁向呼应
	}

	// fmt.Printf("waitGivenTimeOut() txNum:%d get blk_%d lock success\n", txNum, blk.ID())

	// 如果等待过长时间, 有可能是产生了死锁;
	if l.hasXlock(blk) {
		// fmt.Printf("txNum:%d need blk_%d, slock fail for xlock \n", txNum, blk.ID())
		return errors.New("SLock Exception: XLock on given blk")
	}

	val := l.getLockVal(blk)
	l.lock_map[*blk] = val + 1
	return nil
}

func (l *LockTable) XLock(blk *fm.BlockIndex, txNum int32) error {
	// 事务 C
	l.method_lock.Lock()
	defer l.method_lock.Unlock()
	l.initWaitingOnBlk(blk)

	start := time.Now()

	// 如果还有其他S锁，当前X锁, 则等待;
	//for l.hasOtherSLocks(blk) && !l.waitingTooLong(start) {
	for (l.hasOtherSLocks(blk) || l.hasXlock(blk)) && !l.waitingTooLong(start) {
		// fmt.Printf("txNum:%d need blk_%d, get xlock fail and sleep...\n", txNum, blk.ID())
		l.waitGivenTimeOut(blk, txNum)
	}

	// fmt.Printf("waitGivenTimeOut() txNum:%d get blk_%d lock success\n", txNum, blk.ID())

	if l.hasOtherSLocks(blk) {
		// fmt.Printf("err: txNum:%d need blk_%d, get xlock fail and return...\n", txNum, blk.ID())
		return errors.New("XLock error: SLock on given blk")
	}

	// 如果等待过长时间，有可能是产生了死锁;
	if l.hasXlock(blk) {
		// fmt.Printf("txNum:%d need blk_%d, slock fail for xlock\n", txNum, blk.ID())
		return errors.New("XLock error: XLock on given blk")
	}

	// -1表示区块被加上互斥锁
	l.lock_map[*blk] = -1
	// fmt.Printf("txNum:%d, blk_:%d ,l.lock_map:%v \n", txNum, blk.ID(), l.lock_map)
	return nil
}

func (l *LockTable) UnLock(blk *fm.BlockIndex, txNum int32) {
	l.method_lock.Lock()
	defer l.method_lock.Unlock()

	val := l.getLockVal(blk)
	if val > 1 {
		l.lock_map[*blk] = val - 1
	} else {
		delete(l.lock_map, *blk)
		// 通知所有等待给定区块的线程从 select 中恢复;
		// fmt.Printf("txNum:%d unlock by blk: +%v \n", txNum, *blk)
		// 全局通知其他协程;
		l.notifyAll(blk, txNum)
	}
}

func (l *LockTable) hasXlock(blk *fm.BlockIndex) bool {
	return l.getLockVal(blk) < 0
}

func (l *LockTable) hasOtherSLocks(blk *fm.BlockIndex) bool {
	return l.getLockVal(blk) >= 1
}

func (l *LockTable) waitingTooLong(start time.Time) bool {
	elapsed := time.Since(start).Seconds()
	if elapsed >= MAX_WAITING_TIME {
		return true
	}
	return false
}

func (l *LockTable) getLockVal(blk *fm.BlockIndex) int64 {
	val, ok := l.lock_map[*blk]
	if !ok {
		l.lock_map[*blk] = 0
		return 0
	}
	return val
}
