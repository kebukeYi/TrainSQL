package tx

import (
	"fmt"
	bm "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/util"
	"testing"
	"time"
)

// 事务A 睡0秒 读blk1 睡3秒 读blk2 提交
// 事务B 睡1秒 写blk2 睡1秒 读blk1 提交
// 事务C 睡1秒 读blk3 释放3 写blk1 睡1秒 读blk2 提交
func TestCurrencyManager(_ *testing.T) {
	util.ClearDir(util.CCTestDirectory)
	file_manager, _ := fm.NewFileManager(util.CCTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	// 容量只有2个缓存页页, 那么获取第三个页时,就会阻塞等待....
	//buffer_manager := bm.NewBufferManager(file_manager, log_manager, 2)
	// 3个就顺利通过;
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

	go func() {
		txA := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A: request slock 1")
		txA.GetInt(blk1, 0) //如果返回错误，我们应该放弃执行下面操作并执行回滚，这里为了测试而省略
		fmt.Println("Tx A: receive slock 1")
		time.Sleep(3 * time.Second)
		fmt.Println("Tx A: request slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive slock 2")
		fmt.Println("Tx A: Commit")
		txA.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)
		txB := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		txB.Pin(blk1) // 先加载过来
		txB.Pin(blk2) // 加载过来
		fmt.Println("Tx B: request xlock 2")
		txB.SetInt(blk2, 0, 0, false)
		fmt.Println("Tx B: receive xlock 2")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx B: request slock 1")
		txB.GetInt(blk1, 0)
		fmt.Println("Tx B: receive slock 1")
		fmt.Println("Tx B: Commit")
		txB.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)
		txC := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		blk3 := fm.NewBlockIndex("testfile", 3)
		txC.Pin(blk3)
		go func() {
			time.Sleep(4 * time.Second)
			txC.UnPin(blk3)
		}()
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C: request xlock 1")
		txC.SetInt(blk1, 0, 0, false)
		fmt.Println("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx C: request slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive slock 2")
		fmt.Println("Tx C: Commit")
		txC.Commit()
	}()

	time.Sleep(20 * time.Second)
}

// 事务A 睡0秒 读blk1 睡4秒 读blk2 提交
// 事务B 睡1秒 写blk1 睡1秒 读blk2 提交
// 事务C 睡2秒 写blk1 睡1秒 读blk2 提交
func TestCurrencyManager2(_ *testing.T) {
	file_manager, _ := fm.NewFileManager(util.CCTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, 3)

	go func() {
		txA := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		txA.Pin(blk1)
		txA.Pin(blk2)
		fmt.Println("Tx A: request slock 1")
		txA.GetInt(blk1, 0) //如果返回错误，我们应该放弃执行下面操作并执行回滚，这里为了测试而省略
		fmt.Println("Tx A: receive slock 1")
		time.Sleep(3 * time.Second)

		fmt.Println("Tx A: request slock 2")
		txA.GetInt(blk2, 0)
		fmt.Println("Tx A: receive slock 2")
		fmt.Println("Tx A: Commit")
		txA.Commit()
	}()

	go func() {
		time.Sleep(1 * time.Second)
		txB := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		txB.Pin(blk1) // 先加载过来
		txB.Pin(blk2) // 加载过来
		fmt.Println("Tx B: request xlock 1")
		txB.SetInt(blk1, 0, 1, false)
		fmt.Println("Tx B: receive xlock 1")
		time.Sleep(1 * time.Second)
		fmt.Println("Tx B: request slock 2")
		txB.GetInt(blk2, 0)
		fmt.Println("Tx B: receive slock 2")
		fmt.Println("Tx B: Commit")
		txB.Commit()
	}()

	go func() {
		time.Sleep(2 * time.Second)
		txC := NewTransation(file_manager, log_manager, buffer_manager)
		blk1 := fm.NewBlockIndex("testfile", 1)
		blk2 := fm.NewBlockIndex("testfile", 2)
		txC.Pin(blk1)
		txC.Pin(blk2)
		fmt.Println("Tx C: request xlock 1")
		txC.SetInt(blk1, 0, 2, false)
		fmt.Println("Tx C: receive xlock 1")
		time.Sleep(1 * time.Second)

		fmt.Println("Tx C: request slock 1")
		txC.GetInt(blk1, 0)
		fmt.Println("Tx C: receive slock 1")

		fmt.Println("Tx C: request slock 2")
		txC.GetInt(blk2, 0)
		fmt.Println("Tx C: receive slock 2")

		fmt.Println("Tx C: Commit")
		txC.Commit()
	}()
	time.Sleep(20 * time.Second)
}
