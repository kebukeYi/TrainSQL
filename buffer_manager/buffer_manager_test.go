package buffer_manager

import (
	"fmt"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/util"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBufferManager(t *testing.T) {
	util.ClearDir(util.BufferManageTestDirectory)
	file_manager, _ := fm.NewFileManager(util.BufferManageTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	bm := NewBufferManager(file_manager, log_manager, 3) // 只有3个缓存区页;

	buff1, err := bm.Pin(fm.NewBlockIndex("testfile", 1)) //这块缓存区在后面会被写入磁盘
	require.Nil(t, err)

	p := buff1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buff1.SetModified(1, 0) //这里两个参数先不要管
	// buff1.Unpin()

	buff2, err := bm.Pin(fm.NewBlockIndex("testfile", 2))
	require.Nil(t, err)
	_, err = bm.Pin(fm.NewBlockIndex("testfile", 3))
	require.Nil(t, err)

	buff1.Unpin()
	// 下面的bm.pin()会得到 buff1的脏缓存页;
	// 并会让缓存管理区将buff1的脏页数据写入磁盘, 随后再读出 块-4 的数据到内存buff1的页中;
	_, err = bm.Pin(fm.NewBlockIndex("testfile", 4))
	require.Nil(t, err)

	// 开启另外一个协程, 模仿事务, 延迟释放 buff2 缓存页;
	go func() {
		time.Sleep(3000 * time.Millisecond)
		bm.Unpin(buff2)
		fmt.Println("buff2 unpin()")
	}()

	// 此时3个缓存页, 2 3 4 都被使用, 因此当前pin(块-1) 会进入阻塞状态, 等待其他页面被释放;
	// 直到4秒后, buff2 被unpin(), 随后 块-1 被再次读取到内存中;
	buff11, err := bm.Pin(fm.NewBlockIndex("testfile", 1))
	require.Nil(t, err)

	// 在内存中修改 块-1 的数据, 但是没有触发刷盘;
	p11 := buff11.Contents()
	p11.SetInt(80, 9999)
	buff11.SetModified(1, 0)
	bm.Unpin(buff11) // 注意这里不会将buff11的数据写入磁盘;

	// 将 testfile 的区块1再次读入，并确认buff1的数据的确写入磁盘中了;
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockIndex("testfile", 1)
	file_manager.Read(b1, page)
	n1 := page.GetInt(80)
	require.Equal(t, n+1, n1)

	time.Sleep(50 * time.Second)
}
