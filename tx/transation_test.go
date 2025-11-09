package tx

import (
	"fmt"
	bmg "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTranslationRollBack(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "logfile")
	buffer_manager := bmg.NewBufferManager(file_manager, log_manager, 3)

	tx1 := NewTransation(file_manager, log_manager, buffer_manager)
	blk := fm.NewBlockIndex("testfile", 1)

	tx1.Pin(blk) // 锁定块
	// 设置log为false，因为一开始数据没有任何意义，因此不能进行日志记录
	err := tx1.SetInt(blk, 80, 1, false)
	require.Nil(t, err)
	err = tx1.SetString(blk, 40, "one", false)
	require.Nil(t, err)
	tx1.Commit() // 执行回滚操作后，数据会还原到这里写入的内容

	tx2 := NewTransation(file_manager, log_manager, buffer_manager)
	tx2.Pin(blk)
	ival, _ := tx2.GetInt(blk, 80)
	sval, _ := tx2.GetString(blk, 40)
	require.Equal(t, ival, int64(1))
	require.Equal(t, sval, "one")

	new_ival := ival + 1   // 2
	new_sval := sval + "!" // one!
	tx2.SetInt(blk, 80, new_ival, true)
	tx2.SetString(blk, 40, new_sval, true)
	tx2.Commit() // 尝试写入新的数据

	tx3 := NewTransation(file_manager, log_manager, buffer_manager)
	tx3.Pin(blk)
	ival, _ = tx3.GetInt(blk, 80)
	sval, _ = tx3.GetString(blk, 40)
	require.Equal(t, ival, int64(2))
	require.Equal(t, sval, "one!")

	tx3.SetInt(blk, 80, 999, true)
	ival, _ = tx3.GetInt(blk, 80)
	// 写入数据后检查是否写入正确
	require.Equal(t, ival, int64(999))

	tx3.RollBack() // 执行回滚操作, 并确定回滚到第一次写入内容;

	tx4 := NewTransation(file_manager, log_manager, buffer_manager)
	tx4.Pin(blk)
	ival, _ = tx4.GetInt(blk, 80)
	// require.Equal(t, ival, int64(1))
	// require.Equal(t, ival, int64(2))
	sval, _ = tx4.GetString(blk, 40)
	// require.Equal(t, sval, "one!")
	fmt.Println(ival, sval)
	tx4.Commit() //执行到这里时，输出内容应该与第一次写入内容相同;
}
