package tx

import (
	"encoding/binary"
	"fmt"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/stretchr/testify/require"

	"github.com/kebukeYi/TrainSQL/util"
	"testing"
)

func TestStartRecord(t *testing.T) {
	util.ClearDir(util.TxTestDirectory)
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "start_record")

	tx_num := uint64(13) // 交易号
	p := fm.NewPageBySize(32)
	// <start 12>
	p.SetInt(0, uint64(START))
	p.SetInt(8, tx_num)

	start_record := NewStartRecord(p, log_manager)
	expected_str := fmt.Sprintf("<START %d>", tx_num)
	require.Equal(t, expected_str, start_record.ToString())

	_, err := start_record.WriteToLog()
	require.Nil(t, err)

	iter := log_manager.Iterator()
	// 检查写入的日志是否符号预期
	rec := iter.Next()
	rec_op := binary.LittleEndian.Uint64(rec[0:8])
	rec_tx_num := binary.LittleEndian.Uint64(rec[8:len(rec)])
	require.Equal(t, rec_op, uint64(START))
	require.Equal(t, rec_tx_num, tx_num)
}

func TestSetStringRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	// logFile 记录的是 dataFile 的改动记录;
	log_manager, _ := lm.NewLogManager(file_manager, "set_string_record")

	str := "original string"
	blk := uint64(1)
	// dataFile 记录数据;
	dummy_blk := fm.NewBlockIndex("dummy_id", blk)
	tx_num := uint64(1)
	offset := uint64(13)

	// 写入用于恢复的日志
	WriteSetStringLog(log_manager, tx_num, dummy_blk, offset, str)

	pp := fm.NewPageBySize(400)
	pp.SetString(offset, str)

	// 读取日志
	iter := log_manager.Iterator()
	rec := iter.Next()
	log_p := fm.NewPageByBytes(rec)
	setStrRec := NewSetStringRecord(log_p)
	expectd_str := fmt.Sprintf("<SETSTRING %d %d %d %s>", tx_num, blk, offset, str)
	require.Equal(t, expectd_str, setStrRec.ToString())

	pp.SetString(offset, "modify string 1")
	pp.SetString(offset, "modify string 2")

	txStub := NewTxStub(pp)
	setStrRec.Undo(txStub)
	recover_str := pp.GetString(offset)

	require.Equal(t, recover_str, str)
}

func TestSetIntRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "set_int_record")

	val := uint64(11)
	blk := uint64(1)
	dummy_blk := fm.NewBlockIndex("dummy_id", blk)
	tx_num := uint64(1)
	offset := uint64(13)

	//写入用于恢复的日志
	WriteSetIntLog(log_manager, tx_num, dummy_blk, offset, val)

	pp := fm.NewPageBySize(400)
	pp.SetInt(offset, val)
	iter := log_manager.Iterator()
	rec := iter.Next()
	log_p := fm.NewPageByBytes(rec)
	setIntRec := NewSetIntRecord(log_p)
	expectd_str := fmt.Sprintf("<SETINT %d %d %d %d>", tx_num, blk, offset, val)

	require.Equal(t, expectd_str, setIntRec.ToString())

	pp.SetInt(offset, 22)
	pp.SetInt(offset, 33)
	txStub := NewTxStub(pp)
	setIntRec.Undo(txStub)
	recover_val := pp.GetInt(offset)

	require.Equal(t, recover_val, val)
}

func TestRollBackRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "rollback_record")
	tx_num := uint64(13)
	WriteRollBackLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewRollBackRecord(pp)
	expected_str := fmt.Sprintf("<ROLLBACK %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}

func TestCommitRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "commit_record")
	tx_num := uint64(13)
	WriteCommitRecordLog(log_manager, tx_num)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)

	roll_back_rec := NewCommitkRecordRecord(pp)
	expected_str := fmt.Sprintf("<COMMIT %d>", tx_num)

	require.Equal(t, expected_str, roll_back_rec.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	file_manager, _ := fm.NewFileManager(util.TxTestDirectory, 400)
	log_manager, _ := lm.NewLogManager(file_manager, "checkpoint_record")
	WriteCheckPointToLog(log_manager)
	iter := log_manager.Iterator()
	rec := iter.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	check_point_rec := NewCheckPointRecord()
	expected_str := "<CHECKPOINT>"
	require.Equal(t, expected_str, check_point_rec.ToString())
}
