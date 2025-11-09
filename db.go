package simple_db

import (
	bm "github.com/kebukeYi/TrainSQL/buffer_manager"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lm "github.com/kebukeYi/TrainSQL/log_manager"
	"github.com/kebukeYi/TrainSQL/tx"
)

type DB struct {
	file_manager   *fm.FileManager   // 真实数据读写器;
	log_manager    *lm.LogManager    // redolog undolog 日志读写器;
	buffer_manager *bm.BufferManager // 读写缓存管理器;
}
type DBOptions struct {
	DBDirectory     string
	BlockSize       uint64
	BufferSize      uint32
	LogFilePathName string
}

func NewDBWithOptions(options *DBOptions) *DB {
	file_manager, _ := fm.NewFileManager(options.DBDirectory, options.BlockSize)
	log_manager, _ := lm.NewLogManager(file_manager, options.LogFilePathName)
	buffer_manager := bm.NewBufferManager(file_manager, log_manager, options.BufferSize)
	return NewDB(file_manager, log_manager, buffer_manager)
}

func NewDB(file_manager *fm.FileManager, log_manager *lm.LogManager,
	buffer_manager *bm.BufferManager) *DB {
	newDB := &DB{
		file_manager:   file_manager,
		log_manager:    log_manager,
		buffer_manager: buffer_manager,
	}
	// 恢复数据库;
	translation := newDB.NewTranslation()
	translation.Recover()
	translation.Close()
	return newDB
}

func (d *DB) NewTranslation() *tx.Translation {
	transation := tx.NewTransation(d.file_manager, d.log_manager, d.buffer_manager)
	transation.Start()
	return transation
}

func (d *DB) Close() error {
	return nil
}
