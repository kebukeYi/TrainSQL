package log_manager

import (
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	"sync"
)

const (
	UINT64_LEN = 8
)

type LogManager struct {
	file_manager   *fm.FileManager // 日志系统的底层文件管理器;
	log_file       string
	log_page       *fm.Page       // 当前日志页, 非数据页;
	current_blk    *fm.BlockIndex // 当前日志页的磁盘区块;
	latest_lsn     uint64         // 当前日志序列号; log_serial_number
	last_saved_lsn uint64         // 上次存储到磁盘的日志序列号;
	mu             sync.Mutex     // 并发安全;
}

func NewLogManager(file_manager *fm.FileManager, log_file string) (*LogManager, error) {
	log_mgr := LogManager{
		file_manager:   file_manager,
		log_file:       log_file,
		log_page:       fm.NewPageBySize(file_manager.BlockSize()),
		last_saved_lsn: 0,
		latest_lsn:     0,
	}

	blockNums, err := file_manager.BlockNums(log_file)
	if err != nil {
		return nil, err
	}

	if blockNums == 0 { // 如果文件为空则添加新区块;
		blk, err := log_mgr.appendNewBlock()
		if err != nil {
			return nil, err
		}
		log_mgr.current_blk = blk
	} else { // 文件有数据, 则将文件末尾的区块读入内存, 最新的日志总会存储在文件末尾;
		log_mgr.current_blk = fm.NewBlockIndex(log_mgr.log_file, blockNums-1)
		file_manager.Read(log_mgr.current_blk, log_mgr.log_page)
	}
	return &log_mgr, nil
}

func (l *LogManager) appendNewBlock() (*fm.BlockIndex, error) {
	// logFile 中没有数据，则生成新区块;
	blk, err := l.file_manager.CreatBlockIndex(l.log_file)
	if err != nil {
		return nil, err
	}
	/*
		添加日志时, 在内存page的后部往前走; 例如内存400字节，日志100字节，那么
		日志将存储在内存的300到400字节处，因此我们需要把当前内存可用底部偏移
		写入page头8个字节,并每次写入数据后,更新此值; 逆序写;
		读取时, 正序读, 从page头8字节开始读取最新日志位移;
	*/
	// [ 位移值 ; 4 ; 3 ; 2 ; 1]
	writableOffset := l.file_manager.BlockSize()
	l.log_page.SetInt(0, writableOffset)
	l.file_manager.Write(&blk, l.log_page)
	return &blk, nil
}

func (l *LogManager) FlushByLSN(log_serial_number uint64) error {
	/*
		将给定编号及其之前的日志写入磁盘, 注意这里会把与给定日志在同一个区块, 也就是Page中的
		日志也写入磁盘。例如调用FlushLSN(65)表示把编号65及其之前的日志写入磁盘, 如果编号为
		66,67的日志也跟65在同一个Page里, 那么它们也会被写入磁盘;
	*/
	if log_serial_number > l.last_saved_lsn {
		err := l.Flush()
		if err != nil {
			return err
		}
		l.last_saved_lsn = log_serial_number
	}
	return nil
}

func (l *LogManager) Flush() error {
	// 将当前 内存page数据写入指定 blockId 磁盘区块;
	_, err := l.file_manager.Write(l.current_blk, l.log_page)
	if err != nil {
		return err
	}
	return nil
}

func (l *LogManager) Append(log_record []byte) (uint64, error) {
	// 追加日志
	l.mu.Lock()
	defer l.mu.Unlock()
	// 获得 log模块的page页面的可写空间位移处;
	writableOffset := l.log_page.GetInt(0) // 获得可写入的底部偏移
	record_size := uint64(len(log_record)) // 记录字节长度
	bytes_need := record_size + UINT64_LEN
	var err error
	// 剩余空间不足;
	if int(writableOffset-bytes_need) < (UINT64_LEN) {
		// 当前 page 容量不够, 先将当前page日志写入磁盘;
		err = l.Flush()
		if err != nil {
			return l.latest_lsn, err
		}
		// 生成新区块用于写新数据;
		l.current_blk, err = l.appendNewBlock()
		if err != nil {
			return l.latest_lsn, err
		}
		writableOffset = l.log_page.GetInt(0)
	}

	record_pos := writableOffset - bytes_need   // 我们从后部往前写入;
	l.log_page.SetBytes(record_pos, log_record) // 设置下次可以写入的位置;
	l.log_page.SetInt(0, record_pos)            // 覆盖更新 当前页面 空闲位置;
	l.latest_lsn += 1                           // 记录新加入日志的编号;
	return l.latest_lsn, err
}

func (l *LogManager) Iterator() *LogIterator {
	// 先刷盘;
	l.Flush()
	// 再生成日志遍历器;
	// l.current_blk 默认就是文件的最后一个 block 块;
	return NewLogIterator(l.file_manager, l.current_blk)
}
