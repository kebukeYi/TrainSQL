package tx

import (
	"fmt"
	fm "github.com/kebukeYi/TrainSQL/file_manager"
	lg "github.com/kebukeYi/TrainSQL/log_manager"
)

/*
在理论中, 一条SETSTRING记录有7个字段，例如:
 <SETSTRING, 0(txNum), fileName(具体文件), 33(区块), 12(位移), joe(新值), joseph(旧值)>，
在实现上我们改为只用6个字段，上面的记录实际上对应了两次字符串的写入，第一次写入字符串"joseph"，
第二次写入joe，因此在实现上它对应了两条包含六个字段的记录:
<SETSTRING, 0, junk, 33, 12, joseph>
....
<SETSTRING, 0, junk, 33, 12, joe>

回忆一下前面我们实现日志，日志是从后往前写, 非追加写, 也就是<SETSTRING, 0, junk, 33, 12, joe>会写在前面，
<SETSTRING, 0, junk, 33, 12, joseph>会写在后面,
在回滚的时候，我们从前往后读取，因此我们会先读到joe,然后读到joseph，于是执行回滚时我们只要把
读到的字符串写入到给定位置就可以，例如我们先读到joe，然后写入junk文件区块为33偏移为12的地方，
然后又读取joseph，再次将它写入到junk文件区块为33偏移为12的地方，于是就实现了回滚效果，
所以实现上SETSTRING记录不用写入7个字段，只有6个就可以;
*/

type SetStringRecord struct {
	tx_num uint64
	offset uint64
	val    string
	blk    *fm.BlockIndex
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	// <SETSTRING, 0, junk, 33, 12, joseph>
	txNum_pos := uint64(UINT64_LENGTH)
	tx_num := p.GetInt(txNum_pos)
	fileName_pos := txNum_pos + UINT64_LENGTH
	filename := p.GetString(fileName_pos)
	blockId_pos := fileName_pos + p.MaxLengthForString(filename)
	blknum := p.GetInt(blockId_pos)
	blk := fm.NewBlockIndex(filename, blknum)
	opos := blockId_pos + UINT64_LENGTH
	offset := p.GetInt(opos)
	val_pos := opos + UINT64_LENGTH
	val := p.GetString(val_pos) // 将日志中的字符串再次写入给定位置

	return &SetStringRecord{
		tx_num: tx_num,
		blk:    blk,
		offset: offset,
		val:    val,
	}
}

func (s *SetStringRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() uint64 {
	return s.tx_num
}

func (s *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %d %d %s>", s.tx_num, s.blk.ID(),
		s.offset, s.val)

	return str
}

func (s *SetStringRecord) Undo(tx TransationInterface) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false) //将原来的字符串写回去
	tx.UnPin(s.blk)
}

func WriteSetStringLog(log_manager *lg.LogManager, tx_num uint64,
	blk *fm.BlockIndex, offset uint64, val string) (uint64, error) {
	/*
		构造字符串内容的日志,SetStringRecord 在构造中, 默认给定缓存页面已经有了字符串信息,
		但是在初始状态，缓存页面可能还没有相应日志信息，这个接口的作用就是为给定缓存写入
		字符串日志, 格式: <SETSTRING, 0, junk, 33, 12, joseph>
	*/
	// 事务id 从第8字节开始;
	tpos := uint64(UINT64_LENGTH)
	fpos := uint64(tpos + UINT64_LENGTH)
	p := fm.NewPageBySize(1)
	bpos := uint64(fpos + p.MaxLengthForString(blk.FileName()))
	opos := uint64(bpos + UINT64_LENGTH)
	vpos := uint64(opos + UINT64_LENGTH)

	rec_len := uint64(vpos + p.MaxLengthForString(val))
	rec := make([]byte, rec_len)

	p = fm.NewPageByBytes(rec) // 创建新页面
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tpos, tx_num)
	p.SetString(fpos, blk.FileName())
	p.SetInt(bpos, blk.ID())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return log_manager.Append(rec)
}
