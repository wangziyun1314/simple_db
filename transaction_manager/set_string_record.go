package transaction_manager

import (
	"fmt"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
)

/**
在理论上一条set_string有7个字段，例如<SETSTRING , 0, junk, 33, 12, joe, apple>
在实现的时候只用到6个字段，上面的记录实际上对应了两次字符串的写入，第一次写入字符串apple，
第二次写入joe，因此可以转换成两条包含六个字段的记录：
<SETSTRING , 0, JUNK, 33, 12, apple>
...
<SETSTRING, 0, JUNK, 33, 12, joe>
在写入日志的时候是倒着写入的，因此<SETSTRING, 0, JUNK, 33, 12, joe> 会在<SETSTRING , 0, JUNK, 33, 12, apple>
的前面，在回滚的时候我们会先读入joe，再读到apple，所以在读到相应的记录后就直接做相应的操作就可以了。
*/

type SetStringRecord struct {
	val    string
	txNum  uint64
	blk    *fm.BlockId
	offset uint64
}

func NewSetStringRecord(p *fm.Page) *SetStringRecord {
	// 获取事务号
	txPos := uint64(UINT64_LENGTH)
	txNum := p.GetInt(txPos)
	// 获取文件名
	fPos := txPos + uint64(UINT64_LENGTH)
	fileName := p.GetString(fPos)
	// 获取块号
	blkPos := fPos + p.MaxLengthForString(fileName)
	blkNum := p.GetInt(blkPos)
	// 获取偏移的位置
	offsetPos := blkPos + uint64(UINT64_LENGTH)
	offset := p.GetInt(offsetPos)
	// 获取数据
	strPos := offsetPos + uint64(UINT64_LENGTH)
	data := p.GetString(strPos)

	blk := fm.NewBlockId(fileName, blkNum)
	return &SetStringRecord{
		val:    data,
		txNum:  txNum,
		blk:    blk,
		offset: offset,
	}
}

func (s *SetStringRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (s *SetStringRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetStringRecord) ToString() string {
	str := fmt.Sprintf("<SETSTRING %d %d %d %s>", s.txNum, s.blk.Number(), s.offset, s.val)
	return str
}

func (s *SetStringRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetString(s.blk, s.offset, s.val, false) // 将原来的数据写回去
	tx.Unpin(s.blk)
}

//WriteSetStringLog 构造字符串内容的日志，SetStringRecord在构造中默认给定缓冲区中已经有了字符串信息
// 但是在初始化阶段，缓存页面可能还没有相应的日志信息，这个接口的作用就是为给定缓存写入日志内容
func WriteSetStringLog(lm *lm.LogManager, txNum uint64, blk *fm.BlockId, offset uint64, val string) (uint64, error) {
	txNumPos := uint64(UINT64_LENGTH)
	fileNamePos := uint64(txNumPos + UINT64_LENGTH)
	p := fm.NewPageBySize(1)
	blkPost := uint64(fileNamePos + p.MaxLengthForString(blk.FileName()))
	offsetPos := uint64(blkPost + UINT64_LENGTH)
	valPos := uint64(offsetPos + UINT64_LENGTH)

	recLen := uint64(valPos + p.MaxLengthForString(val))
	rec := make([]byte, recLen)
	// 将信息存到page中
	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(txNumPos, txNum)
	p.SetString(fileNamePos, blk.FileName())
	p.SetInt(blkPost, blk.Number())
	p.SetInt(offsetPos, offset)
	p.SetString(valPos, val)
	// 将记录添加到日志中
	return lm.Append(rec)
}
