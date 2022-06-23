package transaction_manager

import (
	"fmt"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
)

type SetIntRecord struct {
	txNum  uint64
	offset uint64
	val    uint64
	blk    *fm.BlockId
}

func NewSetIntRecord(p *fm.Page) *SetIntRecord {
	tPos := uint64(UINT64_LENGTH)
	txNum := p.GetInt(tPos)
	fPos := tPos + UINT64_LENGTH
	fileName := p.GetString(fPos)
	bPos := fPos + p.MaxLengthForString(fileName)
	blkNum := p.GetInt(bPos)
	blk := fm.NewBlockId(fileName, blkNum)
	opos := bPos + UINT64_LENGTH
	offset := p.GetInt(opos)
	vPos := opos + UINT64_LENGTH
	val := p.GetInt(vPos)

	return &SetIntRecord{
		txNum:  txNum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

func (s *SetIntRecord) Op() RECORD_TYPE {
	return SETSTRING
}

func (s *SetIntRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *SetIntRecord) ToString() string {
	str := fmt.Sprintf("<SETINT %d %d %d %d>", s.txNum, s.blk.Number(), s.offset, s.val)
	return str
}

func (s *SetIntRecord) Undo(tx TransactionInterface) {
	tx.Pin(s.blk)
	tx.SetInt(s.blk, s.offset, int64(s.val), false) // 将原来的字符串写回去
	tx.Unpin(s.blk)
}

func WriteSetIntLog(logManager *lm.LogManager, txNum uint64, blk *fm.BlockId, offset uint64, val uint64) (uint64, error) {
	tPos := uint64(UINT64_LENGTH)
	fPos := uint64(tPos + UINT64_LENGTH)
	p := fm.NewPageBySize(1)
	bPos := uint64(fPos + p.MaxLengthForString(blk.FileName()))
	oPos := uint64(bPos + UINT64_LENGTH)
	vPos := uint64(oPos + UINT64_LENGTH)
	recLen := uint64(vPos + UINT64_LENGTH)
	rec := make([]byte, recLen)

	p = fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(SETSTRING))
	p.SetInt(tPos, txNum)
	p.SetString(fPos, blk.FileName())
	p.SetInt(bPos, blk.Number())
	p.SetInt(oPos, offset)
	p.SetInt(vPos, val)

	return logManager.Append(rec)
}
