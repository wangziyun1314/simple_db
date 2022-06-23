package transaction_manager

import (
	"fmt"
	fm "simpleDb/file_manager"
	lg "simpleDb/log_manager"
)

type CommitRecord struct {
	txNum uint64
}

func NewCommitRecord(p *fm.Page) *CommitRecord {
	return &CommitRecord{
		txNum: p.GetInt(UINT64_LENGTH),
	}
}

func (c *CommitRecord) Op() RECORD_TYPE {
	return COMMIT
}

func (c *CommitRecord) TxNumber() uint64 {
	return c.txNum
}

func (c *CommitRecord) Undo(tx TransactionInterface) {
	// 没有回滚操作
}

func (c *CommitRecord) ToString() string {
	return fmt.Sprintf("<COMMIT %d>", c.txNum)
}

func WriteCommitRecord(logManager *lg.LogManager, txNum uint64) (uint64, error) {
	rec := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(COMMIT))
	p.SetInt(UINT64_LENGTH, txNum)

	return logManager.Append(rec)
}
