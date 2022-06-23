package transaction_manager

import (
	"fmt"
	fm "simpleDb/file_manager"
	lg "simpleDb/log_manager"
)

type RollBackRecord struct {
	txNum uint64
}

func NewRollBackRecord(p *fm.Page) *RollBackRecord {
	return &RollBackRecord{
		txNum: p.GetInt(UINT64_LENGTH),
	}
}

func (r *RollBackRecord) Op() RECORD_TYPE {
	return ROLLBACK
}

func (r *RollBackRecord) TxNumber() uint64 {
	return r.txNum
}

func (r *RollBackRecord) Undo(tx TransactionInterface) {
	// 没有回滚操作
}

func (r *RollBackRecord) ToString() string {
	return fmt.Sprintf("<ROLLBACK %d>", r.txNum)
}

func WriteRollBackLog(logManager *lg.LogManager, txNum uint64) (uint64, error) {
	rec := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(ROLLBACK))
	p.SetInt(UINT64_LENGTH, txNum)

	return logManager.Append(rec)
}
