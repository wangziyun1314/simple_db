package transaction_manager

import (
	"math"
	fm "simpleDb/file_manager"
	lg "simpleDb/log_manager"
)

type CheckPointRecord struct {
}

func NewCheckPointRecord() *CheckPointRecord {
	return &CheckPointRecord{}
}

func (c *CheckPointRecord) Op() RECORD_TYPE {
	return CHECKPOINT
}

func (c *CheckPointRecord) TxNumber() uint64 {
	return math.MaxUint64 // 没有对应的事务id
}

func (c *CheckPointRecord) Undo(tx TransactionInterface) {

}

func (c *CheckPointRecord) ToString() string {
	return "<CHECKPOINT>"
}

func WriteCheckPoint(logManager *lg.LogManager) (uint64, error) {
	rec := make([]byte, UINT64_LENGTH)
	p := fm.NewPageByBytes(rec)
	p.SetInt(0, uint64(CHECKPOINT))
	return logManager.Append(rec)
}
