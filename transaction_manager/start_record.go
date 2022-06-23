package transaction_manager

import (
	"fmt"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
)

type StartRecord struct {
	txNum      uint64
	logManager *lm.LogManager
}

func NewStartRecord(lm *lm.LogManager, p *fm.Page) *StartRecord {
	// p 的开头8字节为事务的类型，之后是事务的id
	txNum := p.GetInt(UINT64_LENGTH)
	return &StartRecord{
		txNum:      txNum,
		logManager: lm,
	}
}

func (s *StartRecord) Op() RECORD_TYPE {
	return START
}

func (s *StartRecord) TxNumber() uint64 {
	return s.txNum
}

func (s *StartRecord) Undo(tx TransactionInterface) {
	// 什么也不做
}

func (s *StartRecord) ToString() string {
	str := fmt.Sprintf("<START %d>", s.txNum)
	return str
}

func (s *StartRecord) WriteToLog() (uint64, error) {
	// 日志写入的是二进制数据
	bytes := make([]byte, 2*UINT64_LENGTH)
	p := fm.NewPageByBytes(bytes)
	p.SetInt(uint64(0), uint64(START))
	p.SetInt(UINT64_LENGTH, s.txNum)
	return s.logManager.Append(bytes)
}
