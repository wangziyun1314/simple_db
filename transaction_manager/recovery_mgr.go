package transaction_manager

import (
	bm "simpleDb/buffer_manager"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
)

type RecoveryManager struct {
	logManager    *lm.LogManager
	bufferManager *bm.BufferManager
	tx            *Transaction
	txNum         int32
}

func NewRecoveryManager(tx *Transaction, txNm int32, logManager *lm.LogManager, bufferManager *bm.BufferManager) *RecoveryManager {
	recoveryManager := &RecoveryManager{
		tx:            tx,
		txNum:         txNm,
		logManager:    logManager,
		bufferManager: bufferManager,
	}
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, uint64(txNm))
	startRecord := NewStartRecord(logManager, p)
	startRecord.WriteToLog()

	return recoveryManager
}

func (r *RecoveryManager) Commit() error {
	r.bufferManager.FlushAll(r.txNum)
	lsn, err := WriteCommitRecord(r.logManager, uint64(r.txNum))
	if err != nil {
		return err
	}
	r.logManager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) Rollback() error {
	r.doRollback()
	r.bufferManager.FlushAll(r.txNum)
	lsn, err := WriteRollBackLog(r.logManager, uint64(r.txNum))
	if err != nil {
		return err
	}
	r.logManager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) Recover() error {
	r.doRecover()
	r.bufferManager.FlushAll(r.txNum)
	lsn, err := WriteCheckPoint(r.logManager)
	if err != nil {
		return err
	}
	r.logManager.FlushByLSN(lsn)
	return nil
}

func (r *RecoveryManager) SetInt(buffer *bm.Buffer, offset uint64, newVal int64) (uint64, error) {
	oldVal := buffer.Contents().GetInt(offset)
	block := buffer.Block()
	buffer.Contents().SetInt(offset, uint64(newVal))
	return WriteSetIntLog(r.logManager, uint64(r.txNum), block, offset, oldVal)
}

func (r *RecoveryManager) SetString(buffer *bm.Buffer, offset uint64, newVal string) (uint64, error) {
	oldVal := buffer.Contents().GetString(offset)
	block := buffer.Block()
	buffer.Contents().SetString(offset, newVal)
	return WriteSetStringLog(r.logManager, uint64(r.txNum), block, offset, oldVal)
}

func (r *RecoveryManager) CreateLogRecord(bytes []byte) LogRecordInterface {
	p := fm.NewPageByBytes(bytes)
	switch RECORD_TYPE(p.GetInt(0)) {
	case CHECKPOINT:
		return NewCheckPointRecord()
	case START:
		return NewStartRecord(r.logManager, p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollBackRecord(p)
	case SETINT:
		return NewSetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		panic("unknown log interface")
	}
}

func (r *RecoveryManager) doRollback() {
	iterator := r.logManager.Iterator()
	for iterator.HasNext() {
		rec := iterator.Next()
		logRecord := r.CreateLogRecord(rec)
		if logRecord.TxNumber() == uint64(r.txNum) {
			if logRecord.Op() == START {
				return
			}
			logRecord.Undo(r.tx)
		}
	}
}

func (r *RecoveryManager) doRecover() {
	finishedTxs := make(map[uint64]bool)
	iterator := r.logManager.Iterator()
	for iterator.HasNext() {
		bytes := iterator.Next()
		logRecord := r.CreateLogRecord(bytes)
		if logRecord.Op() == CHECKPOINT {
			return
		}
		if logRecord.Op() == COMMIT || logRecord.Op() == ROLLBACK {
			finishedTxs[logRecord.TxNumber()] = true
		}
		existed := finishedTxs[logRecord.TxNumber()]
		if existed {
			logRecord.Undo(r.tx)
		}
	}
}
