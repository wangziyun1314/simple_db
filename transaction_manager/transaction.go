package transaction_manager

import (
	"errors"
	"fmt"
	bm "simpleDb/buffer_manager"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
	"sync"
)

var txNumMu sync.Mutex
var nextTxNum = int32(0)

func NextTxNum() int32 {
	txNumMu.Lock()
	defer txNumMu.Unlock()
	nextTxNum = nextTxNum + 1
	return nextTxNum
}

type Transaction struct {
	//同步管理器
	recoveryManager *RecoveryManager
	fileManager     *fm.FileManager
	logManager      *lm.LogManager
	bufferManager   *bm.BufferManager
	myBuffers       *BufferList
	txNum           int32
}

func NewTransaction(fileManager *fm.FileManager, logManage *lm.LogManager, bufferManager *bm.BufferManager) *Transaction {
	txNum := NextTxNum()
	tx := &Transaction{
		fileManager:   fileManager,
		logManager:    logManage,
		bufferManager: bufferManager,
		myBuffers:     NewBufferList(bufferManager),
		txNum:         txNum,
	}
	tx.recoveryManager = NewRecoveryManager(tx, txNum, logManage, bufferManager)
	return tx
}

func (t *Transaction) Commit() {
	// 调用恢复管理器执行commit
	t.recoveryManager.Commit()
	fmt.Println(fmt.Sprintf("transaction %d commited", t.txNum))
	// 释放同步管理器
	t.myBuffers.UnpinAll()
}

func (t *Transaction) Rollback() {
	t.recoveryManager.Rollback()
	fmt.Println(fmt.Sprintf("transaction %d roll back", t.txNum))
	// 释放同步管理器
	t.myBuffers.UnpinAll()
}

func (t *Transaction) Recover() {
	// 系统启动时会在所有的交易执行之前执行该函数
	t.bufferManager.FlushAll(t.txNum)
	t.recoveryManager.Recover()
}

func (t *Transaction) Pin(blk *fm.BlockId) {
	t.myBuffers.Pin(blk)
}

func (t *Transaction) Unpin(blk *fm.BlockId) {
	t.myBuffers.Unpin(blk)
}

func (t *Transaction) bufferNotExist(blk *fm.BlockId) error {
	errMessage := fmt.Sprintf("no buffer found for given blk : %d with file name : %s\n", blk.Number(), blk.FileName())
	return errors.New(errMessage)
}

func (t *Transaction) GetInt(blk *fm.BlockId, offset uint64) (uint64, error) {
	// 调用同步管理器加锁
	buffer := t.myBuffers.GetBuffer(blk)
	if buffer == nil {
		return uint64(0), t.bufferNotExist(blk)
	}
	return buffer.Contents().GetInt(offset), nil
}

func (t *Transaction) GetString(blk *fm.BlockId, offset uint64) (string, error) {
	// 调用同步管理器加锁
	buffer := t.myBuffers.GetBuffer(blk)
	if buffer == nil {
		return "", t.bufferNotExist(blk)
	}
	return buffer.Contents().GetString(offset), nil
}

func (t *Transaction) SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error {
	// 调用同步管理器加锁
	buffer := t.myBuffers.GetBuffer(blk)
	if buffer == nil {
		return t.bufferNotExist(blk)
	}
	lsn := uint64(0)
	var err error
	if okToLog {
		lsn, err = t.recoveryManager.SetInt(buffer, offset, val)
		if err != nil {
			return err
		}
	}
	p := buffer.Contents()
	p.SetInt(offset, uint64(val))
	buffer.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error {
	// 调用同步管理器加锁
	buffer := t.myBuffers.GetBuffer(blk)
	if buffer == nil {
		return t.bufferNotExist(blk)
	}
	lsn := uint64(0)
	var err error
	if okToLog {
		lsn, err = t.recoveryManager.SetString(buffer, offset, val)
		if err != nil {
			return err
		}
	}
	p := buffer.Contents()
	p.SetString(offset, val)
	buffer.SetModified(t.txNum, lsn)
	return nil
}

func (t *Transaction) Size(fileName string) uint64 {
	// 调用同步器加锁
	size, _ := t.fileManager.Size(fileName)
	return size
}

func (t *Transaction) Append(fileName string) *fm.BlockId {
	blk, err := t.fileManager.Append(fileName)
	if err != nil {
		return nil
	}
	return blk
}

func (t *Transaction) BlockSize() uint64 {
	return t.fileManager.BlockSize()
}

func (t *Transaction) AvailableBuffers() uint64 {
	return uint64(t.bufferManager.Available())
}
