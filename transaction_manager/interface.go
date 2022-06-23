package transaction_manager

import fm "simpleDb/file_manager"

type TransactionInterface interface {
	Commit()
	Rollback()
	Recover()
	Pin(blk *fm.BlockId)
	Unpin(blk *fm.BlockId)
	GetInt(blk *fm.BlockId, offset uint64) (uint64, error)
	GetString(blk *fm.BlockId, offset uint64) (string, error)
	SetInt(blk *fm.BlockId, offset uint64, val int64, okToLog bool) error
	SetString(blk *fm.BlockId, offset uint64, val string, okToLog bool) error
	AvailableBuffers() uint64
	Size(filename string) uint64
	Append(filename string) *fm.BlockId
	BlockSize() uint64
}

type RECORD_TYPE uint64

const (
	CHECKPOINT RECORD_TYPE = iota
	START
	COMMIT
	ROLLBACK
	SETINT
	SETSTRING
)

const (
	UINT64_LENGTH = 8
	END_OF_FILE   = -1
)

type LogRecordInterface interface {
	Op() RECORD_TYPE              // 返回记录的类别
	TxNumber() uint64             // 返回事务的id
	Undo(tx TransactionInterface) // 回滚操作
	ToString() string             // 获得记录的字符串内容
}
