package log_manager

import (
	fm "simpleDb/file_manager"
)

/*
LogIterator 用于遍历区块内的日志，日志从底部往上写，遍历从上往下读，如果当前区块记录的日志编号为1，2，3，4
存储的顺序为4，3，2，1，日志遍历器读取的顺序为4，3，2，1
*/

type LogIterator struct {
	fileManager *fm.FileManager
	blk         *fm.BlockId
	p           *fm.Page
	currentPos  uint64
	boundary    uint64
}

func NewLogIterator(fileManager *fm.FileManager, blk *fm.BlockId) *LogIterator {
	it := LogIterator{
		fileManager: fileManager,
		blk:         blk,
	}

	// 读取给定区块的数据
	it.p = fm.NewPageBySize(fileManager.BlockSize())
	err := it.moveToBlock(blk)
	if err != nil {
		return nil
	}

	return &it
}

func (it *LogIterator) moveToBlock(blk *fm.BlockId) error {
	// 从磁盘将对应的区块读入内存
	_, err := it.fileManager.Read(blk, it.p)
	if err != nil {
		return err
	}

	// 获取日志的起始地址
	it.boundary = it.p.GetInt(0)
	it.currentPos = it.boundary

	return nil
}

func (it *LogIterator) Next() []byte {
	// 编号最大的会先读取
	if it.currentPos == it.fileManager.BlockSize() {
		// 已经读完全部的数据，需要加载新的区块
		it.blk = fm.NewBlockId(it.blk.FileName(), it.blk.Number()-1)
		it.moveToBlock(it.blk)
	}

	record := it.p.GetBytes(it.currentPos)
	it.currentPos += UINT64_LEN + uint64(len(record))

	return record
}

func (it *LogIterator) HasNext() bool {
	/*
		如果当前区块数据全部读完但是区块号不是0，说明还有其他区块的数据可以读取
	*/
	return it.currentPos < it.fileManager.BlockSize() || it.blk.Number() > 0
}
