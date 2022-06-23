package buffer_manager

import (
	fmgr "simpleDb/file_manager"
	lmgr "simpleDb/log_manager"
)

type Buffer struct {
	fm       *fmgr.FileManager
	lm       *lmgr.LogManager
	contents *fmgr.Page
	blk      *fmgr.BlockId
	pins     uint32 // 引用计数
	txNum    int32  // 事务号
	lsn      uint64 // 日志号
}

func NewBuffer(fm *fmgr.FileManager, lm *lmgr.LogManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		txNum:    -1,
		lsn:      0,
		contents: fmgr.NewPageBySize(fm.BlockSize()),
	}
}

func (b *Buffer) Contents() *fmgr.Page {
	return b.contents
}

func (b *Buffer) Block() *fmgr.BlockId {
	return b.blk
}

func (b *Buffer) SetModified(txNum int32, lsn uint64) {
	// 如果上层组件修改了缓存数据，必须调用这个接口进行通知
	b.txNum = txNum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	// 返回当前缓存数据是否在被使用
	return b.pins > 0
}

func (b *Buffer) ModifyingTx() int32 {
	return b.txNum
}

func (b *Buffer) AssignToBlock(block *fmgr.BlockId) {
	// 将指定的文件区块号的内容读到buffer中
	b.Flush() // 当页面读取其他数据时，先当前数据写入磁盘
	b.blk = block
	b.fm.Read(b.blk, b.Contents()) // 将对应的磁盘区块数据读入到缓存中
	b.pins = 0
}

func (b *Buffer) Flush() {
	if b.txNum > 0 {
		b.lm.FlushByLSN(b.lsn)          // 为系统崩溃恢复提供支持
		b.fm.Write(b.blk, b.Contents()) // 将已经修改的数据写入到磁盘
		b.txNum = -1
	}
}

func (b *Buffer) Pin() {
	b.pins = b.pins + 1
}
func (b *Buffer) Unpin() {
	b.pins = b.pins - 1
}
