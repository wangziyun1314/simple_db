package buffer_manager

import (
	"errors"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
	"sync"
	"time"
)

const (
	MAX_TIME = 3 // 分配缓存最多等待的时间
)

type BufferManager struct {
	bufferPool   []*Buffer
	numAvailable uint32
	mu           sync.Mutex
}

func NewBufferManager(fm *fm.FileManager, lm *lm.LogManager, numAvailable uint32) *BufferManager {
	bufferManager := &BufferManager{
		numAvailable: numAvailable,
	}
	for i := uint32(0); i < numAvailable; i++ {
		buffer := NewBuffer(fm, lm)
		bufferManager.bufferPool = append(bufferManager.bufferPool, buffer)
	}

	return bufferManager
}

func (b *BufferManager) Available() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.numAvailable
}

func (b *BufferManager) FlushAll(txNum int32) {
	// 将给定事务的数据全部写入到磁盘
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, buffer := range b.bufferPool {
		if buffer.txNum == txNum {
			buffer.Flush()
		}
	}
}

func (b *BufferManager) Pin(blk *fm.BlockId) (*Buffer, error) {
	// 将给定磁盘的区块数据分配给缓存页面
	b.mu.Lock()
	defer b.mu.Unlock()

	start := time.Now()
	buff := b.tryPin(blk) // 尝试分配缓存
	for buff == nil && b.waitingToLong(start) == false {
		// 如果无法分配缓存页面，等待一段时间再看看有没有可用的缓存页面
		time.Sleep(MAX_TIME * time.Second)
		buff = b.tryPin(blk)
		if buff == nil {
			return nil, errors.New("no buffer available , careful for dead lock")
		}
	}
	return buff, nil
}

func (b *BufferManager) Unpin(buffer *Buffer) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if buffer == nil {
		return
	}
	buffer.Unpin()
	if !buffer.IsPinned() {
		b.numAvailable = b.numAvailable + 1
		// notifyAll() 唤醒所有等待的线程，暂时不考虑，属于并发管理器的内容
	}
}

func (b *BufferManager) waitingToLong(start time.Time) bool {
	seconds := time.Since(start).Seconds()
	if seconds >= MAX_TIME {
		return true
	}
	return false
}

func (b *BufferManager) tryPin(blk *fm.BlockId) *Buffer {
	// 首先看给定的区块是否已将再缓冲池中了
	buffer := b.findExistingBuffer(blk)
	if buffer == nil {
		// 查看是否还有可用的缓冲页面，有的话将给定磁盘块的数据写入缓存
		buffer = b.chooseUnpinBuffer()
		if buffer == nil {
			return nil
		}
		buffer.AssignToBlock(blk)
	}

	if buffer.IsPinned() == false {
		b.numAvailable = b.numAvailable - 1
	}

	buffer.Pin()
	return buffer
}

func (b *BufferManager) findExistingBuffer(blk *fm.BlockId) *Buffer {
	for _, buffer := range b.bufferPool {
		block := buffer.Block()
		if block != nil && block.Equal(blk) {
			return buffer
		}
	}
	return nil
}

func (b *BufferManager) chooseUnpinBuffer() *Buffer {
	for _, buffer := range b.bufferPool {
		if !buffer.IsPinned() {
			return buffer
		}
	}
	return nil
}
