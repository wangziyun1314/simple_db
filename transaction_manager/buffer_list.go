package transaction_manager

import (
	bm "simpleDb/buffer_manager"
	fm "simpleDb/file_manager"
)

/**
用来记录或者查询当前被pin的内存网页
*/

type BufferList struct {
	buffers       map[*fm.BlockId]*bm.Buffer
	bufferManager *bm.BufferManager
	pins          []*fm.BlockId
}

func NewBufferList(bufferManager *bm.BufferManager) *BufferList {
	return &BufferList{
		buffers:       make(map[*fm.BlockId]*bm.Buffer),
		bufferManager: bufferManager,
		pins:          make([]*fm.BlockId, 0),
	}
}

func (b *BufferList) GetBuffer(bkl *fm.BlockId) *bm.Buffer {
	buffer := b.buffers[bkl]
	return buffer
}

func (b *BufferList) Pin(blk *fm.BlockId) error {
	// 一旦一个内存页被pin后，将其加入到map中进行追踪管理
	buffer, err := b.bufferManager.Pin(blk)
	if err != nil {
		return err
	}
	b.buffers[blk] = buffer
	b.pins = append(b.pins, blk)
	return nil
}

func (b *BufferList) Unpin(blk *fm.BlockId) {
	buffer, ok := b.buffers[blk]
	if !ok {
		return
	}

	b.bufferManager.Unpin(buffer)
	for i, pinnedBlock := range b.pins {
		if pinnedBlock == blk {
			b.pins = append(b.pins[:i], b.pins[i+1:]...)
			break
		}
	}
	delete(b.buffers, blk)
}

func (b *BufferList) UnpinAll() {
	for _, blk := range b.pins {
		buffer := b.buffers[blk]
		b.bufferManager.Unpin(buffer)
	}
	b.buffers = make(map[*fm.BlockId]*bm.Buffer)
	b.pins = make([]*fm.BlockId, 0)
}
