package file_manager

import (
	"crypto/sha256"
	"fmt"
)

type BlockId struct {
	fileName string // 对应磁盘中的二进制文件
	blkNum   uint64 // 二进制文件中的区块编号
}

func NewBlockId(fileName string, blkNum uint64) *BlockId {
	return &BlockId{
		fileName: fileName,
		blkNum:   blkNum,
	}
}

func (b *BlockId) FileName() string {
	return b.fileName
}
func (b *BlockId) Number() uint64 {
	return b.blkNum
}

func (b *BlockId) Equal(other *BlockId) bool {
	return b.fileName == other.fileName && b.Number() == other.blkNum
}

// 哈希方法
func asSha256(o interface{}) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%v", o)))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (b *BlockId) HashCode() string {
	return asSha256(*b)
}
