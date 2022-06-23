package file_manager

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFileManager_Append(t *testing.T) {
	fileManager, _ := NewFileManager("file_test", 400)

	blockId := NewBlockId("testFile", 2)
	p1 := NewPageBySize(fileManager.BlockSize())
	pos1 := uint64(88)
	s := "abcdefghijklm"
	p1.SetString(pos1, s)
	size := p1.MaxLengthForString(s)

	pos2 := pos1 + size
	val := uint64(345)
	p1.SetInt(pos2, val)

	fileManager.Write(blockId, p1)

	p2 := NewPageBySize(fileManager.BlockSize())
	fileManager.Read(blockId, p2)

	require.Equal(t, val, p2.GetInt(pos2))

	require.Equal(t, s, p2.GetString(pos1))
}
