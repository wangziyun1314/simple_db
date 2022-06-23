package buffer_manager

import (
	"fmt"
	"github.com/stretchr/testify/require"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
	"testing"
)

func TestBufferManager_Available(t *testing.T) {
	fileManager, _ := fm.NewFileManager("buffertest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile")
	bufferManager := NewBufferManager(fileManager, logManager, 3)

	buf1, err := bufferManager.Pin(fm.NewBlockId("testfile", 1))
	require.Nil(t, err)

	p := buf1.Contents()
	n := p.GetInt(80)
	p.SetInt(80, n+1)
	buf1.SetModified(1, 0) // 通知缓存管理器数据被修改了
	bufferManager.Unpin(buf1)

	buf2, err := bufferManager.Pin(fm.NewBlockId("testfile", 2))
	require.Nil(t, err)

	buf3, err := bufferManager.Pin(fm.NewBlockId("testfile", 3))
	require.Nil(t, err)
	fmt.Println(buf3)

	_, err = bufferManager.Pin(fm.NewBlockId("testfile", 4))
	require.Nil(t, err) // 这样会让原来的数据写入磁盘

	bufferManager.Unpin(buf2) // 由于buf2数据没有更改，所以unpin不会将数据写入磁盘
	buff2, err := bufferManager.Pin(fm.NewBlockId("testfile", 1))
	require.Nil(t, err)
	p2 := buff2.Contents()
	p2.SetInt(80, 999)
	buff2.SetModified(1, 0)
	bufferManager.Unpin(buff2) // 这里的数据不会写入到磁盘

	// 把testfile的区块1读入，确定之前的buff1的修改写入到了磁盘
	page := fm.NewPageBySize(400)
	b1 := fm.NewBlockId("testfile", 1)
	fileManager.Read(b1, page)
	n1 := page.GetInt(80)
	require.Equal(t, n1, n+1)
}
