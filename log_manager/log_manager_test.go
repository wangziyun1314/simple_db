package log_manager

import (
	"fmt"
	"github.com/stretchr/testify/require"
	fm "simpleDb/file_manager"
	"testing"
)

func makeRecords(s string, n uint64) []byte {
	// 生成日志内容
	p := fm.NewPageBySize(1)
	npos := p.MaxLengthForString(s)
	b := make([]byte, npos+UINT64_LEN)
	p = fm.NewPageByBytes(b)
	p.SetString(0, s)
	p.SetInt(npos, n)
	return b
}

func createRecords(lm *LogManager, start uint64, end uint64) {
	for i := start; i <= end; i++ {
		records := makeRecords(fmt.Sprintf("record%d", i), i)
		lm.Append(records)
	}
}

func TestLogManager_Append(t *testing.T) {
	fileManager, _ := fm.NewFileManager("logtest", 400)
	logManager, err := NewLogManager(fileManager, "logfile")
	require.Nil(t, err)

	createRecords(logManager, 1, 35)

	iter := logManager.Iterator()
	recNum := uint64(35)
	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)
		require.Equal(t, fmt.Sprintf("record%d", recNum), s)

		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, recNum)
		recNum -= 1
	}

	createRecords(logManager, 36, 70)
	logManager.FlushByLSN(65)

	iter = logManager.Iterator()
	recNum = uint64(70)

	for iter.HasNext() {
		rec := iter.Next()
		p := fm.NewPageByBytes(rec)
		s := p.GetString(0)

		require.Equal(t, fmt.Sprintf("record%d", recNum), s)

		npos := p.MaxLengthForString(s)
		val := p.GetInt(npos)
		require.Equal(t, val, recNum)
		recNum -= 1
	}

}
