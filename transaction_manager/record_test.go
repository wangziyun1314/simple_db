package transaction_manager

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/require"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
	"testing"
)

func TestNewStartRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "record_file")

	txNum := uint64(13)
	p := fm.NewPageBySize(32)
	p.SetInt(0, uint64(START))
	p.SetInt(8, txNum)
	startRecord := NewStartRecord(logManager, p)
	expectString := fmt.Sprintf("<START %d>", txNum)
	require.Equal(t, expectString, startRecord.ToString())

	_, err := startRecord.WriteToLog()
	require.Nil(t, err)

	iterator := logManager.Iterator()
	rec := iterator.Next()
	recOp := binary.LittleEndian.Uint64(rec[0:8])
	recNum := binary.LittleEndian.Uint64(rec[8:len(rec)])
	require.Equal(t, recOp, uint64(START))
	require.Equal(t, recNum, txNum)
}

func TestNewSetStringRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "setString")

	str := "original string"
	blk := uint64(1)
	dummy_blk := fm.NewBlockId("dummy_id", blk)
	txNum := uint64(1)
	offset := uint64(13)
	// 写入用于恢复日志
	WriteSetStringLog(logManager, txNum, dummy_blk, offset, str)
	pp := fm.NewPageBySize(400)
	pp.SetString(offset, str)
	iterator := logManager.Iterator()
	rec := iterator.Next()
	logP := fm.NewPageByBytes(rec)
	setStrRec := NewSetStringRecord(logP)
	expectedStr := fmt.Sprintf("<SETSTRING %d %d %d %s>", txNum, blk, offset, str)
	require.Equal(t, expectedStr, setStrRec.ToString())

	pp.SetString(offset, "modify string 1")
	pp.SetString(offset, "modify string 2")
	txSub := NewTxSub(pp)
	setStrRec.Undo(txSub)
	recover_str := pp.GetString(offset)
	require.Equal(t, recover_str, str)
}

func TestNewSetIntRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "setInt")

	val := uint64(11)
	blk := uint64(1)
	dummyBlockId := fm.NewBlockId("dummyId", blk)
	txNum := uint64(1)
	offset := uint64(13)
	// 写入用于恢复日志
	WriteSetIntLog(logManager, txNum, dummyBlockId, offset, val)
	pp := fm.NewPageBySize(400)
	pp.SetInt(offset, val)
	iterator := logManager.Iterator()
	rec := iterator.Next()
	logP := fm.NewPageByBytes(rec)
	setIntRec := NewSetIntRecord(logP)
	expectedStr := fmt.Sprintf("<SETINT %d %d %d %d>", txNum, blk, offset, val)

	require.Equal(t, expectedStr, setIntRec.ToString())

	pp.SetInt(offset, 22)
	pp.SetInt(offset, 33)
	txSub := NewTxSub(pp)
	setIntRec.Undo(txSub)
	recoverVal := pp.GetInt(offset)

	require.Equal(t, recoverVal, val)
}

func TestNewRollBackRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "rollback")

	txNum := uint64(13)
	WriteRollBackLog(logManager, txNum)
	iterator := logManager.Iterator()
	rec := iterator.Next()
	pp := fm.NewPageByBytes(rec)
	rollBackRecord := NewRollBackRecord(pp)
	expectedStr := fmt.Sprintf("<ROLLBACK %d>", txNum)

	require.Equal(t, expectedStr, rollBackRecord.ToString())
}

func TestNewCommitRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "commit")
	txNum := uint64(13)
	WriteCommitRecord(logManager, txNum)
	iterator := logManager.Iterator()
	rec := iterator.Next()
	pp := fm.NewPageByBytes(rec)

	commitRecord := NewCommitRecord(pp)
	expectStr := fmt.Sprintf("<COMMIT %d>", txNum)
	require.Equal(t, expectStr, commitRecord.ToString())
}

func TestCheckPointRecord(t *testing.T) {
	fileManager, _ := fm.NewFileManager("recordTest", 400)
	logManager, _ := lm.NewLogManager(fileManager, "checkPoint")

	WriteCheckPoint(logManager)
	iterator := logManager.Iterator()
	rec := iterator.Next()
	pp := fm.NewPageByBytes(rec)
	val := pp.GetInt(0)

	require.Equal(t, val, uint64(CHECKPOINT))

	record := NewCheckPointRecord()
	expectedStr := fmt.Sprintf("<CHECKPOINT>")
	require.Equal(t, expectedStr, record.ToString())
}
