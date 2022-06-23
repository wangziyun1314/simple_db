package main

import (
	"fmt"
	bmg "simpleDb/buffer_manager"
	fm "simpleDb/file_manager"
	lm "simpleDb/log_manager"
	tx "simpleDb/transaction_manager"
)

func main() {
	fileManager, _ := fm.NewFileManager("tx_test", 400)
	logManager, _ := lm.NewLogManager(fileManager, "logfile")
	bufferManager := bmg.NewBufferManager(fileManager, logManager, 3)

	tx1 := tx.NewTransaction(fileManager, logManager, bufferManager)
	blk := fm.NewBlockId("test_file", 1)
	tx1.Pin(blk)
	// 设置log为false，因为一开始数据没有任何意义，因此不能进行日志记录
	tx1.SetInt(blk, 80, 1, false)
	tx1.SetString(blk, 40, "one", false)
	tx1.Commit() // 执行回滚操作后，数据会还原到这里写入的内容

	tx2 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx2.Pin(blk)
	iVal, _ := tx2.GetInt(blk, 80)
	sVal, _ := tx2.GetString(blk, 40)
	fmt.Println("initial value at location 80 = ", iVal)
	fmt.Println("initial value at location 40 = ", sVal)
	newiVal := iVal + 1
	newsVal := sVal + "!"
	tx2.SetInt(blk, 80, int64(newiVal), true)
	tx2.SetString(blk, 40, newsVal, true)
	tx2.Commit() // 尝试写入新的数据

	tx3 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx3.Pin(blk)
	iVal, _ = tx3.GetInt(blk, 80)
	sVal, _ = tx3.GetString(blk, 40)
	fmt.Println("new value at location 80 = ", iVal)
	fmt.Println("new value at location 40 = ", sVal)
	tx3.SetInt(blk, 80, 999, true)
	getInt, _ := tx3.GetInt(blk, 80)
	fmt.Println("pre-rollback ivalue at location 80 : ", getInt)
	tx3.Rollback()

	tx4 := tx.NewTransaction(fileManager, logManager, bufferManager)
	tx4.Pin(blk)
	iVal111, _ := tx4.GetInt(blk, 80)
	fmt.Println("post-rollback at location 80 = ", iVal111)
	tx4.Commit()
}
