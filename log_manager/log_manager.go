package log_manager

import (
	fm "simpleDb/file_manager"
	"sync"
)

const (
	UINT64_LEN = 8
)

type LogManager struct {
	fileManager  *fm.FileManager
	logFile      string      // 日志文件的名称
	logPage      *fm.Page    // 存储日志的缓冲区
	currentBlk   *fm.BlockId // 日志当前写入的区块号
	latestLsn    uint64      // 当前最新的日志编号
	lastSavedLsn uint64      // 上一次写入磁盘的日志编号
	mu           sync.Mutex
}

// 有一点不明白
func (lm *LogManager) appendNewBlock() (*fm.BlockId, error) {
	// 当缓冲区用完之后调用该接口分配新内存
	blk, err := lm.fileManager.Append(lm.logFile) // 在二进制日志文件末尾添加一个区块
	if err != nil {
		return nil, err
	}

	/*
		添加日志的时候是从内存的底部往上写入的，缓冲区400字节，日志100字节，就会写入到300-400的这个位置，
		首先，在缓冲区的首部写入偏移，假设日志100字节写入缓冲区，下次写入的偏移要从300算起，于是这个300就要写入缓冲区的头8字节
	*/
	lm.logPage.SetInt(0, uint64(lm.fileManager.BlockSize())) // blockSize 假设是400字节
	lm.fileManager.Write(blk, lm.logPage)

	return blk, nil
}

func NewLogManager(fileManager *fm.FileManager, logFile string) (*LogManager, error) {
	logManager := LogManager{
		fileManager:  fileManager,
		logFile:      logFile,
		logPage:      fm.NewPageBySize(fileManager.BlockSize()),
		latestLsn:    0,
		lastSavedLsn: 0,
	}

	logSize, err := fileManager.Size(logFile)
	if err != nil {
		return nil, err
	}

	if logSize == 0 {
		// 如果文件为空，就要为文件添加一个新区块
		blk, err := logManager.appendNewBlock()
		if err != nil {
			return nil, err
		}

		logManager.currentBlk = blk
	} else {
		// 文件已经存在，先把末尾的日志内容读入内存，如果当前对应区块还有空间，新的日志就写入当前区块
		logManager.currentBlk = fm.NewBlockId(logManager.logFile, logSize-1)
		fileManager.Read(logManager.currentBlk, logManager.logPage)
	}

	return &logManager, nil
}

func (lm *LogManager) FlushByLSN(lsn uint64) error {
	// 把给定编号之气的日志全部写入磁盘
	/*
		当我们写入给定编号的日志的时候，接口会把当前日志处于同一区块的日志写入磁盘，
		假设现在写入的日志编号为65，如果66，67，68也处于同一个区块，那么他们也会被写入磁盘
	*/
	if lsn > lm.lastSavedLsn {
		err := lm.Flush()
		if err != nil {
			return err
		}

		lm.lastSavedLsn = lsn
	}
	return nil
}

func (lm *LogManager) Flush() error {
	// 将给定缓冲区的数据写入磁盘
	_, err := lm.fileManager.Write(lm.currentBlk, lm.logPage)
	if err != nil {
		return err
	}

	return nil
}

func (lm *LogManager) Append(logRecord []byte) (uint64, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := lm.logPage.GetInt(0) // 从头部获取可写入的偏移
	recordSize := uint64(len(logRecord))
	bytesNeed := recordSize + UINT64_LEN
	var err error
	if int(boundary-bytesNeed) < int(UINT64_LEN) {
		// 当前没有足够的空间，现将缓冲区数据写入磁盘
		err = lm.Flush()
		if err != nil {
			return lm.latestLsn, err
		}

		// 分配新的空间用于写入新数据
		lm.currentBlk, err = lm.appendNewBlock()
		if err != nil {
			return lm.latestLsn, nil
		}

		boundary = lm.logPage.GetInt(0) // 获得当前可写入的偏移
	}

	recordPosition := boundary - bytesNeed
	lm.logPage.SetBytes(recordPosition, logRecord)
	lm.logPage.SetInt(0, recordPosition)
	lm.latestLsn += 1
	return lm.latestLsn, nil
}

func (lm *LogManager) Iterator() *LogIterator {
	lm.Flush()
	return NewLogIterator(lm.fileManager, lm.currentBlk)
}
