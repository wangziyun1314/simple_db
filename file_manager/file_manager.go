package file_manager

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string
	blockSize   uint64
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewFileManager(dbDirectory string, blockSize uint64) (*FileManager, error) {
	fileManager := FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       false,
		openFiles:   make(map[string]*os.File),
	}

	if _, err := os.Stat(dbDirectory); os.IsNotExist(err) {
		// 目录不存在则生成
		fileManager.isNew = true
		err := os.Mkdir(dbDirectory, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else {
		// 如果目录已经存在，则把目录中的临时文件删除
		err := filepath.Walk(dbDirectory, func(path string, info fs.FileInfo, err error) error {
			mode := info.Mode()
			if mode.IsRegular() {
				name := info.Name()
				if strings.HasPrefix(name, "temp") {
					// 删除临时文件
					os.Remove(filepath.Join(path, name))
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return &fileManager, nil
}

func (f *FileManager) getFile(fileName string) (*os.File, error) {
	path := filepath.Join(f.dbDirectory, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	f.openFiles[fileName] = file
	return file, nil
}

func (f *FileManager) Read(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}

	defer file.Close()

	count, err := file.ReadAt(p.contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (f *FileManager) Write(blk *BlockId, p *Page) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	file, err := f.getFile(blk.FileName())
	if err != nil {
		return 0, err
	}

	defer file.Close()

	count, err := file.WriteAt(p.contents(), int64(blk.Number()*f.blockSize))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (f *FileManager) Size(fileName string) (uint64, error) {
	file, err := f.getFile(fileName)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(stat.Size()) / f.blockSize, nil
}

// Append 不是很明白这个返回
func (f *FileManager) Append(fileName string) (*BlockId, error) {
	newBlockNum, err := f.Size(fileName)
	if err != nil {
		return &BlockId{}, err
	}

	blk := NewBlockId(fileName, newBlockNum)
	file, err := f.getFile(blk.FileName())
	if err != nil {
		return &BlockId{}, err
	}
	defer file.Close()

	b := make([]byte, f.blockSize)
	_, err = file.WriteAt(b, int64(blk.Number()*f.blockSize)) // 在文件的末尾扩大、相当于append
	if err != nil {
		return &BlockId{}, err
	}

	return blk, nil
}

func (f *FileManager) IsNew() bool {
	return f.isNew
}

func (f *FileManager) BlockSize() uint64 {
	return f.blockSize
}
