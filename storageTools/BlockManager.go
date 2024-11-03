package storageTools

import (
	"KVwithWAL/config"
	"os"
)

// After testing make cache private again, remove print statements from BlockCache.go
type BlockManager struct {
	Cache     *BlockCache
	blockSize int
	file      *os.File
}

// perhaps determine filepath based on a block on the drive if the file does not exist(Corrupted) after a restart and try to find it by some byte pattern, save the file paths to config too after they're created, also it is unclear whether more than one WAL is needed.
func NewBlockManager(filePath string) *BlockManager {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
	}
	return &BlockManager{
		Cache:     NewBlockCache(),
		blockSize: config.GetAppConfig().BlockSize,
		file:      file,
	}
}

func (thisBlockManager *BlockManager) ReadBlock(blockIndex int) ([]byte, bool) {
	data, found := thisBlockManager.Cache.Get(blockIndex) // try to get block from cache
	if data != nil {
		return data, found
	}
	offset := blockIndex * thisBlockManager.blockSize
	data = make([]byte, thisBlockManager.blockSize)
	_, err := thisBlockManager.file.ReadAt(data, int64(offset))
	if err != nil {
		return nil, false
	}
	thisBlockManager.Cache.Set(blockIndex, data) //refresh block in cache
	return data, true
}
func (thisBlockManager *BlockManager) WriteBlock(blockIndex int, data []byte) {
	offset := blockIndex * thisBlockManager.blockSize
	thisBlockManager.file.WriteAt(data, int64(offset))
	thisBlockManager.Cache.Set(blockIndex, data)
}
