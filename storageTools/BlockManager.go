package storageTools

import (
	"KVwithWAL/config"
	"io"
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
		blockSize: config.AppConfig.BlockSize,
		file:      file,
	}
}

func (thisBlockManager *BlockManager) ReadBlock(blockIndex int) ([]byte, bool) { //Do i even need this function?
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

func (bm *BlockManager) WriteAtOffset(offset int64, data []byte) error {
	dataLen := len(data)
	blockSize := bm.blockSize

	currentOffset := offset
	bytesWritten := 0

	for bytesWritten < dataLen {
		blockIndex := int(currentOffset) / blockSize
		offsetInBlock := int(currentOffset) % blockSize

		bytesAvailableInBlock := blockSize - offsetInBlock
		bytesRemaining := dataLen - bytesWritten

		bytesToWrite := min(bytesAvailableInBlock, bytesRemaining)

		blockData, found := bm.Cache.Get(blockIndex)
		if !found {
			blockData = make([]byte, blockSize)
			n, err := bm.file.ReadAt(blockData, int64(blockIndex*blockSize))
			if err != nil && err != io.EOF {
				return err
			}
			if err == io.EOF {
				for i := n; i < blockSize; i++ {
					blockData[i] = 0
				}
			}
		}

		copy(blockData[offsetInBlock:], data[bytesWritten:bytesWritten+bytesToWrite])

		bm.WriteBlock(blockIndex, blockData)

		bytesWritten += bytesToWrite
		currentOffset += int64(bytesToWrite)
	}
	return nil
}

func (bm *BlockManager) ReadAtOffset(offset int64, data []byte) error {
	dataLen := len(data)
	blockSize := bm.blockSize

	currentOffset := offset
	bytesRead := 0

	for bytesRead < dataLen {
		blockIndex := int(currentOffset) / blockSize
		offsetInBlock := int(currentOffset) % blockSize

		bytesAvailableInBlock := blockSize - offsetInBlock
		bytesRemaining := dataLen - bytesRead

		bytesToRead := min(bytesAvailableInBlock, bytesRemaining)

		// Get the block
		blockData, found := bm.Cache.Get(blockIndex)
		if !found {
			blockData = make([]byte, blockSize)
			n, err := bm.file.ReadAt(blockData, int64(blockIndex*blockSize))
			if err != nil && err != io.EOF {
				return err
			}
			if n < blockSize {
				// Zero-fill the rest
				for i := n; i < blockSize; i++ {
					blockData[i] = 0
				}
			}
			bm.Cache.Set(blockIndex, blockData)
		}

		// Read data from block
		copy(data[bytesRead:bytesRead+bytesToRead], blockData[offsetInBlock:offsetInBlock+bytesToRead])

		bytesRead += bytesToRead
		currentOffset += int64(bytesToRead)

		// Check if we've reached the end of the file
		fileSize, err := bm.FileSize()
		if err != nil {
			return err
		}
		if currentOffset >= fileSize {
			return io.EOF
		}
	}
	return nil
}
func (bm *BlockManager) FileSize() (int64, error) {
	fileInfo, err := bm.file.Stat()
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}
