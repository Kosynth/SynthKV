package containers

import (
	"KVwithWAL/config"
	"KVwithWAL/model/records"
	"KVwithWAL/storageTools"
	"fmt"
)

type WalSegment struct {
	blockManager       *storageTools.BlockManager
	startOffset        int64
	currentOffset      int64
	maxSegmentSize     int
	isWrittenToSStable bool
}

func NewWalSegment(blockManager *storageTools.BlockManager, maxSegmentSize int64, startOffset int64) *WalSegment {
	return &WalSegment{
		blockManager:       blockManager,
		startOffset:        startOffset,
		currentOffset:      startOffset,
		maxSegmentSize:     config.GetAppConfig().WalSegmentSize,
		isWrittenToSStable: false,
	}
}

func (ws *WalSegment) AppendRecord(record records.WalRecord) error {
	data := records.ToBytes(record)
	recordSize := int64(len(data))

	fmt.Printf("Before writing, currentOffset: %d, recordSize: %d\n", ws.currentOffset, recordSize)

	err := ws.blockManager.WriteAtOffset(ws.currentOffset, data)
	if err != nil {
		return err
	}

	ws.currentOffset += recordSize

	fmt.Printf("After writing, new currentOffset: %d\n", ws.currentOffset)
	return nil
}
func (currenttWalSegment *WalSegment) Size() int64 {
	return currenttWalSegment.currentOffset
}
