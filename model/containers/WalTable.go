package containers

import (
	"KVwithWAL/config"
	"KVwithWAL/model/records"
	"KVwithWAL/storageTools"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type WalTable struct {
	uuid         string
	blockManager *storageTools.BlockManager
	segmentSize  int64
	segments     []*WalSegment
}

func NewWalTable(uuid string, blockManager *storageTools.BlockManager) *WalTable {
	segmentSize := int64(config.AppConfig.WalSegmentSize)
	return &WalTable{
		uuid:         uuid,
		blockManager: blockManager,
		segmentSize:  segmentSize,
		segments:     []*WalSegment{},
	}
}

func (currentWalTable *WalTable) AddRecord(record records.WalRecord) error {
	var currentSegment *WalSegment

	if len(currentWalTable.segments) == 0 || currentWalTable.segments[len(currentWalTable.segments)-1].Size() >= currentWalTable.segmentSize {
		var startOffset int64 = 0
		if len(currentWalTable.segments) > 0 {
			previousSegment := currentWalTable.segments[len(currentWalTable.segments)-1]
			startOffset = previousSegment.currentOffset
		}
		newSegment := NewWalSegment(currentWalTable.blockManager, currentWalTable.segmentSize, startOffset)
		currentWalTable.segments = append(currentWalTable.segments, newSegment)
		currentSegment = newSegment
	} else {
		currentSegment = currentWalTable.segments[len(currentWalTable.segments)-1]
	}

	fmt.Println("Record added to WAL:", record) // Synthko.Debug A2
	return currentSegment.AppendRecord(record)
}

func (currentWalTable *WalTable) ReadAllRecords() ([]records.WalRecord, error) {
	var allRecords []records.WalRecord

	if len(currentWalTable.segments) == 0 {
		fmt.Println("No segments to read from") // Synthko.Debug A2
		return allRecords, nil
	}

	for _, segment := range currentWalTable.segments {
		fmt.Printf("Reading segment starting at offset: %d with currentOffset: %d\n", segment.startOffset, segment.currentOffset) // Synthko.Debug A2
		offset := segment.startOffset
		for offset < segment.currentOffset {
			record, bytesRead, err := readRecordAtOffset(currentWalTable.blockManager, offset)
			if err != nil {
				fmt.Printf("Error reading record at offset %d: %v\n", offset, err) // Synthko.Debug A2
				return nil, err
			}
			fmt.Printf("Offset: %d\n", offset)
			offset += int64(bytesRead)
			fmt.Println("Record read from WAL:", record) // Synthko.Debug A2
			allRecords = append(allRecords, record)
		}
	}

	fmt.Printf("Total records read: %d\n", len(allRecords)) // Synthko.Debug A2
	return allRecords, nil
}

func (wt *WalTable) LoadSegments() error {
	fileSize, err := wt.blockManager.FileSize()
	if err != nil {
		return err
	}

	if fileSize == 0 {
		fmt.Println("WAL file is empty.") // Synthko.Debug A2
		return nil
	}

	offset := int64(0)
	for offset < fileSize {
		segment := NewWalSegment(wt.blockManager, wt.segmentSize, offset)
		wt.segments = append(wt.segments, segment)

		segmentEndOffset, err := wt.scanSegment(segment)
		if err != nil {
			return err
		}

		segment.currentOffset = segmentEndOffset

		if segmentEndOffset == offset {
			fmt.Printf("No progress made in scanSegment. Breaking LoadSegments loop at offset %d.\n", offset) // Synthko.Debug A2
			break
		}

		offset = segmentEndOffset
	}

	return nil
}

func (wt *WalTable) scanSegment(segment *WalSegment) (int64, error) {
	offset := segment.startOffset
	maxFileSize, err := wt.blockManager.FileSize()
	if err != nil {
		return 0, err
	}

	iteration := 0
	for {
		iteration++
		if offset >= maxFileSize {
			fmt.Printf("Reached end of file at offset %d\n", offset) // Synthko.Debug A2
			break
		}

		headerSize := 37
		header := make([]byte, headerSize)

		err := wt.blockManager.ReadAtOffset(offset, header[:4])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				fmt.Printf("Reached EOF at offset %d\n", offset) // Synthko.Debug A2
				break
			}
			return offset, err
		}

		crc32Value := binary.LittleEndian.Uint32(header[:4])
		if crc32Value == 0 {
			fmt.Printf("Encountered zero CRC32 at offset %d. Stopping scan.\n", offset) // Synthko.Debug A2
			break
		}
		err = wt.blockManager.ReadAtOffset(offset+4, header[4:])
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				fmt.Printf("Reached EOF while reading header at offset %d\n", offset) // Synthko.Debug A2
				break
			}
			return offset, err
		}

		keySize := binary.LittleEndian.Uint64(header[21:29])
		valueSize := binary.LittleEndian.Uint64(header[29:37])

		fmt.Printf("Iteration %d: offset=%d, CRC32=%d, keySize=%d, valueSize=%d\n", iteration, offset, crc32Value, keySize, valueSize) // Synthko.Debug A2

		totalSize := int64(headerSize) + int64(keySize) + int64(valueSize)
		if totalSize <= 0 || totalSize > maxFileSize-offset {
			fmt.Printf("Invalid totalSize (%d) at offset %d. Stopping scan.\n", totalSize, offset) // Synthko.Debug A2
			break
		}

		offset += totalSize
		if offset-segment.startOffset >= int64(config.AppConfig.WalSegmentSize) {
			break
		}

	}
	return offset, nil
}

func readRecordAtOffset(blockManager *storageTools.BlockManager, offset int64) (records.WalRecord, int, error) {
	headerSize := 37                   // Read the header first, get data length (what if it is multi-block record? (probably that can't be the case, but i don't remember specs)) and then read the data
	header := make([]byte, headerSize) //later we need to add map for positions of each element in the header
	err := blockManager.ReadAtOffset(offset, header)
	if err != nil {
		return records.WalRecord{}, 0, err
	}

	keySize := binary.LittleEndian.Uint64(header[21:29])
	valueSize := binary.LittleEndian.Uint64(header[29:37])
	totalSize := int(headerSize) + int(keySize) + int(valueSize)

	fmt.Printf("Offset: %d\n", offset)
	fmt.Printf("Key Size: %d\n", keySize)
	fmt.Printf("Value Size: %d\n", valueSize)
	fmt.Printf("Total Size: %d\n", totalSize)

	data := make([]byte, totalSize)
	err = blockManager.ReadAtOffset(offset, data)
	if err != nil {
		return records.WalRecord{}, 0, err
	}

	record := records.FromBytes(data)
	expectedCRC := binary.LittleEndian.Uint32(data[:4])
	actualCRC := crc32.ChecksumIEEE(data[4:])

	if expectedCRC != actualCRC {
		return records.WalRecord{}, 0, fmt.Errorf("CRC mismatch")
	}

	return record, totalSize, nil
}
