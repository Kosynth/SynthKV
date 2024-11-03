package containers

import (
	"KVwithWAL/config"
	"KVwithWAL/model/records"
	"os"
)

type WalSegment struct {
	records []records.WalRecord
}

type WalTable struct {
	uuid        string
	file        *os.File
	segmentSize config.GetAppConfig().WalSegmentSize
	segments    []WalSegment
}

func createNewTable() WalTable {
	// Create a new table here, 1.6 Block Manager and Block Cache looks scary
	return WalTable{}
}

func ReadAllRecords() []records.WalRecord {
	//this is just a placeholder,
	return []records.WalRecord{}
}
