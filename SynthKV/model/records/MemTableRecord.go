package records

import (
	"time"
)

type MemTableRecord struct {
	cRC32     uint32
	keySize   uint64
	valueSize uint64
	tombstone bool
	operation operationType
	key       string
	value     []byte
	timeStamp time.Time
}

func (m *MemTableRecord) GenerateFromWALRecords(walRecords []WalRecord) []*MemTableRecord {
	memTableRecords := make([]*MemTableRecord, len(walRecords))

	for i, wr := range walRecords {
		memTableRecord := &MemTableRecord{
			cRC32:     wr.cRC32, // not sure if i need it here
			keySize:   wr.keySize,
			valueSize: wr.valueSize,
			tombstone: wr.tombstone, //need tombstone here still cause the record can be deleted while it is still in memtable, no need to pass such objects in SSTABLES
			operation: wr.operation,
			key:       wr.key,
			value:     wr.value,
			timeStamp: wr.timeStamp, // not sure if i need it here
		}

		memTableRecords[i] = memTableRecord
	}

	return memTableRecords
}
