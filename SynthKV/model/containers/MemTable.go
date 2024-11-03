package containers

import (
	"KVwithWAL/model/records"
)

type MemTable struct {
	id         int
	table      map[string]*records.MemTableRecord
	maxSize    int
	isWritable bool
}

var nextID int = 1

func NewMemTable(maxSize int, isWritable bool) *MemTable {
	memTable := &MemTable{
		id:         nextID,
		table:      make(map[string]*records.MemTableRecord),
		maxSize:    maxSize,
		isWritable: isWritable,
	}
	nextID++
	return memTable
}
