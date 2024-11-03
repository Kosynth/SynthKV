package records

import (
	"KVwithWAL/config"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"time"
)

type operationType int

const (
	Put    operationType = 0
	Delete operationType = 1
)

type WalRecord struct {
	cRC32           uint32
	keySize         uint64
	valueSize       uint64
	totalSize       uint64
	processedToWAL  bool
	addedToMemTable bool
	tombstone       bool
	timeStamp       time.Time
	operation       operationType
	key             string
	value           []byte
}

// Create WALRecord here, the data that needs filling here is CRC32, keySize, valueSize, tombStone, proccessedToWAL=false, AddedToMemtable=false, timestamp return WALRecord
func CreateRecord(operation operationType, key string, value []byte) WalRecord {

	record := WalRecord{
		key:             key,
		value:           value,
		processedToWAL:  false,
		addedToMemTable: false,
		timeStamp:       time.Now(),
		totalSize:       uint64(len(key) + len(value) + config.GetAppConfig().StaticWALAttributesSize),
		cRC32:           uint32(0), //calculate this when converting to bytes
		keySize:         uint64(len(key)),
		valueSize:       uint64(len(value)),
		tombstone:       false,
		operation:       operation,
	}
	return record
}

func saveToWAL(record WalRecord) {
	//Write to file here, first find the object that we're currently writing to, then call a function inside it, if successful, (set processedToWAL to true, add to processing query for MemTable, when MemTableQueueRecord is created, set addedToMemTable to true) if not, handle error
}

func readWALPage( /*set of WAL files goes here, how do i determine which one is the best*/ ) /*WALRecord*/ {
	//
	//Read from WAL files here, if successful, return WALRecord or a set of them, if not, handle error
}

func ToBytes(r WalRecord) []byte {
	data := make([]byte, r.totalSize)
	offset := 4

	//Fuck, I hate this, need to initialize a map in config where all our types have length and base offset on those
	writeField := func(value interface{}, length int) {
		switch v := value.(type) {
		case uint32:
			binary.LittleEndian.PutUint32(data[offset:], v)
			offset += length
		case uint64:
			binary.LittleEndian.PutUint64(data[offset:], v)
			offset += length
		case bool:
			if v {
				data[offset] = 1
			} else {
				data[offset] = 0
			}
			offset++
		case string:
			bytes := []byte(v)
			copy(data[offset:], []byte(v))
			offset += len(bytes)
		case []byte:
			copy(data[offset:], v)
			offset += length
		}
	}

	writeField(r.keySize, 8)
	writeField(r.valueSize, 8)
	writeField(r.tombstone, 1)
	writeField(uint32(r.operation), 4)
	writeField(r.key, int(r.keySize))
	writeField(r.value, int(r.valueSize))
	writeField(uint64(r.timeStamp.UnixNano()), 8)

	r.cRC32 = crc32.ChecksumIEEE(data[4:offset])
	binary.LittleEndian.PutUint32(data[:4], r.cRC32)
	return data
}

// #region TestingAndShit
func FromBytes(data []byte) WalRecord { //only for testing
	var r WalRecord
	offset := 0
	r.cRC32 = binary.LittleEndian.Uint32(data[:4])
	offset += 4
	r.keySize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	r.valueSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	r.tombstone = data[offset] == 1
	offset++
	r.operation = operationType(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	r.key = string(data[offset : offset+int(r.keySize)])
	offset += int(r.keySize)
	r.value = data[offset : offset+int(r.valueSize)]
	offset += int(r.valueSize)
	r.timeStamp = time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset:])))
	offset += 8
	return r
}

func GenerateRandomRecords() []WalRecord {
	var records []WalRecord

	for i := 0; i < 1000; i++ {
		record := CreateRecord(operationType(rand.Intn(2)), generateRandomKey(), generateRandomValue())
		records = append(records, record)
	}
	return records
}

func generateRandomKey() string {
	dictionary := []string{
		"apple", "banana", "cherry", "date", "elderberry", "fig", "grape", "honeydew", "kiwi", "lemon",
		"mango", "nectarine", "orange", "pear", "quince", "raspberry", "strawberry", "tangerine", "ugli", "watermelon",
		"almond", "cashew", "walnut", "pecan", "hazelnut", "pistachio", "macadamia", "pine", "chestnut", "coconut",
		"apricot", "blueberry", "blackberry", "cranberry", "gooseberry", "grapefruit", "lime", "pomegranate", "plum", "peach",
		"avocado", "guava", "passionfruit", "papaya", "dragonfruit", "lychee", "rhubarb", "mulberry", "boysenberry", "loganberry",
		"carrot", "celery", "cucumber", "lettuce", "onion", "pepper", "potato", "tomato", "zucchini", "broccoli",
		"garlic", "ginger", "kale", "mushroom", "radish", "spinach", "squash", "turnip", "yam", "asparagus",
		"beet", "cabbage", "cauliflower", "leek", "okra", "parsnip", "pea", "rhubarb", "rutabaga", "artichoke",
		"black-eyed", "chickpea", "lentil", "lima", "mung", "navy", "pinto", "soy", "adzuki", "kidney",
	}
	word := dictionary[rand.Intn(len(dictionary))]
	key := word + dictionary[rand.Intn(len(dictionary))] + dictionary[rand.Intn(len(dictionary))]
	return key
}

func generateRandomValue() []byte {
	size := rand.Intn(100) + 1
	value := make([]byte, size)
	rand.Read(value)
	return value
}

// #endregion
