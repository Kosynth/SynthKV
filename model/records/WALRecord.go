package records

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
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
	cRC32     uint32
	keySize   uint64
	valueSize uint64
	totalSize uint64
	tombstone bool
	timeStamp time.Time
	operation operationType
	key       string
	value     []byte
}

func GetJsonRecord(r WalRecord) string {
	return fmt.Sprintf(
		`{"key":"%s","value":"%s","timeStamp":"%s","tombstone":%t,"operation":%d}`,
		r.key,
		hex.EncodeToString(r.value),
		r.timeStamp.Format(time.RFC3339Nano),
		r.tombstone,
		r.operation,
	)
}

// Create WALRecord here, the data that needs filling here is CRC32, keySize, valueSize, tombStone, proccessedToWAL=false
func CreateRecord(operation operationType, key string, value []byte) WalRecord {

	record := WalRecord{
		key:       key,
		value:     value,
		timeStamp: time.Now(),
		totalSize: uint64(4 + 16 + 1 + 8 + 8 + len(key) + len(value)), //uint64(len(key) + len(value) + config.GetAppConfig().StaticWALAttributesSize),
		cRC32:     uint32(0),                                          //calculate this when converting to bytes
		keySize:   uint64(len(key)),
		valueSize: uint64(len(value)),
		tombstone: false,
		operation: operation,
	}
	return record
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
		case int64:
			binary.LittleEndian.PutUint64(data[offset:], uint64(v))
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
			copy(data[offset:], bytes)
			offset += len(bytes)
		case []byte:
			copy(data[offset:], v)
			offset += length
		}
	}
	//header
	writeField(r.timeStamp.Unix(), 8) //why 16 bytes though? 2024-11-03T16:23:32.1519913+01:00 before 2024-11-03T16:47:48.9387203+01:00 7 positions anyway, no added precision
	writeField(int64(r.timeStamp.Nanosecond()), 8)
	//fmt.Printf("Time before serialization: %s\n", r.timeStamp.Format(time.RFC3339Nano))
	writeField(r.tombstone, 1)

	writeField(r.keySize, 8)
	writeField(r.valueSize, 8)
	//data
	writeField(r.key, int(r.keySize))
	writeField(r.value, int(r.valueSize))

	r.cRC32 = crc32.ChecksumIEEE(data[4:offset])
	binary.LittleEndian.PutUint32(data[:4], r.cRC32)
	return data
}

// #region TestingAndShit

func FromBytes(data []byte) WalRecord { //probably needs rewriting
	var r WalRecord
	offset := 0
	r.cRC32 = binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	r.timeStamp = time.Unix(int64(binary.LittleEndian.Uint64(data[offset:])), int64(binary.LittleEndian.Uint64(data[offset+8:]))) //why 16 bytes though? 2024-11-03T16:23:32.1519913+01:00 before 2024-11-03T16:47:48.9387203+01:00 after 7 positions anyway, no added precision
	offset += 16

	r.tombstone = data[offset] == 1
	offset++

	r.keySize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	r.valueSize = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	r.key = string(data[offset : offset+int(r.keySize)])
	offset += int(r.keySize)
	r.value = data[offset : offset+int(r.valueSize)]
	offset += int(r.valueSize)

	//fmt.Printf("Time after deserialization: %s\n", r.timeStamp.Format(time.RFC3339Nano))
	return r
}
func GenerateRandomRecords(count int) []WalRecord {
	var records []WalRecord

	for i := 0; i < count; i++ {
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
