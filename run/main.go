package main

import (
	"KVwithWAL/config"
	"KVwithWAL/initUtils"
	"KVwithWAL/model/containers"
	"KVwithWAL/model/records"
	"KVwithWAL/storageTools"
	"fmt"
)

func main() {
	InitAndPrintCFG()

	//EVERYTHING BELOW THIS LINE IS FOR TESTING PURPOSES
	TestSerialization()
	TestIfTheCacheStillWorks()
	GenerateRandomRecordsAndSaveToWal()
	ReadFromWalAndPrint()
}

func GenerateRandomRecordsAndSaveToWal() {
	randomRecords := records.GenerateRandomRecords(1000)
	blockManager := storageTools.NewBlockManager(config.GetAppConfig().PathToWALFile)
	walTable := containers.NewWalTable("123", blockManager)

	for _, r := range randomRecords {
		err := walTable.AddRecord(r)
		if err != nil {
			fmt.Printf("Error adding record: %v\n", err)
			return
		}
	}
	ReadRecords, err := walTable.ReadAllRecords()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf(records.GetJsonRecord(ReadRecords[0]))
}
func ReadFromWalAndPrint() {
	blockManager := storageTools.NewBlockManager("Records.wal")
	walTable := containers.NewWalTable("test-uuid", blockManager)
	err := walTable.LoadSegments()
	if err != nil {
		fmt.Println("Error loading segments:", err)
		return
	}
	readRecords, err := walTable.ReadAllRecords()
	if err != nil {
		fmt.Println("Error reading records:", err)
		return
	}
	fmt.Print("[")
	objectNum := 0
	for _, record := range readRecords {
		objectNum++
		jsonStr := records.GetJsonRecord(record)
		fmt.Printf("\"objectNum\":\"%d\"", objectNum)
		fmt.Println(jsonStr)
		fmt.Print(",")
	}
	fmt.Print("]")
}
func TestIfTheCacheStillWorks() {
	//Tests for saving records to a file via BlockManager
	data := []byte{}
	file := "testFile.txt"
	blockManager := storageTools.NewBlockManager(file)
	randomRecords := records.GenerateRandomRecords(100)
	//Testing for multiple blocks
	for _, r := range randomRecords {
		data = append(data, records.ToBytes(r)...)
	}
	totalBlocks := (len(data) + config.GetAppConfig().BlockSize - 1) / config.GetAppConfig().BlockSize
	for i := 0; i < totalBlocks; i++ {
		start := i * config.GetAppConfig().BlockSize
		end := start + config.GetAppConfig().BlockSize

		if end > len(data) {
			end = len(data)
		}
		chunk := data[start:end]

		blockManager.WriteBlock(i, chunk)
	}
	//fmt.Printf("Data: %+v\n", data)
	fmt.Printf("DataLength: %d bytes || Written %d blocks of size %d to %s\n", len(data), totalBlocks, config.GetAppConfig().BlockSize, file)

	//Testing cache
	var found bool
	for _, idx := range []int{0, 1, 450, 0, 3, 13, 1} {
		fmt.Printf("\nReading block %d\n", idx)
		data, found := blockManager.ReadBlock(idx)
		if found {
			fmt.Printf("Data read from block %d: %v....\n", idx, data[:min(25, len(data))])
		} else {
			fmt.Printf("Block %d not found\n", idx)
		}
		blockManager.Cache.PrintCacheState()
	}

	fmt.Println("\n--- Writing to blocks ---")
	newData := []byte("its 4 am")
	blockManager.WriteBlock(2, newData)
	fmt.Printf("Wrote new data to block 2\n")
	blockManager.Cache.PrintCacheState()

	newData = []byte("what am i doing with my life")
	blockManager.WriteBlock(5, newData)
	fmt.Printf("Wrote new data to block 5\n")
	blockManager.Cache.PrintCacheState()

	fmt.Printf("\nReading block %d after writing new data\n", 2)
	data, found = blockManager.ReadBlock(2)
	if found {
		fmt.Printf("Data read from block %d: %s....\n", 2, data[:min(25, len(data))])

	}
	blockManager.Cache.PrintCacheState()
}
func InitAndPrintCFG() {
	cfg, _ := initUtils.InitializeConfiguration("config/appConfig.json")
	fmt.Printf("Config: %+v\n", cfg)
}

func TestSerialization() {
	basicTestData := records.GenerateRandomRecords(1)[0]
	fmt.Printf("Byte: %+v\n", records.ToBytes(basicTestData))
	fmt.Printf("Hex: ")
	for _, b := range records.ToBytes(basicTestData) {
		fmt.Printf("%02x ", b)
	}
	fmt.Println()
	encodedData := records.ToBytes(basicTestData)
	decodedRecord := records.FromBytes(encodedData)
	fmt.Printf("Before serializing: %+v\n", basicTestData)
	fmt.Printf("After deserializing: %+v\n", decodedRecord)
}
