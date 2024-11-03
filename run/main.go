package main

import (
	"KVwithWAL/config"
	"KVwithWAL/initUtils"
	"KVwithWAL/model/records"
	"KVwithWAL/storageTools"
	"fmt"
)

func main() {

	//EVERYTHING BELOW THIS LINE IS FOR TESTING PURPOSES

	//Create a new configuration, from file, if possible
	cfg, _ := initUtils.InitializeConfiguration("config/appConfig.json")
	fmt.Printf("Config: %+v\n", cfg)

	//Tests for records serialization and deserialization
	basicTestData := records.CreateRecord(records.Put, "key", []byte("value"))
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

	//Tests for saving records to a file via BlockManager
	data := []byte{}
	file := "testFile.txt"
	blockManager := storageTools.NewBlockManager(file)
	randomRecords := records.GenerateRandomRecords()
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
