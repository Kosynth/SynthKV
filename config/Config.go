package config

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type Config struct {
	BlockSize               int               `json:"block_size"`
	CacheSize               int               `json:"cache_size"`
	MemtableSize            int               `json:"memtable_size"`
	WalSegmentSize          int               `json:"wal_segment_size"`
	CompactionAlgorithm     string            `json:"compaction_algorithm"`
	LsmTreeLevels           int               `json:"lsm_tree_levels"`
	TokenBucket             TokenBucketConfig `json:"token_bucket"`
	StaticWALAttributesSize int
	PathToWALFile           string
}

type TokenBucketConfig struct {
	Capacity       int `json:"capacity"`
	RefillInterval int `json:"refill_interval"`
}

var appConfig *Config
var once sync.Once

func InitConfig(filepath string, staticSize int) (*Config, error) {
	var err error
	once.Do(func() {
		// Default values declared here
		appConfig = &Config{
			BlockSize:           4096, //size in bytes, 4096 in my system, not sure how to get this value from os.*
			CacheSize:           1024, //size in blocks 4MB buffer by default
			MemtableSize:        1000,
			WalSegmentSize:      500,
			CompactionAlgorithm: "size-tiered",
			LsmTreeLevels:       4,
			TokenBucket: TokenBucketConfig{
				Capacity:       3,
				RefillInterval: 1,
			},
			StaticWALAttributesSize: staticSize,
			PathToWALFile:           "Records.wal",
		}
		// Overwrite if config file contains values
		file, fileErr := os.Open(filepath)
		if fileErr != nil {
			fmt.Println("Config file not found, using default values.")
			return
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if decodeErr := decoder.Decode(appConfig); decodeErr != nil {
			err = fmt.Errorf("invalid config file: %w", decodeErr)
		}
	})
	return appConfig, err
}

func GetAppConfig() *Config {
	return appConfig
}
