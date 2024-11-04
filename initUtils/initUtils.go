package initUtils

import (
	"KVwithWAL/config"
	"KVwithWAL/logger"
)

func InitializeConfiguration(filepath string) (*config.Config, error) {

	// walRecordType := reflect.TypeOf((*records.WalRecord)(nil)).Elem()

	//This should initialize the header size value, but It turned out to be harder than I thought to build it dynamically, as I need only a few fields from the struct in the header

	// for i := 0; i < walRecordType.NumField(); i++ {
	// 	field := walRecordType.Field(i)
	// 	switch field.Type.Kind() {
	// 	case reflect.String, reflect.Slice:
	// 		continue // Skip string and byte slice fields
	// 	default:
	// 		totalSize += int(field.Type.Size())
	// 	}
	// }
	config, err := config.InitConfig(filepath, 37)
	logger.InitLog()

	return config, err

}
