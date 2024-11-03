package initUtils

import (
	"KVwithWAL/config"
	"KVwithWAL/model/records"
	"reflect"
)

func InitializeConfiguration(filepath string) (*config.Config, error) {
	totalSize := 0
	walRecordType := reflect.TypeOf((*records.WalRecord)(nil)).Elem()

	for i := 0; i < walRecordType.NumField(); i++ {
		field := walRecordType.Field(i)
		switch field.Type.Kind() {
		case reflect.String, reflect.Slice:
			continue // Skip string and byte slice fields
		default:
			totalSize += int(field.Type.Size())
		}
	}
	return config.InitConfig(filepath, totalSize)
}
