package shared

import (
	"fmt"
	"os"
)

func GetFilename(queryname string, appId string, shards int) string {
	total := 0
	for i, char := range appId {
		total += int(char) * i
	}
	hash := total % shards
	return fmt.Sprintf("%s-%d.csv", queryname, hash)
}

func InitStoreFiles(queryname string, shards int) ([]*os.File, error) {
	files := make([]*os.File, shards)
	for i := 0; i < shards; i++ {
		file, err := os.Create(fmt.Sprintf("%s-%d.csv", queryname, i))
		if err != nil {
			for _, file := range files {
				file.Close()
			}
			return nil, err
		}
		files[i] = file
	}
	return files, nil
}

func GetStoreRWriter(filename string, appId string, shards int) (*os.File, error) {
	filename = GetFilename(filename, appId, shards)
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
}

func GetStoreROnly(filename string, appId string, shards int) (*os.File, error) {
	filename = GetFilename(filename, appId, shards)
	return os.Open(filename)
}
