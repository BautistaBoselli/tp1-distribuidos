package shared

import (
	"fmt"
	"os"
	"strings"
)

type Directory struct {
	DirName string
	Files   []*os.File
}

func (d Directory) Close() {
	for _, file := range d.Files {
		file.Close()
	}
}

func (d Directory) Delete() {
	d.Close()
	os.RemoveAll(d.DirName)
}

func GetFilename(clientId string, queryname string, appId string, shards int) string {
	total := 0
	for i, char := range appId {
		total += int(char) * i
	}
	hash := total % shards
	// return fmt.Sprintf("%s-%d.csv", queryname, hash)
	return fmt.Sprintf("client-%s/%s-%d.csv", clientId, queryname, hash)
}

func InitStoreFiles(clientId string, queryname string, shards int) (Directory, error) {
	dirName := fmt.Sprintf("client-%s", clientId)
	if err := os.Mkdir(dirName, 0755); err != nil {
		return Directory{"", nil}, err
	}

	files := make([]*os.File, shards)
	for i := 0; i < shards; i++ {
		file, err := os.Create(fmt.Sprintf("%s/%s-%d.csv", dirName, queryname, i))
		if err != nil {
			for _, file := range files {
				file.Close()
			}
			return Directory{"", nil}, err
		}
		files[i] = file
	}
	// return files, nil
	return Directory{dirName, files}, nil
}

func GetStoreRWriter(filename string) (*os.File, error) {
	pathArray := strings.Split(filename, "/")
	pathArray = pathArray[:len(pathArray)-1]
	os.MkdirAll(strings.Join(pathArray, "/"), 0755)
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
}

// func GetStoreROnly(filename string) (*os.File, error) {
// 	return os.Open(filename)
// }
