package shared

import (
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

func GetStoreRWriter(filename string) (*os.File, error) {
	pathArray := strings.Split(filename, "/")
	pathArray = pathArray[:len(pathArray)-1]
	os.MkdirAll(strings.Join(pathArray, "/"), 0755)
	return os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
}
