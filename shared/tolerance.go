package shared

import (
	"encoding/binary"
	"encoding/csv"
	"io"
	"os"
)

type Processed struct {
	file      *os.File
	processed map[int64]bool
}

func NewProcessed(path string) *Processed {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return nil
	}

	processed := make(map[int64]bool)

	var current int64
	for {
		err := binary.Read(file, binary.BigEndian, &current)
		if err == io.EOF {
			break
		}

		processed[current] = true
	}

	return &Processed{file: file, processed: processed}
}

func (p *Processed) Add(id int64) {
	if p.processed[id] {
		return
	}

	p.processed[id] = true

	err := binary.Write(p.file, binary.BigEndian, int64(id))
	if err != nil {
		log.Errorf("failed to write to file: %v", err)
		return
	}
}

func (p *Processed) Contains(id int64) bool {
	return p.processed[id]
}

func (p *Processed) Count() int {
	return len(p.processed)
}

func (p *Processed) Close() {
	p.file.Close()
}

type Commit struct {
	commit *os.File
	writer *csv.Writer
	Data   [][]string
}

// data: [[filename, tmpFilename],[filename, tmpFilename],[key,value]]

// commitFile:
// filename,tmpFilename
// filename,tmpFilename
// key,value
// END
func NewCommit(path string) *Commit {
	commit, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Errorf("failed to create commit file: %v", err)
		return nil
	}

	writer := csv.NewWriter(commit)

	return &Commit{commit: commit, writer: writer}
}

func (c *Commit) Write(data [][]string) {
	if c.Data != nil {
		log.Infof("Last commit was not ended, last: %v, new: %v", c.Data, data)
	}

	c.Data = data
	c.writer.WriteAll(data)
	c.writer.Write([]string{"END"})
	c.writer.Flush()
}

func (c *Commit) End() {
	c.commit.Truncate(0)
	c.Data = nil
}

func RestoreCommit(path string, onCommit func(commit *Commit)) {
	commitFile, err := os.Open(path)
	if err != nil {
		log.Infof("No commit file found: %v", err) // TODO: remove all logs and trucate commit if corrupted
		return
	}

	reader := csv.NewReader(commitFile)
	reader.FieldsPerRecord = -1

	data, err := reader.ReadAll()
	if err != nil {
		log.Infof("failed to read commit file: %v", err)
		return
	}

	if len(data) == 0 {
		log.Infof("Empty commit file")
		return
	}

	if len(data[len(data)-1]) == 0 {
		log.Infof("Empty commit file (2)")
		return
	}

	if data[len(data)-1][0] != "END" {
		log.Infof("Empty commit file (3)")
		return
	}

	commit := &Commit{commit: commitFile, Data: data[:len(data)-1]}

	onCommit(commit)
}

type Cache[T any] struct {
	cache map[int32]T
}

func NewCache[T any]() *Cache[T] {
	return &Cache[T]{cache: make(map[int32]T)}
}

func (c *Cache[T]) Add(key int32, value T) {
	c.cache[key] = value
}

func (c *Cache[T]) Get(key int32) (T, bool) {
	value, ok := c.cache[key]
	return value, ok
}
