package shared

import (
	"encoding/binary"
	"io"
	"os"
)

type Processed struct {
	file      *os.File
	processed map[int32]bool
}

func NewProcessed(path string) *Processed {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return nil
	}

	processed := make(map[int32]bool)

	var current int32
	for {
		err := binary.Read(file, binary.BigEndian, &current)
		if err == io.EOF {
			break
		}

		processed[current] = true
	}

	return &Processed{file: file, processed: processed}
}

func (p *Processed) Add(id int32) {
	p.processed[id] = true

	err := binary.Write(p.file, binary.BigEndian, int32(id))
	if err != nil {
		log.Errorf("failed to write to file: %v", err)
		return
	}
}

func (p *Processed) Contains(id int32) bool {
	return p.processed[id]
}

func (p *Processed) Count() int {
	return len(p.processed)
}

func (p *Processed) Close() {
	p.file.Close()
}
