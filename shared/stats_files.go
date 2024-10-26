package shared

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func UpsertStatsFile(clientId string, queryname string, shards int, message *middleware.Stats) *middleware.Stats {
	file, err := GetStoreRWriter(GetFilename(clientId, queryname, strconv.Itoa(message.AppId), shards))
	if err != nil {
		log.Errorf("Error opening file: %s", err)
		return nil
	}
	defer file.Close()

	tempFile, err := os.CreateTemp("", fmt.Sprintf("temp-stats-%d.csv", message.AppId))
	if err != nil {
		log.Errorf("Error creating temp file: %s", err)
		return nil
	}
	defer os.Remove(tempFile.Name())

	reader := csv.NewReader(file)
	writer := csv.NewWriter(tempFile)

	var updatedStat *middleware.Stats

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Errorf("Error reading CSV: %s", err)
			return nil
		}
		if record[0] == strconv.Itoa(message.AppId) {
			stat, err := ParseStat(record)
			if err != nil {
				log.Errorf("Error parsing stat: %s", err)
				continue
			}
			stat.Positives += message.Positives
			stat.Negatives += message.Negatives
			updatedStat = stat

			record[3] = strconv.Itoa(stat.Positives)
			record[4] = strconv.Itoa(stat.Negatives)
		}

		writer.Write(record)
	}

	if updatedStat == nil {
		writer.Write([]string{
			strconv.Itoa(message.AppId),
			message.Name,
			strings.Join(message.Genres, ";"),
			strconv.Itoa(message.Positives),
			strconv.Itoa(message.Negatives),
		})
	}

	writer.Flush()
	tempFile.Close()
	file.Name()

	os.Rename(tempFile.Name(), file.Name())

	if updatedStat != nil {
		return updatedStat
	}
	return message
}

func GetTopStats(queryname string, cant int, compare func(a *middleware.Stats, b *middleware.Stats) bool) []middleware.Stats {
	top := make([]middleware.Stats, 0)

	for i := range 100 {
		file, err := os.Open(fmt.Sprintf("%s-%d.csv", queryname, i))
		if err != nil {
			log.Errorf("Error opening file: %s", err)
			return nil
		}
		defer file.Close()

		reader := csv.NewReader(file)

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("Error reading file: %s", err)
			}

			newStat, err := ParseStat(record)
			if err != nil {
				log.Errorf("Error parsing stat: %s", err)
				continue
			}

			place := -1
			for i, topStat := range top {
				if compare(newStat, &topStat) {
					place = i
					break
				}
			}

			if len(top) < cant {
				top = append(top, *newStat)
			} else if place != -1 {
				top[cant-1] = *newStat
			}

			sort.Slice(top, func(i, j int) bool {
				return compare(&top[i], &top[j])
			})
		}
	}

	return top
}

func ParseStat(record []string) (*middleware.Stats, error) {
	positives, err := strconv.Atoi(record[3])
	if err != nil {
		return nil, err
	}

	appId, err := strconv.Atoi(record[0])
	if err != nil {
		return nil, err
	}

	negatives, err := strconv.Atoi(record[4])
	if err != nil {
		return nil, err
	}

	return &middleware.Stats{
		AppId:     appId,
		Name:      record[1],
		Genres:    strings.Split(record[2], ";"),
		Positives: positives,
		Negatives: negatives,
	}, nil
}

/******* FS HASHMAP ********/

func UpsertStats(clientId string, game *middleware.Stats) error {
	os.MkdirAll(fmt.Sprintf("./database/%s", clientId), 0644)
	file, err := os.OpenFile(fmt.Sprintf("./database/%s/%d.csv", clientId, game.AppId), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("failed to get file stat: %v", err)
	}

	if stat.Size() > 0 {
		reader := csv.NewReader(file)

		record, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read line: %v", err)
		}

		prevStat, err := ParseStat(record)
		if err != nil {
			log.Errorf("Error parsing stat: %s", err)
			return err
		}

		game.Positives += prevStat.Positives
		game.Negatives += prevStat.Negatives

		_, err = file.Seek(0, 0)
		if err != nil {
			log.Fatalf("failed to seek to start of file: %v", err)
		}
	}

	writer := csv.NewWriter(file)

	err = writer.Write([]string{strconv.Itoa(game.AppId), game.Name, strings.Join(game.Genres, ","), strconv.Itoa(game.Positives), strconv.Itoa(game.Negatives)})
	if err != nil {
		log.Fatalf("failed to write to file: %v", err)
	}

	writer.Flush()

	return nil
}

func GetTopStatsFS(clientId string, cant int, compare func(a *middleware.Stats, b *middleware.Stats) bool) []middleware.Stats {
	top := make([]middleware.Stats, 0)

	dentries, err := os.ReadDir(fmt.Sprintf("./database/%s", clientId))
	if err != nil {
		log.Fatalf("failed to read directory: %v", err)
	}

	log.Infof("Found %d files at ./database/%s", len(dentries), clientId)

	for _, dentry := range dentries {
		log.Infof("Processing file: %s", dentry.Name())
		file, err := os.Open(fmt.Sprintf("./database/%s/%s", clientId, dentry.Name()))
		if err != nil {
			log.Fatalf("failed to open file: %v", err)
		}

		reader := csv.NewReader(file)

		record, err := reader.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("failed to read line: %v", err)
		}

		newStat, err := ParseStat(record)
		if err != nil {
			log.Errorf("Error parsing stat: %s", err)
			continue
		}

		place := -1
		for i, topStat := range top {
			if compare(newStat, &topStat) {
				place = i
				break
			}
		}

		if len(top) < cant {
			top = append(top, *newStat)
		} else if place != -1 {
			top[cant-1] = *newStat
		}

		sort.Slice(top, func(i, j int) bool {
			return compare(&top[i], &top[j])
		})
	}

	return top

}
