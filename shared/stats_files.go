package shared

import (
	"encoding/csv"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"tp1-distribuidos/middleware"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

func UpsertStatsFile(filename string, message *middleware.Stats) *middleware.Stats {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Errorf("Error opening file: %s", err)
		return nil
	}
	defer file.Close()

	tempFile, err := os.CreateTemp("", "temp-stats.csv")
	if err != nil {
		log.Errorf("Error creating temp file: %s", err)
		return nil
	}
	defer tempFile.Close()

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
			stat, err := parseStat(record)
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
	file.Close()

	os.Rename(tempFile.Name(), filename)

	if updatedStat != nil {
		return updatedStat
	}
	return message
}

func GetTopStats(filename string, cant int, compare func(a *middleware.Stats, b *middleware.Stats) bool) []middleware.Stats {
	top := make([]middleware.Stats, 0)

	file, err := os.Open(filename)
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

		newStat, err := parseStat(record)
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

func parseStat(record []string) (*middleware.Stats, error) {
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