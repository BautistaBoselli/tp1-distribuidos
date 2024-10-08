package middleware

import (
	"strconv"
	"strings"
)

type Game struct {
	AppId       int
	Name        string
	Year        int
	Genres      []string
	Windows     bool
	Mac         bool
	Linux       bool
	AvgPlaytime int64
}

const appIdIndex = 0
const nameIndex = 1
const yearIndex = 2
const genreIndex = 3
const windowsIndex = 4
const macIndex = 5
const linuxIndex = 6
const avgPlaytimeIndex = 7

func NewGame(record []string) *Game {
	appId, err := strconv.Atoi(record[appIdIndex])
	if err != nil {
		return nil
	}

	year, err := strconv.Atoi(record[yearIndex])
	if err != nil {
		return nil
	}

	avgPlaytime, err := strconv.ParseInt(record[avgPlaytimeIndex], 10, 64)
	if err != nil {
		return nil
	}
	return &Game{
		AppId:       appId,
		Name:        record[nameIndex],
		Year:        year,
		Genres:      strings.Split(record[genreIndex], ";"),
		Windows:     record[windowsIndex] == "true",
		Mac:         record[macIndex] == "true",
		Linux:       record[linuxIndex] == "true",
		AvgPlaytime: avgPlaytime,
	}
}

type GameBatch struct {
	Game *Game
	Last bool
}

type Review struct {
	AppId string
	Text  string
	Score int
}

const appIdIndexReview = 0
const textIndexReview = 1
const scoreIndexReview = 2

func NewReview(record []string) *Review {
	score, err := strconv.Atoi(record[scoreIndexReview])
	if err != nil {
		return nil
	}
	return &Review{
		AppId: record[appIdIndexReview],
		Text:  record[textIndexReview],
		Score: score,
	}
}

type Stats struct {
	AppId     int
	Name      string
	Text      string
	Genres    []string
	Positives int
	Negatives int
}

type Query1Result struct {
	AppId   string
	Name    string
	Windows int64
	Mac     int64
	Linux   int64
	Final   bool
}

type Query2Result struct {
	TopGames []Game
}

type Query3Result struct {
	TopGames []Stats
}

type Query4Result struct {
	Stats []Stats
}

type Query5Result struct {
	Stats []Stats
}

func NewStats(game []string, review *Review) *Stats {
	appId, err := strconv.Atoi(game[appIdIndex])
	if err != nil {
		return nil
	}
	if review.Score > 0 {
		return &Stats{
			AppId:     appId,
			Name:      game[nameIndex],
			Genres:    strings.Split(game[genreIndex], ","),
			Text:      review.Text,
			Positives: 1,
			Negatives: 0,
		}
	}

	return &Stats{
		AppId:     appId,
		Name:      game[nameIndex],
		Genres:    strings.Split(game[genreIndex], ","),
		Text:      review.Text,
		Positives: 0,
		Negatives: 1,
	}
}

type Result struct {
	QueryId             int
	IsFragmentedMessage bool
	IsFinalMessage      bool
	Payload              any
}
