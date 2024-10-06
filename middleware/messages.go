package middleware

import (
	"strconv"
	"strings"
)

type Game struct {
	AppId       string
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
	year, err := strconv.Atoi(record[yearIndex])
	if err != nil {
		return nil
	}

	avgPlaytime, err := strconv.ParseInt(record[avgPlaytimeIndex], 10, 64)
	if err != nil {
		return nil
	}
	return &Game{
		AppId:       record[appIdIndex],
		Name:        record[nameIndex],
		Year:        year,
		Genres:      strings.Split(record[genreIndex], ","),
		Windows:     record[windowsIndex] == "true",
		Mac:         record[macIndex] == "true",
		Linux:       record[linuxIndex] == "true",
		AvgPlaytime: avgPlaytime,
	}
}

type GameBatch struct {
	Games []Game
	Last  bool
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
	AppId     string
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
	AppId       string
	Name        string
	AvgPlaytime int64
	Final       bool
	// Year 	  int ponerle que devuelva esto?
}

type Query3Result struct {
	AppId                 string
	Name                  string
	PositiveReviewsAmount int
}

type Query4Result struct {
	AppId string
	Name  string
}

type Query5Result struct {
	AppId string
	Name  string
	NegativeReviewsAmount int
	TotalReviewsAmount    int
}