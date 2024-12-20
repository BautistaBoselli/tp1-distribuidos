package middleware

import (
	"slices"
	"strconv"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
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
const genreIndex = 36
const windowsIndex = 17
const macIndex = 18
const linuxIndex = 19
const avgPlaytimeIndex = 29

func NewGame(record []string) *Game {
	appId, err := strconv.Atoi(record[appIdIndex])
	if err != nil {
		return nil
	}

	if len(record[nameIndex]) == 0 {
		return nil
	}

	year, err := strconv.Atoi(record[yearIndex][len(record[yearIndex])-4:])
	if err != nil {
		return nil
	}

	avgPlaytime, err := strconv.ParseInt(record[avgPlaytimeIndex], 10, 64)
	if err != nil {
		return nil
	}

	if len(record[genreIndex]) == 0 {
		return nil
	}

	genres := strings.Split(record[genreIndex], ",")

	game := &Game{
		AppId:       appId,
		Name:        record[nameIndex],
		Year:        year,
		Genres:      genres,
		Windows:     record[windowsIndex] == "True",
		Mac:         record[macIndex] == "True",
		Linux:       record[linuxIndex] == "True",
		AvgPlaytime: avgPlaytime,
	}
	return game
}

type GameMsg struct {
	ClientId string
	ShardId  int
	Game     *Game
	Last     bool
	msg      amqp.Delivery
}

func (g *GameMsg) Ack() {
	g.msg.Ack(false)
}

type Review struct {
	Id    int
	AppId string
	Text  string
	Score int
}

const appIdIndexReview = 0
const textIndexReview = 2
const scoreIndexReview = 3

func NewReview(record []string, id int) *Review {
	score, err := strconv.Atoi(record[scoreIndexReview])
	if err != nil {
		return nil
	}

	if len(record[textIndexReview]) == 0 {
		return nil
	}

	return &Review{
		Id:    id,
		AppId: record[appIdIndexReview],
		Text:  record[textIndexReview],
		Score: score,
	}
}

type ReviewsMsg struct {
	Id        int
	ClientId  string
	Reviews   []Review
	Last      int
	Processed map[int]int
	msg       amqp.Delivery
}

func (r *ReviewsMsg) Ack() {
	r.msg.Ack(false)
}

func (r *ReviewsMsg) Nack() {
	r.msg.Nack(false, true)
}

type ReviewsProcessedMsg struct {
	ClientId string
	BatchId  int
	msg      amqp.Delivery
}

func (r *ReviewsProcessedMsg) Ack() {
	r.msg.Ack(false)
}

type Stats struct {
	Id        int
	AppId     int
	Name      string
	Text      string
	Genres    []string
	Positives int
	Negatives int
}

func NewStats(game []string, review *Review) *Stats {
	appId, err := strconv.Atoi(game[appIdIndex])
	if err != nil {
		return nil
	}

	if len(game[1]) == 0 {
		return nil
	}

	genres := strings.Split(game[3], ",")
	if !slices.Contains(genres, "Action") {
		review.Text = ""
	}

	if review.Score > 0 {
		return &Stats{
			Id:        review.Id,
			AppId:     appId,
			Name:      game[1],
			Genres:    genres,
			Text:      review.Text,
			Positives: 1,
			Negatives: 0,
		}
	}

	return &Stats{
		Id:        review.Id,
		AppId:     appId,
		Name:      game[1],
		Genres:    genres,
		Text:      review.Text,
		Positives: 0,
		Negatives: 1,
	}
}

type StatsMsg struct {
	ClientId string
	Stats    *Stats
	Last     bool
	msg      amqp.Delivery
}

func (s *StatsMsg) Ack() {
	s.msg.Ack(false)
}

type Result struct {
	Id             int64
	ClientId       string
	QueryId        int
	ShardId        int
	IsFinalMessage bool
	Payload        interface{}
	msg            amqp.Delivery
}

func (r *Result) Ack() {
	r.msg.Ack(false)
}

type Query1Result struct {
	Windows int64
	Mac     int64
	Linux   int64
	Final   bool
}

type Query2Result struct {
	TopGames []Game
}

type Query3Result struct {
	TopStats []Stats
}

type Query4Result struct {
	Game string
}

type Query5Result struct {
	Stats []Stats
}

type ClientsFinishedMsg struct {
	ClientId int
	msg      amqp.Delivery
}

func (c *ClientsFinishedMsg) Ack() {
	c.msg.Ack(false)
}
