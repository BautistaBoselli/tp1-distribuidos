package queries

import (
	"encoding/csv"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"
)

const QUERY2_TOP_SIZE = 10

type Query2 struct {
	middleware *middleware.Middleware
	shardId    int
	clients    map[string]*Query2Client
	commit     *shared.Commit
}

func NewQuery2(m *middleware.Middleware, shardId int) *Query2 {

	return &Query2{
		middleware: m,
		shardId:    shardId,
		clients:    make(map[string]*Query2Client),
		commit:     shared.NewCommit("./database/commit.csv"),
	}
}

func (q *Query2) Run() {
	log.Info("Query 2 running")

	shared.RestoreCommit("./database/commit.csv", func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][1], commit.Data[0][2])

		processed := shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", commit.Data[0][0]))

		if processed != nil {
			appId, _ := strconv.Atoi(commit.Data[0][0])
			processed.Add(int64(appId))
		}
	})

	gamesQueue, err := q.middleware.ListenGames("2."+strconv.Itoa(q.shardId), fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 2-%d] Processed %d games in %s (%.2f games/s)", q.shardId, total, elapsed, rate)
	})

	cancelWg := &sync.WaitGroup{}
	gamesQueue.Consume(cancelWg, func(message *middleware.GameMsg) error {
		metric.Update(1)

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery2Client(q.middleware, q.commit, message.ClientId, q.shardId)
			q.clients[message.ClientId] = client
		}

		client.processGame(message)
		return nil
	})
}

type Query2Client struct {
	middleware     *middleware.Middleware
	commit         *shared.Commit
	clientId       string
	shardId        int
	processedGames *shared.Processed
	result         middleware.Query2Result
}

func NewQuery2Client(m *middleware.Middleware, commit *shared.Commit, clientId string, shardId int) *Query2Client {
	os.Mkdir(fmt.Sprintf("./database/%s", clientId), 0777)
	resultFile, err := os.OpenFile(fmt.Sprintf("./database/%s/query-2.csv", clientId), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		log.Errorf("Error opening file: %s", err)
	}

	result := middleware.Query2Result{
		TopGames: make([]middleware.Game, 0),
	}

	reader := csv.NewReader(resultFile)
	lines, err := reader.ReadAll()
	if err == nil {
		for _, line := range lines {
			appId, _ := strconv.Atoi(line[0])
			avgPlaytime, _ := strconv.ParseInt(line[2], 10, 64)

			game := middleware.Game{
				AppId:       appId,
				Name:        line[1],
				AvgPlaytime: avgPlaytime,
			}

			result.TopGames = append(result.TopGames, game)
		}
	}

	return &Query2Client{
		middleware:     m,
		commit:         commit,
		clientId:       clientId,
		shardId:        shardId,
		processedGames: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		result:         result,
	}
}

func (qc *Query2Client) processGame(msg *middleware.GameMsg) {
	if msg.Last {
		qc.sendResult()
		qc.End()
		msg.Ack()
		return
	}

	game := msg.Game

	if qc.processedGames.Contains(int64(game.AppId)) {
		log.Infof("Game %d already processed", game.AppId)
		msg.Ack()
		return
	}

	if game.Year < 2010 || game.Year > 2019 || !slices.Contains(game.Genres, "Indie") {
		msg.Ack()
		return
	}

	// Find the position where the new game should be inserted
	insertIndex := len(qc.result.TopGames)
	for i, topGame := range qc.result.TopGames {
		if game.AvgPlaytime > topGame.AvgPlaytime {
			insertIndex = i
			break
		}
	}

	// If the game should be in the top TOP_SIZE
	if insertIndex < QUERY2_TOP_SIZE {
		qc.result.TopGames = append(qc.result.TopGames, middleware.Game{})
		copy(qc.result.TopGames[insertIndex+1:], qc.result.TopGames[insertIndex:])
		qc.result.TopGames[insertIndex] = *game
	}

	if len(qc.result.TopGames) > QUERY2_TOP_SIZE {
		qc.result.TopGames = qc.result.TopGames[:QUERY2_TOP_SIZE]
	}

	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s", qc.clientId), "query-2.csv")
	if err != nil {
		log.Errorf("failed to create temp file: %v", err)
		return
	}

	writer := csv.NewWriter(tmpFile)
	for _, game := range qc.result.TopGames {
		writer.Write([]string{strconv.Itoa(game.AppId), game.Name, strconv.FormatInt(game.AvgPlaytime, 10)})
	}
	writer.Flush()

	realFilename := fmt.Sprintf("./database/%s/query-2.csv", qc.clientId)

	qc.commit.Write([][]string{
		{strconv.Itoa(game.AppId), tmpFile.Name(), realFilename},
	})

	qc.processedGames.Add(int64(game.AppId))
	os.Rename(tmpFile.Name(), realFilename)

	qc.commit.End()

	msg.Ack()
}

func (qc *Query2Client) sendResult() {
	log.Infof("Query 2 [FINAL]")

	result := middleware.Result{
		ClientId:       qc.clientId,
		QueryId:        2,
		IsFinalMessage: true,
		Payload:        qc.result,
	}

	if err := qc.middleware.SendResult("2", &result); err != nil {
		log.Errorf("Failed to send result: %v", err)
	}
	for i, game := range qc.result.TopGames {
		log.Debugf("Top %d game: %s (%d)", i+1, game.Name, game.AvgPlaytime)
	}
}

func (qc *Query2Client) End() {
	// os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedGames.Close()
}
