package queries

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tp1-distribuidos/middleware"
	"tp1-distribuidos/shared"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Query1 struct {
	middleware     *middleware.Middleware
	shardId        int
	resultInterval int
	clients        map[string]*Query1Client
	commit         *shared.Commit
	activeClients  *shared.Processed
}

func NewQuery1(m *middleware.Middleware, shardId int, resultInterval int) *Query1 {
	return &Query1{
		middleware:     m,
		shardId:        shardId,
		resultInterval: resultInterval,
		clients:        make(map[string]*Query1Client),
		commit:         shared.NewCommit("./database/commit.csv"),
		activeClients:  shared.NewProcessed("./database/active_clients.bin"),
	}
}

func (q *Query1) Run() {
	time.Sleep(500 * time.Millisecond)
	log.Info("Query 1 running")

	shared.RestoreCommit("./database/commit.csv", func(commit *shared.Commit) {
		log.Infof("Restored commit: %v", commit)

		os.Rename(commit.Data[0][2], commit.Data[0][3])

		processed := shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", commit.Data[0][0]))

		if processed != nil {
			appId, _ := strconv.Atoi(commit.Data[0][1])
			processed.Add(int64(appId))
		}
	})

	inactiveClientsQueue, err := q.middleware.ListenInactiveClients()
	if err != nil {
		log.Errorf("Error listening inactive clients: %s", err)
		return
	}

	go inactiveClientsQueue.Consume(func(message []byte) error {
		clientId := binary.BigEndian.Uint16(message)
		clientIdStr := strconv.Itoa(int(clientId))
		os.RemoveAll(fmt.Sprintf("./database/%s", clientIdStr))
		return nil
	})

	gamesQueue, err := q.middleware.ListenGames("1."+strconv.Itoa(q.shardId), fmt.Sprintf("%d", q.shardId))
	if err != nil {
		log.Errorf("Error listening games: %s", err)
		return
	}

	metric := shared.NewMetric(10000, func(total int, elapsed time.Duration, rate float64) string {
		return fmt.Sprintf("[Query 1-%d] Processed %d games in %s (%.2f games/s)", q.shardId, total, elapsed, rate)
	})

	cancelWg := &sync.WaitGroup{}
	gamesQueue.Consume(cancelWg, func(message *middleware.GameMsg) error {
		metric.Update(1)

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery1Client(q.middleware, q.commit, message.ClientId, q.shardId, q.resultInterval)
			q.clients[message.ClientId] = client
		}

		client.processGame(message)
		return nil
	})

}

type Query1Client struct {
	middleware     *middleware.Middleware
	commit         *shared.Commit
	clientId       string
	shardId        int
	processedGames *shared.Processed
	result         middleware.Query1Result
	resultInterval int
}

func NewQuery1Client(m *middleware.Middleware, commit *shared.Commit, clientId string, shardId int, resultInterval int) *Query1Client {
	os.Mkdir(fmt.Sprintf("./database/%s", clientId), 0777)
	resultFile, err := os.OpenFile(fmt.Sprintf("./database/%s/query-1.csv", clientId), os.O_CREATE|os.O_RDONLY, 0777)
	if err != nil {
		log.Errorf("Error opening file: %s", err)
	}

	defer resultFile.Close()

	result := middleware.Query1Result{
		Windows: 0,
		Mac:     0,
		Linux:   0,
	}

	reader := bufio.NewReader(resultFile)
	line, err := reader.ReadString('\n')

	if err == nil {
		fields := strings.Split(strings.TrimSuffix(line, "\n"), ",")

		if len(fields) == 3 {
			result.Windows, _ = strconv.ParseInt(fields[0], 10, 64)
			result.Linux, _ = strconv.ParseInt(fields[1], 10, 64)
			result.Mac, _ = strconv.ParseInt(fields[2], 10, 64)
		}
	}

	return &Query1Client{
		middleware:     m,
		commit:         commit,
		clientId:       clientId,
		shardId:        shardId,
		processedGames: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		resultInterval: resultInterval,
		result:         result,
	}
}

func (qc *Query1Client) processGame(msg *middleware.GameMsg) {
	if msg.Last {
		qc.sendResult(true, 0)
		msg.Ack()
		qc.End()
		return
	}

	game := msg.Game

	if qc.processedGames.Contains(int64(game.AppId)) {
		log.Infof("Game %d already processed", game.AppId)

		if qc.processedGames.Count()%qc.resultInterval == 0 {
			qc.sendResult(false, game.AppId)
		}

		msg.Ack()
		return
	}

	if game.Windows {
		qc.result.Windows++
	}
	if game.Linux {
		qc.result.Linux++
	}
	if game.Mac {
		qc.result.Mac++
	}

	tmpFile, err := os.CreateTemp(fmt.Sprintf("./database/%s", qc.clientId), "query-1.csv")
	if err != nil {
		log.Errorf("failed to create temp file: %v", err)
		return
	}

	tmpFile.WriteString(fmt.Sprintf("%d,%d,%d\n", qc.result.Windows, qc.result.Linux, qc.result.Mac))

	shared.TestTolerance(1, 3000, fmt.Sprintf("Exiting after tmp (game %d)", game.AppId))

	realFilename := fmt.Sprintf("./database/%s/query-1.csv", qc.clientId)

	qc.commit.Write([][]string{
		{qc.clientId, strconv.Itoa(game.AppId), tmpFile.Name(), realFilename},
	})

	shared.TestTolerance(1, 3000, fmt.Sprintf("Exiting after creating commit (game %d)", game.AppId))

	qc.processedGames.Add(int64(game.AppId))

	shared.TestTolerance(1, 3000, fmt.Sprintf("Exiting after adding processed game (game %d)", game.AppId))

	os.Rename(tmpFile.Name(), realFilename)

	shared.TestTolerance(1, 3000, fmt.Sprintf("Exiting after renaming (game %d)", game.AppId))

	if qc.processedGames.Count()%qc.resultInterval == 0 {
		qc.sendResult(false, game.AppId)
	}

	shared.TestTolerance(1, 3000, fmt.Sprintf("Exiting after sending result (game %d)", game.AppId))

	qc.commit.End()

	msg.Ack()
}

func (qc *Query1Client) sendResult(final bool, appId int) {
	qc.result.Final = final

	id := qc.getNextId()
	resultMsg := &middleware.Result{
		Id:             id,
		ClientId:       qc.clientId,
		QueryId:        1,
		ShardId:        qc.shardId,
		IsFinalMessage: final,
		Payload:        qc.result,
	}

	shared.TestTolerance(1, 8, "Exiting before sending result")

	if resultMsg.IsFinalMessage {
		log.Infof("Query 1 [FINAL] - Query 1-%d - Windows: %d, Linux: %d, Mac: %d", qc.shardId, qc.result.Windows, qc.result.Linux, qc.result.Mac)

		if err := qc.middleware.SendResult("1", resultMsg); err != nil {
			log.Errorf("Failed to send result: %v", err)
		}

	} else {
		log.Infof("Query 1 [PARTIAL] - Query 1-%d - Windows: %d, Linux: %d, Mac: %d", qc.shardId, qc.result.Windows, qc.result.Linux, qc.result.Mac)

		if err := qc.middleware.SendResult("1", resultMsg); err != nil {
			log.Errorf("Failed to send result: %v", err)
		}
	}

	shared.TestTolerance(1, 8, "Exiting before restarting counter")

	qc.result = middleware.Query1Result{
		Final:   false,
		Windows: 0,
		Linux:   0,
		Mac:     0,
	}
	os.Remove(fmt.Sprintf("./database/%s/query-1.csv", qc.clientId))

}

// clientId (2 bytes) + queryId (1 byte) + shardId (1 byte) + appId (4 bytes)
func (qc *Query1Client) getNextId() int64 {
	clientId, _ := strconv.Atoi(qc.clientId)

	clientIdHigh := (clientId >> 8) & 0xFF // Get high byte
	clientIdLow := clientId & 0xFF         // Get low byte

	processed := qc.processedGames.Count() + 1 // 0 means last

	return int64(clientIdHigh)<<56 | int64(clientIdLow)<<48 | int64(1)<<40 | int64(qc.shardId)<<32 | int64(processed)
}

func (qc *Query1Client) End() {
	os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedGames.Close()
}
