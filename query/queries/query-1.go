package queries

import (
	"bufio"
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
}

func NewQuery1(m *middleware.Middleware, shardId int, resultInterval int) *Query1 {
	return &Query1{
		middleware:     m,
		shardId:        shardId,
		resultInterval: resultInterval,
		clients:        make(map[string]*Query1Client),
	}
}

func (q *Query1) Run() {
	log.Info("Query 1 running")

	gamesQueue, err := q.middleware.ListenGames(fmt.Sprintf("%d", q.shardId))
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

		if message.Last {
			log.Info("HOLA 2 - Last para client: ", message.ClientId)
		}

		client, exists := q.clients[message.ClientId]
		if !exists {
			client = NewQuery1Client(q.middleware, message.ClientId, q.shardId, q.resultInterval)
			q.clients[message.ClientId] = client
		}

		client.processGame(message)
		return nil
	})

	log.Info("HOLA 1 - Query 1 finished")
}

type Query1Client struct {
	middleware     *middleware.Middleware
	clientId       string
	shardId        int
	processedGames *shared.Processed
	result         middleware.Query1Result
	resultInterval int
}

func NewQuery1Client(m *middleware.Middleware, clientId string, shardId int, resultInterval int) *Query1Client {
	os.Mkdir(fmt.Sprintf("./database/%s", clientId), 0777)
	resultFile, err := os.OpenFile(fmt.Sprintf("./database/%s/query-1.csv", clientId), os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Errorf("Error opening file: %s", err)
	}

	result := middleware.Query1Result{
		Windows: 0,
		Mac:     0,
		Linux:   0,
	}

	reader := bufio.NewReader(resultFile)
	line, err := reader.ReadString('\n')
	if err == nil {
		fields := strings.Split(line, ",")

		if len(fields) == 3 {
			result.Windows, _ = strconv.ParseInt(fields[0], 10, 64)
			result.Linux, _ = strconv.ParseInt(fields[1], 10, 64)
			result.Mac, _ = strconv.ParseInt(fields[2], 10, 64)
		}
	}

	return &Query1Client{
		middleware:     m,
		clientId:       clientId,
		shardId:        shardId,
		processedGames: shared.NewProcessed(fmt.Sprintf("./database/%s/processed.bin", clientId)),
		resultInterval: 500,
		result: middleware.Query1Result{
			Windows: 0,
			Mac:     0,
			Linux:   0,
		},
	}
}

func (qc *Query1Client) processGame(msg *middleware.GameMsg) {
	if msg.Last {
		qc.sendResult(true)
		qc.Close()
		msg.Ack()
		return
	}

	game := msg.Game

	if qc.processedGames.Contains(int32(game.AppId)) {
		log.Infof("Game %d already processed", game.AppId)
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

	// TODO: commit

	qc.processedGames.Add(int32(game.AppId))
	os.Rename(tmpFile.Name(), fmt.Sprintf("./database/%s/query-1.csv", qc.clientId))

	if qc.processedGames.Count()%qc.resultInterval == 0 {
		qc.sendResult(false)
	}

	// TODO: endCommit

	msg.Ack()
}

func (qc *Query1Client) sendResult(final bool) {
	qc.result.Final = final

	resultMsg := &middleware.Result{
		ClientId:       qc.clientId,
		QueryId:        1,
		IsFinalMessage: final,
		Payload:        qc.result,
	}

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

	qc.result = middleware.Query1Result{
		Final:   false,
		Windows: 0,
		Linux:   0,
		Mac:     0,
	}
}

func (qc *Query1Client) Close() {
	// os.RemoveAll(fmt.Sprintf("./database/%s", qc.clientId))
	qc.processedGames.Close()
}
