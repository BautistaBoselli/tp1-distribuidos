package reducer

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"tp1-distribuidos/middleware"
)

const resultsBatchSize = 10

type ReducerQuery5 struct {
	middleware           *middleware.Middleware
	pendingFinalAnswers  int
	totalNegativeReviews int
}

func NewReducerQuery5(middleware *middleware.Middleware) (*ReducerQuery5, error) {
	_, err := os.Create("reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: create file | result: error | message: %s", err)
		return nil, err
	}

	return &ReducerQuery5{
		middleware:           middleware,
		pendingFinalAnswers:  2, // por ahora hardcodeado indicando que son 2 nodos mandando
		totalNegativeReviews: 0, // inicializa en 0
	}, nil
}

func (r *ReducerQuery5) Close() {
	r.middleware.Close()
}

func (r *ReducerQuery5) Run() {
	defer r.Close()

	resultsQueue, err := r.middleware.ListenResults("5")
	if err != nil {
		log.Fatalf("action: listen reviews| result: error | message: %s", err)
		return
	}

	resultsQueue.Consume(func(result *middleware.Result, ack func()) error {
		log.Infof("Result: %v", result)
		r.processResult(result)

		ack()

		if result.IsFinalMessage {
			r.pendingFinalAnswers--
		}

		if r.pendingFinalAnswers == 0 {
			r.sendFinalResult()
		}

		return nil
	})
}

func (r *ReducerQuery5) processResult(result *middleware.Result) {

	switch result.Payload.(type) {

	case middleware.Query5Result:
		tmpFile, err := os.Create("tmp-reducer-query-5.csv")
		if err != nil {
			log.Fatalf("action: create file | result: error | message: %s", err)
			return
		}

		defer tmpFile.Close()

		tmpFileWriter := csv.NewWriter(tmpFile)

		queryStats := result.Payload.(middleware.Query5Result).Stats
		reducerQuery5Reader 

		// stats := result.Payload.(middleware.Query5Result)

		// for _, stat := range stats {
		// 	r.totalGames += 1
		// 	r.totalNegativeReviews += stat.Negatives
		// 	r.query5FileWriter.Write([]string{strconv.Itoa(stat.AppId), stat.Name, strconv.Itoa(stat.Negatives)})
		// 	r.query5FileWriter.Flush()
	}
}

func (r *ReducerQuery5) sendFinalResult() {
	queryFileReader := csv.NewReader(r.query5File)
	resultsBuffer := make([]string, resultsBatchSize)
	avgNegativeReviews := r.totalNegativeReviews / r.totalGames
	upperPercentile := avgNegativeReviews * 90 / 100 // recien me doy cuenta que esto esta mal porque no estoy calculando el cuantil 90, sino que estoy calculando el 90% de la media

	for {
		stat, err := queryFileReader.Read()
		if err != nil && err == io.EOF {
			break
		}

		if err != nil {
			log.Errorf("action: read stat of query file | result: error | message: %s", err)
			continue
		}

		negReviews, err := strconv.Atoi(stat[2])
		if err != nil {
			log.Errorf("action: convert negative reviews to int | result: error | message: %s", err)
			continue
		}

		if negReviews > upperPercentile {
			resultsBuffer = append(resultsBuffer, stat[1]) // stat[1] es el AppName
		}

		if len(resultsBuffer) == resultsBatchSize {
			if err := r.middleware.SendResult("5", &middleware.Result{
				QueryId:             5,
				IsFragmentedMessage: false,
				IsFinalMessage:      false,
				Payload:             resultsBuffer,
			}); err != nil {
				log.Errorf("action: send result | result: error | message: %s", err)
			}

			resultsBuffer = make([]string, resultsBatchSize)
		}
	}

	if len(resultsBuffer) > 0 {
		r.middleware.SendResult("5", &middleware.Result{
			QueryId:             5,
			IsFragmentedMessage: false,
			IsFinalMessage:      true,
			Payload:             resultsBuffer,
		})
	}
}
