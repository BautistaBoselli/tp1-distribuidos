package reducer

import (
	// "encoding/csv"
	// "io"
	"encoding/csv"
	"io"
	"os"
	"strconv"

	// "strconv"
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
		if err := r.processResult(result); err != nil {
			log.Fatalf("action: process result | result: error | message: %s", err)
			return err
		}

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

func (r *ReducerQuery5) processResult(result *middleware.Result) error {

	switch result.Payload.(type) {

	case middleware.Query5Result:
		tmpFile, err := os.Create("tmp-reducer-query-5.csv")
		if err != nil {
			log.Fatalf("action: create file | result: error | message: %s", err)
			return err
		}
		defer tmpFile.Close()
		tmpFileWriter := csv.NewWriter(tmpFile)

		queryStats := result.Payload.(middleware.Query5Result).Stats

		file, err := os.Open("reducer-query-5.csv")
		if err != nil {
			log.Fatalf("action: open file | result: error | message: %s", err)
			return err
		}
		defer file.Close()
		fileReader := csv.NewReader(file)
		record, err := fileReader.Read()

		index := 0

		cutCond := r.totalNegativeReviews / 10
		for i := 0; i < cutCond; i++ {
			if err != nil && err != io.EOF {
				log.Fatalf("action: read file | result: error | message: %s", err)
				return err
			}
			if err == io.EOF {
				valueToWrite := []string{strconv.Itoa(queryStats[index].AppId), queryStats[index].Name, strconv.Itoa(queryStats[index].Negatives)}
				writeToFile(valueToWrite, tmpFileWriter)
				index++
				continue
			}

			negatives, err := strconv.Atoi(record[2])
			if err != nil {
				log.Fatalf("action: convert negative reviews to int | result: error | message: %s", err)
				return err
			}

			if negatives >= queryStats[index].Negatives {
				valueToWrite := []string{record[0], record[1], record[2]}
				writeToFile(valueToWrite, tmpFileWriter)
				record, err = fileReader.Read()
				if err != nil && err != io.EOF {
					log.Fatalf("action: read file | result: error | message: %s", err)
					return err
				}

			} else {
				valueToWrite := []string{strconv.Itoa(queryStats[index].AppId), queryStats[index].Name, strconv.Itoa(queryStats[index].Negatives)}
				writeToFile(valueToWrite, tmpFileWriter)
				index++
			}

		}
		// replace file with tmp file
		if err := os.Rename("tmp-reducer-query-5.csv", "reducer-query-5.csv"); err != nil {
			log.Fatalf("action: rename file | result: error | message: %s", err)
			return err
		}
	}
	// check tmp file is actually deleted
	return nil
}

func writeToFile(value []string, writer *csv.Writer) {
	if err := writer.Write(value); err != nil {
		log.Fatalf("action: write to file | result: error | message: %s", err)
	}
	writer.Flush()
}

func (r *ReducerQuery5) sendFinalResult() {
	file, err := os.Open("reducer-query-5.csv")
	if err != nil {
		log.Fatalf("action: open file | result: error | message: %s", err)
		return
	}
	defer file.Close()

	fileReader := csv.NewReader(file)

	for {
		record, err := fileReader.Read()
		if err == io.EOF {
			result := middleware.Result{
				QueryId:             5,
				IsFragmentedMessage: false,
				IsFinalMessage:      true,
				Payload:             record,
			}
			if err := r.middleware.SendResult("5", &result); err != nil {
				log.Fatalf("action: send final result | result: error | message: %s", err)
				break
			}
			if err != nil {
				log.Fatalf("action: read file query 5 | result: error | message: %s", err)
			}

			result = middleware.Result{
				QueryId:             5,
				IsFragmentedMessage: false,
				IsFinalMessage:      false,
				Payload:             record,
			}

			if err := r.middleware.SendResult("5", &result); err != nil {
				log.Fatalf("action: send final result | result: error | message: %s", err)
			}

		}
	}
}
