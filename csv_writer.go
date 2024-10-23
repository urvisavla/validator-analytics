package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type CSVWriter struct {
	file    *os.File
	writer  *csv.Writer
	headers []string
}

func NewCSVWriter(filename string) (*CSVWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	headers := []string{"sequence_number", "node_id", "signature", "name", "close_time", "operations_json", "network"}

	writer := csv.NewWriter(file)
	writer.Comma = ',' // Use comma as separator
	err = writer.Write(headers)
	if err != nil {
		return nil, err
	}

	return &CSVWriter{
		file:    file,
		writer:  writer,
		headers: headers,
	}, nil
}

func (w *CSVWriter) Write(ctx context.Context, msg Message) error {
	validator := msg.Payload.(Validator)
	operationsJSON, err := json.Marshal(validator.Operations)
	if err != nil {
		fmt.Println("Error marshaling operations to JSON:", err)
		return err
	}

	// Prepare the row data
	row := []string{
		strconv.Itoa(int(validator.SequenceNumber)),
		validator.NodeId,
		validator.Signature,
		validator.Name,
		strconv.FormatInt(validator.CloseTime, 10),
		string(operationsJSON), // operations as json in CSV
		validator.Network,
	}

	// Write the row
	if err := w.writer.Write(row); err != nil {
		return err
	}

	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) Close() {
	w.writer.Flush()
	err := w.file.Close()
	fmt.Printf("error closing CSV file %v", err)
}
