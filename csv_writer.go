package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type CSVWriter struct {
	file    *os.File
	writer  *csv.Writer
	mutex   sync.Mutex
	headers []string
}

func NewCSVWriter(filename string, headers []string) (*CSVWriter, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

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

func (w *CSVWriter) WriteJSON(jsonData []byte) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Convert data to a map
	var mapData map[string]interface{}
	if err := json.Unmarshal(jsonData, &mapData); err != nil {
		return err
	}

	// Prepare the row data
	row := make([]string, len(w.headers))
	for i, header := range w.headers {
		if val, ok := mapData[header]; ok {
			row[i] = fmt.Sprint(val)
		}
	}

	// Write the row
	if err := w.writer.Write(row); err != nil {
		return err
	}

	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) Close() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	w.writer.Flush()
	return w.file.Close()
}
