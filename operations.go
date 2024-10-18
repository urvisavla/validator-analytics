package main

import (
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
)

func getOperationCategory(opType int) string {
	// map for operation types
	operationTypeMap := map[int]string{
		0:  "Account Creation",
		9:  "Account Creation",
		1:  "Payments",
		2:  "Payments",
		13: "Payments",
		3:  "Offers and AMMs",
		4:  "Offers and AMMs",
		12: "Offers and AMMs",
		22: "Offers and AMMs",
		23: "Offers and AMMs",
		6:  "Trust",
		7:  "Trust",
		21: "Trust",
		14: "Claimable Balances",
		15: "Claimable Balances",
		20: "Claimable Balances",
		16: "Sponsorship",
		17: "Sponsorship",
		18: "Sponsorship",
	}

	if opType > 23 {
		return "Soroban"
	}

	// default to 'Other'
	if val, ok := operationTypeMap[opType]; ok {
		return val
	}

	return "Other"
}

type Operations struct {
	Total      int            `json:"total"`
	Categories map[string]int `json:"categories"`
}

func getOperationsStat(transactionReader *ingest.LedgerTransactionReader) (Operations, error) {
	operations := Operations{0, make(map[string]int)}
	for {
		tx, err := transactionReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return operations, fmt.Errorf("could not read transaction %w", err)
		}

		for _, op := range tx.Envelope.Operations() {
			catType := getOperationCategory(int(op.Body.Type))
			operations.Categories[catType]++
		}
		operations.Total += len(tx.Envelope.Operations())
	}
	return operations, nil
}
