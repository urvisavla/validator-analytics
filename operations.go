package main

import (
	"fmt"
	"io"

	"github.com/stellar/go/ingest"
)

func getOperationCategory(opType int) string {
	// map for operation types
	operationTypeMap := map[int]string{
		0:  "account_creation",
		9:  "account creation",
		1:  "payments",
		2:  "payments",
		13: "payments",
		3:  "offers_and_AMMs",
		4:  "offers_and_AMMs",
		12: "offers_and_AMMs",
		22: "offers_and_AMMs",
		23: "offers_and_AMMs",
		6:  "trust",
		7:  "trust",
		21: "trust",
		14: "claimable_balances",
		15: "claimable_balances",
		20: "claimable_balances",
		16: "sponsorship",
		17: "sponsorship",
		18: "sponsorship",
	}

	if opType > 23 {
		return "soroban"
	}

	// default to 'Other'
	if val, ok := operationTypeMap[opType]; ok {
		return val
	}

	return "other"
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
