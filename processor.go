package main

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/stellar/go/ingest"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/time"
	"github.com/stellar/go/xdr"
)

type Validator struct {
	SequenceNumber uint32     `json:"sequence_number"`
	NodeId         string     `json:"node_id"`
	Signature      string     `json:"signature"`
	Name           string     `json:"name"`
	CloseTime      int64      `json:"close_time"`
	Operations     Operations `json:"operations"`
	Network        string     `json:"network"`
}

type Processor interface {
	Process(context.Context, Message) error
}

type processor struct {
	outboundAdapters []OutboundAdapter
}

func updateMetrics(data Validator) {

	LedgerClosed.WithLabelValues(data.Name, data.NodeId).Set(float64(data.SequenceNumber))
	OperationsTotal.WithLabelValues(data.Name, data.NodeId).Observe(float64(data.Operations.Total))

	// Update operations by category
	for category, count := range data.Operations.Categories {
		OperationsByCategory.WithLabelValues(data.Name, data.NodeId, category).Observe(float64(count))
	}
}

func (p *processor) Process(ctx context.Context, msg Message) error {
	ledgerCloseMeta, err := p.extractLedgerCloseMeta(msg)
	if err != nil {
		return err
	}

	transactionReader, err := p.createTransactionReader(ledgerCloseMeta)
	if err != nil {
		return err
	}

	validator, err := p.extractValidatorInfo(ledgerCloseMeta)
	if err != nil {
		return err
	}

	validator.Operations, err = getOperationsStat(transactionReader)
	if err != nil {
		return err
	}

	fmt.Printf("%s Ledger: %s Validator: %s Operations:%v \n", time.Now().ToTime(), validator.SequenceNumber, validator.Name, validator.Operations)
	return p.sendValidatorInfo(ctx, validator)
}

func (p *processor) extractLedgerCloseMeta(msg Message) (xdr.LedgerCloseMeta, error) {
	ledgerCloseMeta, ok := msg.Payload.(xdr.LedgerCloseMeta)
	if !ok {
		return xdr.LedgerCloseMeta{}, fmt.Errorf("invalid payload type")
	}
	return ledgerCloseMeta, nil
}

func (p *processor) createTransactionReader(ledgerCloseMeta xdr.LedgerCloseMeta) (*ingest.LedgerTransactionReader, error) {
	return ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(network.PublicNetworkPassphrase, ledgerCloseMeta)
}

func (p *processor) extractValidatorInfo(ledgerCloseMeta xdr.LedgerCloseMeta) (Validator, error) {
	ledgerHeader := ledgerCloseMeta.LedgerHeaderHistoryEntry().Header
	LedgerCloseValueSignature, ok := ledgerHeader.ScpValue.Ext.GetLcValueSignature()
	if !ok {
		return Validator{}, fmt.Errorf("ledger close value signature not found")
	}

	nodeID, err := getAddress(LedgerCloseValueSignature.NodeId)
	if err != nil {
		return Validator{}, err
	}

	signature := base64.StdEncoding.EncodeToString(LedgerCloseValueSignature.Signature)
	return Validator{
		SequenceNumber: ledgerCloseMeta.LedgerSequence(),
		NodeId:         nodeID,
		Signature:      signature,
		Name:           getValidatorName(nodeID),
		CloseTime:      ledgerCloseMeta.LedgerCloseTime(),
		Network:        network.PublicNetworkPassphrase,
	}, nil
}

// sendValidatorInfo marshals and sends the validator information.
func (p *processor) sendValidatorInfo(ctx context.Context, validator Validator) error {
	updateMetrics(validator)
	for _, a := range p.outboundAdapters {
		err := a.Write(ctx, Message{Payload: validator})
		if err != nil {
			fmt.Println("Error sending Validator info to outbound adapter:", err)
			return err
		}
	}
	return nil
}
