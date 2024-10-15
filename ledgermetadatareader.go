package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/stellar/go/historyarchive"
	"github.com/stellar/go/ingest/cdp"
	"github.com/stellar/go/ingest/ledgerbackend"
	"github.com/stellar/go/support/datastore"
	"github.com/stellar/go/support/storage"
	"github.com/stellar/go/xdr"
)

type LedgerMetadataReader struct {
	processors         []Processor
	historyArchiveURLs []string
	dataStoreConfig    datastore.DataStoreConfig
}

func NewLedgerMetadataReader(config *datastore.DataStoreConfig, historyArchiveUrls []string, processors []Processor) (*LedgerMetadataReader, error) {
	if config == nil {
		return nil, errors.New("missing configuration")
	}
	return &LedgerMetadataReader{processors: processors, dataStoreConfig: *config, historyArchiveURLs: historyArchiveUrls}, nil
}

func (adapter *LedgerMetadataReader) Subscribe(receiver Processor) {
	adapter.processors = append(adapter.processors, receiver)
}

func (a *LedgerMetadataReader) Run(ctx context.Context) error {

	historyArchive, err := historyarchive.NewArchivePool(a.historyArchiveURLs, historyarchive.ArchiveOptions{
		ConnectOptions: storage.ConnectOptions{
			UserAgent: "cdp-hackathon-validators",
			Context:   ctx,
		},
	})
	if err != nil {
		return errors.Wrap(err, "error creating history archive client")
	}
	latestNetworkLedger, err := historyArchive.GetLatestLedgerSequence()

	if err != nil {
		return errors.Wrap(err, "error getting latest ledger")
	}

	ledgerRange := ledgerbackend.UnboundedRange(latestNetworkLedger)

	pubConfig := cdp.PublisherConfig{
		DataStoreConfig:       a.dataStoreConfig,
		BufferedStorageConfig: cdp.DefaultBufferedStorageBackendConfig(a.dataStoreConfig.Schema.LedgersPerFile),
	}
	pubConfig.BufferedStorageConfig.RetryLimit = 20
	pubConfig.BufferedStorageConfig.RetryWait = 3

	fmt.Printf("Starting at ledger %v ...\n", latestNetworkLedger)
	return cdp.ApplyLedgerMetadata(ledgerRange, pubConfig, ctx,
		func(lcm xdr.LedgerCloseMeta) error {
			for _, processor := range a.processors {
				if err = processor.Process(ctx, Message{Payload: lcm}); err != nil {
					return err
				}
			}
			return nil
		})
}
