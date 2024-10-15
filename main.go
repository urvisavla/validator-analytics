package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/pelletier/go-toml"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
)

type Message struct {
	Payload interface{}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	cfg, err := toml.LoadFile("config.toml")
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	datastoreConfig := datastore.DataStoreConfig{}
	if err = cfg.Unmarshal(&datastoreConfig); err != nil {
		fmt.Printf("error unmarshalling TOML config: %v\n", err)
		return
	}
	processors := []Processor{&ValidatorProcessor{}}

	reader, err := NewLedgerMetadataReader(&datastoreConfig, network.PublicNetworkhistoryArchiveURLs, processors)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("ingestion pipeline ended %v\n", reader.Run(ctx))
}
