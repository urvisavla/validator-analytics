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
	"github.com/zeromq/goczmq"
)

type Message struct {
	Payload interface{}
}

type ZeroMQOutboundAdapter struct {
	Publisher *goczmq.Sock
}

func (adapter *ZeroMQOutboundAdapter) Process(ctx context.Context, msg Message) error {
	_, err := adapter.Publisher.Write(msg.Payload.([]byte))
	return err
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
	publisher, err := goczmq.NewPub("tcp://127.0.0.1:5555")
	if err != nil {
		log.Printf("error creating 0MQ publisher: %v\n", err)
		return
	}
	defer publisher.Destroy()

	outboundAdapter := &ZeroMQOutboundAdapter{Publisher: publisher}

	processors := []Processor{&processor{outboundAdapter: outboundAdapter}}

	reader, err := NewLedgerMetadataReader(&datastoreConfig, network.PublicNetworkhistoryArchiveURLs, processors)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("ingestion pipeline ended %v\n", reader.Run(ctx))
}
