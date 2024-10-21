package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"

	"github.com/pelletier/go-toml"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

var registry = prometheus.NewRegistry()
var namespace = "validator_bias"

var OperationsTotal = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace: namespace,
		Name:      "total_operations",
	},
	[]string{"name", "node_id"},
)
var OperationsByCategory = prometheus.NewSummaryVec(
	prometheus.SummaryOpts{
		Namespace: namespace,
		Name:      "operations_by_category",
	},
	[]string{"name", "node_id", "category"},
)

var LedgerClosed = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: namespace,
	Name:      "latest_ledger_closed",
},
	[]string{"name", "node_id"},
)

func init() {

	registry.MustRegister(LedgerClosed)
	registry.MustRegister(OperationsTotal)
	registry.MustRegister(OperationsByCategory)
	fmt.Println("Prometheus metrics registered")
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	go func() {
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		http.ListenAndServe(":8080", nil)
	}()

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
	writer, err := NewCSVWriter("temp.csv", []string{"sequence_number", "node_id", "signature", "name", "close_time", "operations", "network"})
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	processors := []Processor{&processor{
		outboundAdapter: outboundAdapter,
		csvWriter:       writer,
	}}

	reader, err := NewLedgerMetadataReader(&datastoreConfig, network.PublicNetworkhistoryArchiveURLs, processors)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("ingestion pipeline ended %v\n", reader.Run(ctx))
}
