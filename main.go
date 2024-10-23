package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"unsafe"

	"github.com/pelletier/go-toml"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go/network"
	"github.com/stellar/go/support/datastore"
)

type Config struct {
	StartLedger uint32
	EndLedger   uint32
	EnableCSV   bool
	CSVPath     string
	ConfigPath  string
	MetricsPort int
}

// parseFlags parses command line arguments into a Config struct
func parseFlags() *Config {
	cfg := &Config{}

	flag.UintVar((*uint)(unsafe.Pointer(&cfg.StartLedger)), "start-ledger", 0, "Starting ledger sequence number (0 means latest)")
	flag.UintVar((*uint)(unsafe.Pointer(&cfg.EndLedger)), "end-ledger", 0, "Ending ledger sequence number (0 means unbounded)")
	flag.BoolVar(&cfg.EnableCSV, "enable-csv", false, "Enable CSV output")
	flag.StringVar(&cfg.CSVPath, "csv-path", "ledger_data.csv", "Path to CSV output file")
	flag.StringVar(&cfg.ConfigPath, "config", "config.toml", "Path to TOML configuration file")
	flag.IntVar(&cfg.MetricsPort, "metrics-port", 8080, "Port for Prometheus metrics")

	flag.Parse()
	return cfg
}

type Message struct {
	Payload interface{}
}

type OutboundAdapter interface {
	Write(ctx context.Context, msg Message) error
	Close()
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
	cmdFlags := parseFlags()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	go func() {
		metricsAddr := fmt.Sprintf(":%d", cmdFlags.MetricsPort)
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		http.ListenAndServe(metricsAddr, nil)
	}()

	cfg, err := toml.LoadFile(cmdFlags.ConfigPath)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}

	datastoreConfig := datastore.DataStoreConfig{}
	if err = cfg.Unmarshal(&datastoreConfig); err != nil {
		fmt.Printf("error unmarshalling TOML config: %v\n", err)
		return
	}

	// Create and register outbound adapters
	var outboundAdapters []OutboundAdapter
	zeroMQOutboundAdapter, err := NewZeroMQOutboundAdapter()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	outboundAdapters = append(outboundAdapters, zeroMQOutboundAdapter)

	if cmdFlags.EnableCSV {
		writer, err := NewCSVWriter(cmdFlags.CSVPath)
		if err != nil {
			log.Printf("%v\n", err)
			return
		}
		outboundAdapters = append(outboundAdapters, writer)
	}

	processors := []Processor{&processor{
		outboundAdapters: outboundAdapters,
	}}

	reader, err := NewLedgerMetadataReader(
		&datastoreConfig,
		network.PublicNetworkhistoryArchiveURLs,
		processors,
		cmdFlags.StartLedger,
		cmdFlags.EndLedger,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	log.Printf("ingestion pipeline ended %v\n", reader.Run(ctx))
	for _, adapter := range outboundAdapters {
		adapter.Close()
	}
}
