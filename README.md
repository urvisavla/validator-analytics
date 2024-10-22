# Validator Analytics Tool

This tool is designed to monitor and analyze [validator](https://developers.stellar.org/docs/validators) behavior on the Stellar network to identify potential biases.
It leverages existing CDP components such as the data lake of Stellar ledger metadata created by [Galexie](https://github.com/stellar/go/tree/master/services/galexie) and stellar [ingestion](https://github.com/stellar/go/tree/master/ingest) sdk. 


![alt text](/architecture.png)


## Prequisites:
To use this tool, you'll need Go, Python, and other programs like ZeroMQ and Jupyter Notebook. 
Additionally, you'll need a GCS bucket containing ledger data and local Google credentials to access it.

## Setup
1. Update the destination_bucket_path in config.toml
2. `sh go build -o validator-info`
3. `./validator-info`
It will begin streaming data about ledgers as they are finalized on the network, starting with the most recent checkpoint.

4. To fetch historical data, run following. This writes data to temp.csv file:
   `./validator-info --start-ledger 49715711 --end-ledger 49715721`

5. Install python 3.11
6. `sh pip3 install -r requirements.txt`
7. Run Jupyter notebook using `sh python3 -m notebook`
8. Run play button on the notebook.
Above will fetch data from stream and write metrics to `graph.html`


### Prometheus and grafana setup (Optional)
You can view the metrics directly by visiting `http://localhost:8080/metrics` in your browser. You can export the metrics to Prometheus and visualize them using Grafana.
We included a [grafana.json](https://github.com/urvisavla/validator-analytics/blob/main/dashboards/grafana.json) file with pre-built dashboard to help you get started.

If you want to spin up a local instance of Prometheus and Grafana, follow the steps below. 

1. Run following commands
```sh
docker pull prom/prometheus:latest

docker network create grafana-prometheus

docker run --rm --name my-prometheus --network grafana-prometheus --network-alias prometheus --publish 9090:9090 --volume /<path>/prometheus.yml:/etc/prometheus/prometheus.yml --detach prom/prometheus

docker pull grafana/grafana-oss:latest

docker run --rm --name grafana --network grafana-prometheus --network-alias grafana --publish 3000:3000 --detach grafana/grafana-oss:latest
```
2. Access Grafana on http://localhost:3000
