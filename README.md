# cdp-hackathon

## Setup

1. Install Go
2. Update the destination_bucket_path in config.toml
3. `sh go build -o validator-info`
4. `./validator-info`
Above will fetch ledger close data and write data to stream

To fetch historical data, run following. This writes data to temp.csv file:
   `./validator-info --start-ledger 49715711 --end-ledger 49715721`

5. Install python 3.11
6. `sh pip3 install -r requirements.txt`
7. Run Jupyter notebook using `sh python3 -m notebook`
8. Run play button on the notebook.
Above will fetch data from stream and write metrics to `graph.html`

### Prometheus and grafana setup

1. Run following commands
```sh
docker pull prom/prometheus:latest

docker network create grafana-prometheus

docker run --rm --name my-prometheus --network grafana-prometheus --network-alias prometheus --publish 9090:9090 --volume /<path>/prometheus.yml:/etc/prometheus/prometheus.yml --detach prom/prometheus

docker pull grafana/grafana-oss:latest

docker run --rm --name grafana --network grafana-prometheus --network-alias grafana --publish 3000:3000 --detach grafana/grafana-oss:latest
```
2. Access Grafana on http://localhost:3000