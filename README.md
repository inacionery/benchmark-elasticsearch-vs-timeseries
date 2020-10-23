# Elasticsearch vs MemSQL vs TimescaleDB for time series and metrics data

## Environment 👨🏻‍💻

- Mac OS X - 16 GB RAM
- Docker
- PHP 7.3
- PHP Composer
- Elasticsearch 7.9.2
- MemSQL 7.1.11 + MemSQL Studio 2.0.2
- TimescaleDB PostgreSQL 12

Ensure `localhost` points the IP of your Docker daemon by editing your `/etc/hosts` file.

## Run it! 🏃 (recommended)

It's recommended to use available commands in the Makefile to get started quickly. :blush:

```bash
make install # install docker stack
make start # start docker stack (not needed after an install)
make stop # stop docker stack
make clean # delete all docker stack (including generated data)

make bench-ac-elasticsearch # benchmark AC elasticsearch performance
make bench-ac-memsql # benchmark AC memsql performance
make bench-ac-timescaledb # benchmark AC timescaledb performance

make bench-ingestion-elasticsearch # benchmark elasticsearch ingestion performance
make bench-ingestion-memsql # benchmark memsql ingestion performance
make bench-ingestion-timescaledb # benchmark timescaledb ingestion performance
make bench-response-time-elasticsearch # benchmark elasticsearch query response time
make bench-response-time-memsql # benchmark memsql query response time
make bench-response-time-timescaledb # benchmark timescaledb query response time
make bench-disk-usage-elasticsearch # benchmark elasticsearch disk usage (run ingestion command first)
make bench-disk-usage-memsql # benchmark memsql disk usage (run ingestion command first)
make bench-disk-usage-timescaledb # benchmark timescaledb disk usage (run ingestion command first)
```

## Or, if you prefer doing it manually...

### Dockerization 🐳

#### Elasticsearch

```bash
# Create network, volume and containers.
docker network create bench_elasticsearch && \
docker run -p 19200:9200 -v $PWD/elasticsearch:/usr/share/elasticsearch/data --name bench_elasticsearch -d -e "discovery.type=single-node" -e "network.bind_host=0.0.0.0" -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" --net bench_elasticsearch elasticsearch:7.9.2
```

#### MemSQL

```bash
# Create network, volume and containers.
docker network create bench_memsql && \
docker run -p 13306:3306 -p 18080:8080 --name bench_memsql -d -e LICENSE_KEY=$(LICENSE_KEY) --net bench_memsql memsql/cluster-in-a-box:centos-7.1.11-6c108deb15-2.0.2-1.8.0 && \
docker start bench_memsql
```

#### TimescaleDB

```bash
# Create network, volume and containers.
docker network create bench_timescaledb && \
docker run -p 15432:5432 -v $PWD/timescaledb:/var/lib/postgresql/statistic --name bench_timescaledb -d -e POSTGRES_PASSWORD=password --net=bench_timescaledb timescale/timescaledb:latest-pg12
```

### Benchmark it! ✨

```bash
composer install

# Elasticsearch
php src/elasticsearch/ingestion.php
php src/elasticsearch/response-time.php

# Elasticsearch AC
php src/elasticsearch/ingestion-ac.php
php src/elasticsearch/response-time-ac.php

# MemSQL
php src/memsql/ingestion.php
php src/memsql/response-time.php

# MemSQL AC
php src/memsql/ingestion-ac.php
php src/memsql/response-time-ac.php

# TimescaleDB
php src/timescaledb/ingestion.php
php src/timescaledb/response-time.php

# TimescaleDB AC
php src/timescaledb/ingestion-ac.php
php src/timescaledb/response-time-ac.php
```
