CURRENT_DIR=$(shell pwd)
LICENSE_KEY = ${LICENSE_KEY}

bench-clickhouse:
	time make bench-ingestion-clickhouse
	time make bench-disk-usage-clickhouse
	time make bench-response-time-clickhouse
	time make bench-response-time-clickhouse
	time make bench-response-time-clickhouse
	make clean-clickhouse-store

bench-ac-chart-elasticsearch:
	time make bench-disk-usage-ac-elasticsearch
	time make bench-response-time-ac-chart-elasticsearch
	time make bench-response-time-ac-chart-elasticsearch
	time make bench-response-time-ac-chart-elasticsearch

bench-ac-elasticsearch:
	time make bench-disk-usage-ac-elasticsearch
	time make bench-response-time-ac-elasticsearch
	time make bench-response-time-ac-elasticsearch
	time make bench-response-time-ac-elasticsearch

bench-elasticsearch:
	time make bench-ingestion-elasticsearch
	sleep 10
	time make bench-disk-usage-elasticsearch
	time make bench-response-time-elasticsearch
	time make bench-response-time-elasticsearch
	time make bench-response-time-elasticsearch

bench-ac-memsql:
	time make bench-disk-usage-ac-memsql
	time make bench-response-time-ac-memsql
	time make bench-response-time-ac-memsql
	time make bench-response-time-ac-memsql

bench-ac-chart-memsql:
	time make bench-disk-usage-ac-memsql
	time make bench-response-time-ac-chart-memsql
	time make bench-response-time-ac-chart-memsql
	time make bench-response-time-ac-chart-memsql

bench-memsql:
	time make bench-ingestion-memsql
	sleep 10
	time make bench-disk-usage-memsql
	time make bench-response-time-memsql
	time make bench-response-time-memsql
	time make bench-response-time-memsql

bench-ac-postgres:
	time make bench-disk-usage-postgres
	time make bench-response-time-ac-postgres
	time make bench-response-time-ac-postgres
	time make bench-response-time-ac-postgres

bench-ac-chart-postgres:
	time make bench-disk-usage-postgres
	time make bench-response-time-ac-chart-postgres
	time make bench-response-time-ac-chart-postgres
	time make bench-response-time-ac-chart-postgres

bench-ac-timescaledb:
	time make bench-disk-usage-timescaledb
	time make bench-response-time-ac-timescaledb
	time make bench-response-time-ac-timescaledb
	time make bench-response-time-ac-timescaledb

bench-ac-chart-timescaledb:
	time make bench-disk-usage-timescaledb
	time make bench-response-time-ac-chart-timescaledb
	time make bench-response-time-ac-chart-timescaledb
	time make bench-response-time-ac-chart-timescaledb

bench-timescaledb:
	time make bench-ingestion-timescaledb
	sleep 10
	time make bench-disk-usage-timescaledb
	time make bench-response-time-timescaledb
	time make bench-response-time-timescaledb
	time make bench-response-time-timescaledb

bench-disk-usage-clickhouse:
	du -shm ./clickhouse

bench-disk-usage-ac-elasticsearch:
	curl -s -XGET "http://localhost:19200/_cat/indices?v" | grep page

bench-disk-usage-elasticsearch:
	curl -s -XGET "http://localhost:19200/_cat/indices?v" | grep statistic

bench-disk-usage-ac-memsql:
	php ./src/memsql/disk-ac.php

bench-disk-usage-memsql:
	php ./src/memsql/disk.php

bench-disk-usage-timescaledb:
	du -shm ./timescaledb

bench-disk-usage-postgres:
	du -shm ./postgres

bench-ingestion-clickhouse:
	php ./src/clickhouse/ingestion.php

bench-ingestion-ac-elasticsearch:
	php ./src/elasticsearch/ingestion-ac.php

bench-ingestion-elasticsearch:
	php ./src/elasticsearch/ingestion.php

bench-ingestion-ac-memsql:
	php ./src/memsql/ingestion-ac.php

bench-ingestion-memsql:
	php ./src/memsql/ingestion.php

bench-ingestion-ac-timescaledb:
	php ./src/timescaledb/ingestion-ac.php

bench-ingestion-timescaledb:
	php ./src/timescaledb/ingestion.php

bench-ingestion-ac-postgres:
	php ./src/postgres/ingestion-ac.php

bench-response-time-clickhouse:
	php ./src/clickhouse/response-time.php

bench-response-time-ac-elasticsearch:
	php ./src/elasticsearch/response-time-ac.php

bench-response-time-ac-chart-elasticsearch:
	php ./src/elasticsearch/response-time-ac-chart.php

bench-response-time-elasticsearch:
	php ./src/elasticsearch/response-time.php

bench-response-time-ac-memsql:
	php ./src/memsql/response-time-ac.php

bench-response-time-ac-chart-memsql:
	php ./src/memsql/response-time-ac-chart.php

bench-response-time-memsql:
	php ./src/memsql/response-time.php

bench-response-time-ac-timescaledb:
	php ./src/timescaledb/response-time-ac.php

bench-response-time-ac-chart-timescaledb:
	php ./src/timescaledb/response-time-ac-chart.php

bench-response-time-timescaledb:
	php ./src/timescaledb/response-time.php

bench-response-time-ac-postgres:
	php ./src/postgres/response-time-ac.php

bench-response-time-ac-chart-postgres:
	php ./src/postgres/response-time-ac-chart.php

clean-clickhouse-store:
	rm -rf ./clickhouse/store
	make restart-clickhouse

clean-clickhouse: stop-clickhouse
	docker rm bench_clickhouse > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_clickhouse > /dev/null 2>&1 || true
	rm -rf ./clickhouse && mkdir clickhouse && touch clickhouse/.gitkeep

clean-elasticsearch: stop-elasticsearch
	docker rm bench_elasticsearch > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_elasticsearch > /dev/null 2>&1 || true
	rm -rf ./elasticsearch && mkdir elasticsearch && touch elasticsearch/.gitkeep

clean-memsql: stop-memsql
	docker rm bench_memsql > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_memsql > /dev/null 2>&1 || true

clean-timescaledb: stop-timescaledb
	docker rm bench_timescaledb > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_timescaledb > /dev/null 2>&1 || true
	rm -rf ./timescaledb && mkdir timescaledb && touch timescaledb/.gitkeep

clean-postgres: stop-postgres
	docker rm bench_postgres > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_postgres > /dev/null 2>&1 || true
	rm -rf ./postgres && mkdir postgres && touch postgres/.gitkeep

clean: clean-clickhouse clean-elasticsearch clean-memsql clean-timescaledb clean-postgres

install-clickhouse:
	docker network create bench_clickhouse && \
	docker run -p 18123:8123 -p 19000:9000 -p 19009:9009 -d --name bench_clickhouse --ulimit nofile=262144:262144 -v $(CURRENT_DIR)/clickhouse:/var/lib/clickhouse yandex/clickhouse-server

install-elasticsearch:
	docker network create bench_elasticsearch && \
	docker run -p 19200:9200 -v $(CURRENT_DIR)/elasticsearch:/usr/share/elasticsearch/data --name bench_elasticsearch -d -e ES_JAVA_OPTS="-Xms8g -Xmx8g" -e "discovery.type=single-node" -e "network.bind_host=0.0.0.0" -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" --net bench_elasticsearch elasticsearch:7.9.2

install-memsql:
	docker network create bench_memsql && \
	docker run -p 13306:3306 -p 18080:8080 --name bench_memsql -d -e LICENSE_KEY=$(LICENSE_KEY) --net bench_memsql memsql/cluster-in-a-box:latest && \
	docker start bench_memsql

install-timescaledb:
	docker network create bench_timescaledb && \
	docker run -p 15432:5432 -v $(CURRENT_DIR)/timescaledb:/var/lib/postgresql/database --name bench_timescaledb -d -e POSTGRES_PASSWORD=password -e TS_TUNE_MEMORY=8GB -e TS_TUNE_NUM_CPUS=16 --net=bench_timescaledb timescale/timescaledb:2.0.0-rc3-pg12

install-postgres:
	docker network create bench_postgres && \
	docker run -p 5432:5432 -v $(CURRENT_DIR)/postgres/db-files:/var/lib/postgresql/data --name bench_postgres -d -e POSTGRES_PASSWORD=password --net=bench_postgres postgres:12.5

install: install-clickhouse install-elasticsearch install-memsql install-timescaledb install-postgres
	composer install

restart-clickhouse:
	docker restart bench_clickhouse

start-clickhouse:
	docker start bench_clickhouse

start-elasticsearch:
	docker start bench_elasticsearch

start-memsql:
	docker start bench_memsql

start-timescaledb:
	docker start bench_timescaledb

start-postgres:
	docker start bench_postgres

start: start-clickhouse start-elasticsearch start-memsql start-timescaledb start-postgres

stop-clickhouse:
	docker stop bench_clickhouse > /dev/null 2>&1 || true

stop-elasticsearch:
	docker stop bench_elasticsearch > /dev/null 2>&1 || true

stop-memsql:
	docker stop bench_memsql > /dev/null 2>&1 || true

stop-timescaledb:
	docker stop bench_timescaledb > /dev/null 2>&1 || true

stop-postgres:
	docker stop bench_postgres> /dev/null 2>&1 || true

stop: stop-clickhouse stop-elasticsearch stop-memsql stop-timescaledb stop-postgres