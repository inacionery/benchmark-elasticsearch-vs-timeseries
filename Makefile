CURRENT_DIR=$(shell pwd)
LICENSE_KEY = ${LICENSE_KEY}

bench-ac-elasticsearch:
	time make bench-ingestion-ac-elasticsearch
	sleep 10
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
	time make bench-ingestion-ac-memsql
	sleep 10
	time make bench-disk-usage-ac-memsql
	time make bench-response-time-ac-memsql
	time make bench-response-time-ac-memsql
	time make bench-response-time-ac-memsql

bench-memsql:
	time make bench-ingestion-memsql
	sleep 10
	time make bench-disk-usage-memsql
	time make bench-response-time-memsql
	time make bench-response-time-memsql
	time make bench-response-time-memsql

bench-ac-timescaledb:
	time make bench-ingestion-ac-timescaledb
	sleep 10
	time make bench-disk-usage-timescaledb
	time make bench-response-time-ac-timescaledb
	time make bench-response-time-ac-timescaledb
	time make bench-response-time-ac-timescaledb

bench-timescaledb:
	time make bench-ingestion-timescaledb
	sleep 10
	time make bench-disk-usage-timescaledb
	time make bench-response-time-timescaledb
	time make bench-response-time-timescaledb
	time make bench-response-time-timescaledb

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

bench-response-time-ac-elasticsearch:
	php ./src/elasticsearch/response-time-ac.php

bench-response-time-elasticsearch:
	php ./src/elasticsearch/response-time.php

bench-response-time-ac-memsql:
	php ./src/memsql/response-time-ac.php

bench-response-time-memsql:
	php ./src/memsql/response-time.php

bench-response-time-ac-timescaledb:
	php ./src/timescaledb/response-time-ac.php

bench-response-time-timescaledb:
	php ./src/timescaledb/response-time.php

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

clean: clean-elasticsearch clean-memsql clean-timescaledb

install-elasticsearch:
	docker network create bench_elasticsearch && \
	docker run -p 19200:9200 -v $(CURRENT_DIR)/elasticsearch:/usr/share/elasticsearch/data --name bench_elasticsearch -d -e "discovery.type=single-node" -e "network.bind_host=0.0.0.0" -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" --net bench_elasticsearch elasticsearch:7.9.2

install-memsql:
	docker network create bench_memsql && \
	docker run -p 13306:3306 -p 18080:8080 --name bench_memsql -d -e LICENSE_KEY=$(LICENSE_KEY) --net bench_memsql memsql/cluster-in-a-box:centos-7.1.11-6c108deb15-2.0.2-1.8.0 && \
	docker start bench_memsql

install-timescaledb:
	docker network create bench_timescaledb && \
	docker run -p 15432:5432 -v $(CURRENT_DIR)/timescaledb:/var/lib/postgresql/database --name bench_timescaledb -d -e POSTGRES_PASSWORD=password --net=bench_timescaledb timescale/timescaledb:latest-pg12

install: install-elasticsearch install-memsql install-timescaledb
	composer install

start-elasticsearch:
	docker start bench_elasticsearch

start-memsql:
	docker start bench_elasticsearch

start-timescaledb:
	docker start bench_timescaledb

start: start-elasticsearch start-memsql start-timescaledb

stop-elasticsearch:
	docker stop bench_elasticsearch > /dev/null 2>&1 || true

stop-memsql:
	docker stop bench_memsql > /dev/null 2>&1 || true

stop-timescaledb:
	docker stop bench_timescaledb > /dev/null 2>&1 || true

stop: stop-elasticsearch stop-memsql stop-timescaledb