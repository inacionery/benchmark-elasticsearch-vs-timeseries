CURRENT_DIR=$(shell pwd)

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

bench-disk-usage-timescaledb:
	du -shm ./timescaledb

bench-ingestion-ac-elasticsearch:
	php ./src/elasticsearch/ingestion-ac.php

bench-ingestion-elasticsearch:
	php ./src/elasticsearch/ingestion.php

bench-ingestion-ac-timescaledb:
	php ./src/timescaledb/ingestion-ac.php

bench-ingestion-timescaledb:
	php ./src/timescaledb/ingestion.php

bench-response-time-ac-elasticsearch:
	php ./src/elasticsearch/response-time-ac.php

bench-response-time-elasticsearch:
	php ./src/elasticsearch/response-time.php

bench-response-time-ac-timescaledb:
	php ./src/timescaledb/response-time-ac.php

bench-response-time-timescaledb:
	php ./src/timescaledb/response-time.php

clean-elasticsearch: stop-elasticsearch
	docker rm bench_elasticsearch > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_elasticsearch > /dev/null 2>&1 || true
	rm -rf ./elasticsearch && mkdir elasticsearch && touch elasticsearch/.gitkeep

clean-timescaledb: stop-timescaledb
	docker rm bench_timescaledb > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_timescaledb > /dev/null 2>&1 || true
	rm -rf ./timescaledb && mkdir timescaledb && touch timescaledb/.gitkeep

clean: clean-elasticsearch clean-timescaledb

install-elasticsearch:
	docker network create bench_elasticsearch && \
	docker run -p 19200:9200 -v $(CURRENT_DIR)/elasticsearch:/usr/share/elasticsearch/data --name bench_elasticsearch -d -e "discovery.type=single-node" -e "network.bind_host=0.0.0.0" -e "http.cors.enabled=true" -e "http.cors.allow-origin=*" --net bench_elasticsearch elasticsearch:7.9.2

install-timescaledb:
	docker network create bench_timescaledb && \
	docker run -p 15432:5432 -v $(CURRENT_DIR)/timescaledb:/var/lib/postgresql/database --name bench_timescaledb -d -e POSTGRES_PASSWORD=password --net=bench_timescaledb timescale/timescaledb:latest-pg12

install: install-elasticsearch install-timescaledb
	composer install

start-elasticsearch:
	docker start bench_elasticsearch

start-timescaledb:
	docker start bench_timescaledb

start: start-elasticsearch start-timescaledb

stop-elasticsearch:
	docker stop bench_elasticsearch > /dev/null 2>&1 || true

stop-timescaledb:
	docker stop bench_timescaledb > /dev/null 2>&1 || true

stop: stop-elasticsearch stop-timescaledb