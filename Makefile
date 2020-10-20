CURRENT_DIR=$(shell pwd)


install-elasticsearch:
	docker network create bench_elasticsearch && \
	docker run -p 19200:9200 -v $(CURRENT_DIR)/elasticsearch:/usr/share/elasticsearch/data --name bench_elasticsearch -d -e "discovery.type=single-node" --net bench_elasticsearch elasticsearch:7.0.0 && \
	docker run -p 15601:5601 --name bench_kibana -d -e ELASTICSEARCH_HOSTS=http://bench_elasticsearch:9200 --net bench_elasticsearch kibana:7.0.0

install: install-influxdb install-elasticsearch
	composer install


start-elasticsearch:
	docker start bench_elasticsearch bench_kibana

start: start-influxdb start-elasticsearch


stop-elasticsearch:
	docker stop bench_elasticsearch bench_kibana > /dev/null 2>&1 || true

stop: stop-influxdb stop-elasticsearch


clean-elasticsearch: stop-elasticsearch
	docker rm bench_elasticsearch bench_kibana > /dev/null 2>&1 || true
	sleep 5
	docker network rm bench_elasticsearch > /dev/null 2>&1 || true
	rm -rf ./elasticsearch && mkdir elasticsearch && touch elasticsearch/.gitkeep

clean: clean-influxdb clean-elasticsearch

bench-ingestion-influxdb:
	php ./src/influxdb/ingestion.php

bench-ingestion-elasticsearch:
	php ./src/elasticsearch/ingestion.php

bench-response-time-influxdb:
	php ./src/influxdb/response-time.php

bench-response-time-elasticsearch:
	php ./src/elasticsearch/response-time.php

bench-disk-usage-influxdb:
	du -sh ./influxdb/data/rio

bench-disk-usage-elasticsearch:
	curl -s -XGET "http://dev.test:19200/_cat/indices?v" | grep statistic
