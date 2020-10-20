<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    yield $connection->query("DROP TABLE IF EXISTS statistic CASCADE");
    yield $connection->query("DROP TABLESPACE IF EXISTS statistic");
    yield $connection->query("CREATE TABLESPACE statistic LOCATION '/var/lib/postgresql/database'");
    yield $connection->query("CREATE TABLE statistic (time TIMESTAMPTZ NOT NULL, count integer, project varchar(255), statusCode SMALLINT, statusCodeType varchar(255), userAgent varchar(255), userAgentType SMALLINT) TABLESPACE statistic");
    yield $connection->query("SELECT create_hypertable('statistic', 'time')");
    yield $connection->query("CREATE MATERIALIZED VIEW one_hour AS SELECT time_bucket('1 hours', time) AS one_hour, statusCodeType, count(*) FROM statistic GROUP BY statusCodeType, one_hour");

    $count = 0;
    $values = "";
    $start = microtime(true);
    foreach (generateFixtures(1, DURATION_LAST_MONTH, 100, 50, TIMESCALEDB) as $fixtures) {
        foreach ($fixtures as $fixture) {
            $values .= "('" . $fixture['date']->format(DateTime::ISO8601) . "'," . $fixture['value'] . ",'" . $fixture['tags']['project'] . "'," . $fixture['tags']['statusCode'] . ",'" . $fixture['tags']['statusCodeType'] . "','" . $fixture['tags']['userAgent'] . "'," . $fixture['tags']['userAgentType'] . "),";
            $count++;
        }

        if ($count > 300000) {
            $values = substr($values, 0, -1);
            yield $connection->query("INSERT INTO statistic(\"time\", count, project, statusCode, statusCodeType, userAgent, userAgentType) VALUES $values");
            $count = 0;
            $values = "";
        }
    }

    if ($count > 0) {
        $values = substr($values, 0, -1);
        yield $connection->query("INSERT INTO statistic(\"time\", count, project, statusCode, statusCodeType, userAgent, userAgentType) VALUES $values");
    }

    $end = microtime(true);
    $executionTime = getTime($end - $start);

    echo "[TimescaleDB] Ingestion time: $executionTime\r\n";

    $start = microtime(true);
    yield $connection->query("REFRESH MATERIALIZED VIEW one_hour");

    $end = microtime(true);
    $executionTime = getTime($end - $start);

    echo "[TimescaleDB] Refresh view: $executionTime\r\n";
});
