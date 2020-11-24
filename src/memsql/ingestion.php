<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    yield $connection->query("DROP DATABASE IF EXISTS statistic");
    yield $connection->query("CREATE DATABASE statistic");
    yield $connection->query("USE statistic");
    yield $connection->query("DROP TABLE IF EXISTS statistic");
    yield $connection->query("CREATE TABLE statistic (time DATETIME SERIES TIMESTAMP, count integer, project varchar(255), statusCode SMALLINT, statusCodeType varchar(255), userAgent varchar(255), userAgentType SMALLINT, KEY (`project`) USING CLUSTERED COLUMNSTORE, SHARD KEY (`time`))");

    $count = 0;
    $values = "";
    $start = microtime(true);
    foreach (generateFixtures(1, DURATION_LAST_MONTH, 100, 50, MEMSQL) as $fixtures) {
        foreach ($fixtures as $fixture) {
            $values .= "('" . $fixture['date']->format(DateTime::ISO8601) . "'," . $fixture['value'] . ",'" . $fixture['tags']['project'] . "'," . $fixture['tags']['statusCode'] . ",'" . $fixture['tags']['statusCodeType'] . "','" . $fixture['tags']['userAgent'] . "'," . $fixture['tags']['userAgentType'] . "),";
            $count++;
        }

        if ($count > 10000) {
            $values = substr($values, 0, -1);
            yield $connection->query("INSERT INTO statistic(time, count, project, statusCode, statusCodeType, userAgent, userAgentType) VALUES $values");
            $count = 0;
            $values = "";
        }
    }

    if ($count > 0) {
        $values = substr($values, 0, -1);
        yield $connection->query("INSERT INTO statistic(time, count, project, statusCode, statusCodeType, userAgent, userAgentType) VALUES $values");
    }

    yield $connection->query("OPTIMIZE TABLE statistic FULL");

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Ingestion time: $executionTime\r\n";
});
