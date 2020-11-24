<?php

require_once __DIR__ . '/../utils.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    $days = generateDays(100);
    $start = microtime(true);

    yield $connection->query("USE page");

    foreach ($days as $i => $day) {
        $from = $day->modify("-60 day")->format(DateTime::RFC3339);
        $middle = $day->modify("-30 day")->format(DateTime::RFC3339);
        $to = $day->format(DateTime::RFC3339);

        yield $connection->query("SELECT eventDay, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page WHERE eventDay >= '$from' AND eventDay < '$middle' GROUP BY eventDay ORDER BY eventDay ASC");

        yield $connection->query("SELECT eventDay, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page WHERE eventDay >= '$middle' AND eventDay < '$to' GROUP BY eventDay ORDER BY eventDay ASC");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Response time: $executionTime\r\n";
});
