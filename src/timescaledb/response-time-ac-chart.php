<?php



require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    $days = generateDays(100);
    $start = microtime(true);

    foreach ($days as $i => $day) {
        $from = $day->modify("-60 day")->format(DateTime::RFC3339);
        $middle = $day->modify("-30 day")->format(DateTime::RFC3339);
        $to = $day->format(DateTime::RFC3339);

        yield $connection->query("SELECT * FROM histogram_one_day WHERE one_day >= '$from' AND one_day < '$middle'");
        yield $connection->query("SELECT * FROM histogram_one_day WHERE one_day >= '$middle' AND one_day < '$to'");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[TimescaleDB] Materialized Response time: $executionTime\r\n";

    $start = microtime(true);
    foreach ($days as $i => $day) {
        $from = $day->modify("-60 day")->format(DateTime::RFC3339);
        $middle = $day->modify("-30 day")->format(DateTime::RFC3339);
        $to = $day->format(DateTime::RFC3339);

        yield $connection->query("SELECT one_day, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not null and knownIndividual is null ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not null and knownIndividual is null ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM histogram_one_day_continuous WHERE one_day >= '$from' AND one_day < '$middle' GROUP BY one_day ORDER BY one_day ASC");

        yield $connection->query("SELECT one_day, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not null and knownIndividual is null ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not null and knownIndividual is null ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM histogram_one_day_continuous WHERE one_day >= '$middle' AND one_day < '$to' GROUP BY one_day ORDER BY one_day ASC");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[TimescaleDB] Continuous Response time: $executionTime\r\n";
});
