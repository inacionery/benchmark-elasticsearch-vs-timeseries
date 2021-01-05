<?php

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=5432');

    $connection = yield Postgres\connect($config);

    $days = generateDays(100);
    $start = microtime(true);

    foreach ($days as $i => $day) {
        $from = $day->modify("-90 day")->format(DateTime::RFC3339);
        $middle = $day->modify("-60 day")->format(DateTime::RFC3339);
        $to = $day->format(DateTime::RFC3339);

        yield $connection->query("SELECT * FROM eventDate_histogram_view WHERE eventDate >= '$from' AND eventDate < '$middle'");
        yield $connection->query("SELECT * FROM eventDate_histogram_view WHERE eventDate >= '$middle' AND eventDate < '$to'");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[PostgreSQL] Materialized Response time: $executionTime\r\n";
});
