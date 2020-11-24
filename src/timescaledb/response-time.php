<?php

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    $hours = generateHours(100);

    $start = microtime(true);
    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-7 day")->format(DateTime::ISO8601);
        $to = $hour->format(DateTime::ISO8601);

        yield $connection->query("SELECT * FROM one_hour WHERE one_hour > '$from' AND one_hour <= '$to'");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[TimescaleDB] Response time: $executionTime\r\n";
});
