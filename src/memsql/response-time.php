<?php

require_once __DIR__ . '/../utils.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    $hours = generateHours(100);

    $start = microtime(true);
    yield $connection->query("USE statistic");
    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-7 day")->format(DateTime::ISO8601);
        $to = $hour->format(DateTime::ISO8601);

        yield $connection->query("SELECT time_bucket('1h') AS one_hour, statusCodeType, count(*) FROM statistic WHERE time > '$from' AND time <= '$to' GROUP BY statusCodeType, one_hour");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Response time: $executionTime\r\n";
});
