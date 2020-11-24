<?php

require_once __DIR__ . '/../utils.php';

use ClickHouseDB\Client;

$clickHouseClient = new Client([
    'host' => 'localhost',
    'port' => 18123,
    'username' => 'default',
    'password' => ''
]);

$clickHouseClient->database('statistic');

$hours = generateHours(100);

$start = microtime(true);

foreach ($hours as $i => $hour) {
    $Bindings = [
        'from' => $hour->modify("-7 day"),
        'to' => $hour
    ];

    $statement = $clickHouseClient->select("SELECT toYear(event_time) y, toMonth(event_time) m, toDayOfMonth(event_time) d, toHour(event_time) h, statusCodeType, count(*) FROM statistic WHERE event_time > :from AND event_time <= :to GROUP BY statusCodeType, y, m, d, h", $Bindings);

    $statement->rows();
}

$end = microtime(true);
$executionTime = $end - $start;

echo "[ClickHouseDB] Response time: $executionTime\r\n";
