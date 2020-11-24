<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use ClickHouseDB\Client;

$clickHouseClient = new Client([
    'host' => 'localhost',
    'port' => 18123,
    'username' => 'default',
    'password' => ''
]);

$clickHouseClient->write("DROP DATABASE IF EXISTS statistic");
$clickHouseClient->write("CREATE DATABASE IF NOT EXISTS statistic");

$clickHouseClient->database('statistic');

$clickHouseClient->write("DROP TABLE IF EXISTS statistic");
$clickHouseClient->write("CREATE TABLE statistic (event_time DateTime, count UInt16, project String, statusCode UInt8, statusCodeType String, userAgent String, userAgentType UInt8) ENGINE = SummingMergeTree() ORDER BY event_time");

$count = 0;
$values = [];
$start = microtime(true);
foreach (generateFixtures(1, DURATION_LAST_MONTH, 100, 50, CLICKHOUSEDB) as $fixtures) {
    foreach ($fixtures as $fixture) {
        $values[] = [$fixture['date'], $fixture['value'], $fixture['tags']['project'], $fixture['tags']['statusCode'], $fixture['tags']['statusCodeType'], $fixture['tags']['userAgent'], $fixture['tags']['userAgentType']];
        $count++;
    }

    if ($count > 300000) {
        $clickHouseClient->insert(
            'statistic',
            $values,
            ['event_time', 'count', 'project', 'statusCode', 'statusCodeType', 'userAgent', 'userAgentType']
        );
        $count = 0;
        $values = [];
    }
}

if ($count > 0) {
    $clickHouseClient->insert(
        'statistic',
        $values,
        ['event_time', 'count', 'project', 'statusCode', 'statusCodeType', 'userAgent', 'userAgentType']
    );
}

$clickHouseClient->select("OPTIMIZE TABLE statistic FINAL");

$end = microtime(true);
$executionTime = $end - $start;

echo "[ClickHouseDB] Ingestion time: $executionTime\r\n";
