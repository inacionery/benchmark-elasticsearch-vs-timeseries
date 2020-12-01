<?php

require_once __DIR__ . '/../utils.php';

use ClickHouseDB\Client;

$clickHouseClient = new Client([
    'host' => 'localhost',
    'port' => 18123,
    'username' => 'default',
    'password' => ''
]);

$clickHouseClient->database('page');

$hours = generateHours(1);
$start = microtime(true);

foreach ($hours as $i => $hour) {
    $Bindings = [
        'from' => $hour->modify("-30 day"),
        'to' => $hour
    ];

    $statement = $clickHouseClient->select("SELECT canonicalUrl, title, dataSourceId, count(*) as total, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT if(individualId IS NULL, sessionId, NULL)) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, AVG(timeOnPage) AS avgTimeOnPage, AVG(engagementScore) AS avgEngagementScore, if(sessions_count > 0 , bounce_field / sessions_count, 0) AS bounces, if(sessions_count > 0 , exits_field / sessions_count, 0) AS exit, missing_sessions_count + users_count  AS visitors FROM page WHERE eventDate >= :from AND eventDate < :to AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20", $Bindings);

    $statement->rows();
}

$statement = $clickHouseClient->select("SELECT * FROM page where eventDate = '2020-11-30T17:00:00'");

// totals row
print_r($statement->totals());


$end = microtime(true);
$executionTime = $end - $start;

echo "[ClickHouseDB] Response time: $executionTime\r\n";
