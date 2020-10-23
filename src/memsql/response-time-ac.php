<?php

require_once __DIR__ . '/../utils.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    $hours = generateHours(100);
    $start = microtime(true);

    yield $connection->query("USE page");

    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-30 day")->format(DateTime::ISO8601);
        $to = $hour->format(DateTime::ISO8601);

        yield $connection->query("SELECT time_bucket('1h') AS one_hour, url, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, GROUP_CONCAT(individualId) AS individualIds, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) AS missing_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, AVG(timeOnPage) AS avgTimeOnPage,	 AVG(engagementScore) AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounce) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, (COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) + COUNT(DISTINCT individualId)) AS visitors, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM page WHERE eventdate > '$from' AND eventdate <= '$to' GROUP BY one_hour, url, title, dataSourceId ORDER BY visitors DESC LIMIT 20");
        yield $connection->query("SELECT time_bucket('1h') AS one_hour, url FROM page WHERE eventdate > '$from' AND eventdate <= '$to' GROUP BY one_hour, url ");
    }

    $end = microtime(true);
    $executionTime = getTime($end - $start);

    echo "[MemSQL] Response time: $executionTime\r\n";
});
