<?php

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    $hours = generateHours(100);
    $start = microtime(true);

    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-30 day")->format(DateTime::ISO8601);
        $to = $hour->format(DateTime::ISO8601);

        yield $connection->query("WITH top AS ( SELECT canonicalUrl, title, dataSourceId, ( COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END ) + COUNT(DISTINCT individualId) ) AS visitors, SUM(total) AS total FROM eventDate_one_hour WHERE one_hour >= '$from' AND one_hour < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20 ) SELECT canonicalUrl, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END ) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounces) AS bounce_field, SUM(timeOnPages)/top.total AS avgTimeOnPage, SUM(engagementScores)/top.total AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounces) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, visitors, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM eventDate_one_hour JOIN top USING (canonicalUrl, title, dataSourceId) WHERE one_hour >= '$from' AND one_hour < '$to' AND sessionId IS NOT NULL GROUP BY top.total, visitors, canonicalUrl, title, dataSourceId ORDER BY visitors DESC, top.total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC;");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[TimescaleDB] Materialized Response time: $executionTime\r\n";
});
