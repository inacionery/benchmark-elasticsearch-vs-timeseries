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

        yield $connection->query("WITH top_20_visitors AS (SELECT canonicalUrl, title, dataSourceId, (COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) + COUNT(DISTINCT individualId)) AS visitors, count(*) as total FROM page WHERE eventdate >= '$from' AND eventdate < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20) SELECT canonicalUrl, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, AVG(timeOnPage) AS avgTimeOnPage, AVG(engagementScore) AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounce) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, visitors, total, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM page JOIN top_20_visitors USING (canonicalUrl, title, dataSourceId) WHERE eventdate >= '$from' AND eventdate < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC;");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Response time: $executionTime\r\n";
});
