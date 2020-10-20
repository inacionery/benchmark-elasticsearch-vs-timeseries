<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    yield $connection->query("DROP TABLE IF EXISTS page CASCADE");
    yield $connection->query("DROP TABLESPACE IF EXISTS page");
    yield $connection->query("CREATE TABLESPACE page LOCATION '/var/lib/postgresql/database'");
    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates TIMESTAMPTZ[], engagementScore DECIMAL, entrances BIGINT, eventDate TIMESTAMPTZ, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate TIMESTAMPTZ, formSubmissions BIGINT, indirectAccess BIGINT, indirectAccessDates TIMESTAMPTZ[], individualId TEXT, interactionDates TIMESTAMPTZ[], knownIndividual BOOLEAN, lastEventDate TIMESTAMPTZ, modifiedDate TIMESTAMPTZ, pageScrollsDepth INTEGER, pageScrollsEventDate TIMESTAMPTZ, platformName TEXT, primaryKey TEXT, reads BIGINT, region TEXT, searchTerm TEXT, segmentNames TEXT[], sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT) TABLESPACE page");
    yield $connection->query("SELECT create_hypertable('page', 'eventdate')");
    yield $connection->query("CREATE MATERIALIZED VIEW one_hour AS SELECT time_bucket('1 hours', eventDate) AS one_hour, url, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field,	 COUNT(DISTINCT individualId) AS users_count, ARRAY_AGG(individualId) AS individualIds, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) AS missing_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, AVG(timeOnPage) AS avgTimeOnPage,	 AVG(engagementScore) AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounce) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, (COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) + COUNT(DISTINCT individualId)) AS visitors, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM page GROUP BY one_hour, url, title, dataSourceId");
    yield $connection->query("CREATE MATERIALIZED VIEW one_hour_url AS SELECT time_bucket('1 hours', eventDate) AS one_hour, url FROM page GROUP BY one_hour, url");

    $count = 0;
    $values = "";
    $start = microtime(true);
    foreach (generatePages(1, DURATION_LAST_MONTH, 1) as $page) {

        $values .= "(" . $page['asset'] . "," . $page['bounce'] . ",'" . escapeSlashes($page['browserName']) . "','" . $page['canonicalUrl'] . "','" . $page['channelId'] . "','" . escapeSlashes($page['city']) . "','" . $page['contentLanguageId'] . "','" . escapeSlashes($page['country']) . "'," . $page['ctaClicks'] . ",'" . $page['dataSourceId'] . "','" . escapeSlashes($page['deviceType']) . "'," . $page['directAccess'] . ",'" . printArray(array_map(function ($date) {
            return formatDate($date);
        }, $page['directAccessDates'])) . "'," . $page['engagementScore'] . "," . $page['entrances'] . ",'" . formatDate($page['eventDate']) . "'," . $page['exits'] . ",'" . $page['experienceId'] . "','" . $page['experimentId'] . "','" . formatDate($page['firstEventDate']) . "'," . $page['formSubmissions'] . "," . $page['indirectAccess'] . ",'" . printArray(array_map(function ($date) {
            return formatDate($date);
        }, $page['indirectAccessDates'])) . "','" . $page['individualId'] . "','" . printArray(array_map(function ($date) {
            return formatDate($date);
        }, $page['interactionDates'])) . "'," . $page['knownIndividual'] . ",'" . formatDate($page['lastEventDate']) . "','" . formatDate($page['modifiedDate']) . "'," . (empty($page['pageScrolls']['depth']) ? 'NULL' : $page['pageScrolls']['depth']) . "," . (formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) ? "'" . formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) . "'" : 'NULL') . ",'" . escapeSlashes($page['platformName']) . "','" . $page['primaryKey'] . "'," . $page['reads'] . ",'" . escapeSlashes($page['region']) . "','" . $page['searchTerm'] . "','" . printArray($page['segmentNames']) . "','" . $page['sessionId'] . "'," . $page['timeOnPage'] . ",'" . escapeSlashes($page['title']) . "','" . escapeSlashes($page['url']) . "','" . $page['userId'] . "','" . $page['variantId'] . "'," . $page['views'] . "),";

        $count++;

        if ($count > 10000) {
            $values = substr($values, 0, -1);
            yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
            $count = 0;
            $values = "";
        }
    }

    if ($count > 0) {
        $values = substr($values, 0, -1);
        yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
    }

    $end = microtime(true);
    $executionTime = getTime($end - $start);

    echo "[TimescaleDB] Ingestion time: $executionTime\r\n";

    $start = microtime(true);
    yield $connection->query("REFRESH MATERIALIZED VIEW one_hour");
    yield $connection->query("REFRESH MATERIALIZED VIEW one_hour_url");

    $end = microtime(true);
    $executionTime = getTime($end - $start);

    echo "[TimescaleDB] Refresh view: $executionTime\r\n";
});

function escapeSlashes($string)
{
    return str_replace("'", "''", $string);
}

function printArray($array)
{
    return str_replace("]", "}", str_replace("[", "{", json_encode($array)));
}

function formatDate(DateTimeImmutable $date = null)
{
    if ($date) {
        return $date->format(DateTime::RFC3339);
    }
    return NULL;
}
