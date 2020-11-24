<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

$users = isset($argv[1]) ? $argv[1] : 10;

Amp\Loop::run(function () {
    global $users;
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    yield $connection->query("DROP TABLE IF EXISTS page CASCADE");
    yield $connection->query("DROP TABLESPACE IF EXISTS page");
    yield $connection->query("CREATE TABLESPACE page LOCATION '/var/lib/postgresql/database'");
    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates TIMESTAMPTZ[], engagementScore DECIMAL, entrances BIGINT, eventDate TIMESTAMPTZ, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate TIMESTAMPTZ, formSubmissions BIGINT, indirectAccess BIGINT, indirectAccessDates TIMESTAMPTZ[], individualId TEXT, interactionDates TIMESTAMPTZ[], knownIndividual BOOLEAN, lastEventDate TIMESTAMPTZ, modifiedDate TIMESTAMPTZ, pageScrollsDepth INTEGER, pageScrollsEventDate TIMESTAMPTZ, platformName TEXT, primaryKey TEXT, reads BIGINT, region TEXT, searchTerm TEXT, segmentNames TEXT[], sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT) TABLESPACE page");
    yield $connection->query("SELECT create_hypertable('page', 'eventdate')");

    $count = 0;
    $values = "";
    $start = microtime(true);
    foreach (generatePages($users, 6 * DURATION_LAST_MONTH, 5) as $page) {

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
    $IngestionTime = $end - $start;

    echo "[TimescaleDB] Ingestion time: $IngestionTime\r\n";

    $start = microtime(true);

    yield $connection->query("CREATE MATERIALIZED VIEW histogram_one_day AS SELECT time_bucket( '1 day', eventDate) AS one_day, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not null and knownIndividual is null ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not null and knownIndividual is null ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page GROUP BY one_day ORDER BY one_day ASC");

    yield $connection->query("CREATE INDEX one_day_idx ON histogram_one_day(one_day)");

    $end = microtime(true);

    $materializedTime = $end - $start;

    echo "[TimescaleDB] Create Materialized: $materializedTime\r\n";

    $start = microtime(true);

    yield $connection->query("CREATE MATERIALIZED VIEW histogram_one_day_continuous WITH (timescaledb.continuous) AS SELECT time_bucket( '1 day', eventDate) AS one_day, knownIndividual, sessionId, individualId FROM page GROUP BY one_day, knownIndividual, sessionId, individualId");

    $end = microtime(true);

    $continuousTime = $end - $start;

    echo "[TimescaleDB] Create Continuous: $continuousTime\r\n";

    $start = microtime(true);
    yield $connection->query("REFRESH MATERIALIZED VIEW histogram_one_day");
    $end = microtime(true);

    $refreshMaterializedTime = $end - $start;

    echo "[TimescaleDB] Refresh Materialized: $refreshMaterializedTime\r\n";

    $start = microtime(true);
    yield $connection->query("CALL refresh_continuous_aggregate('histogram_one_day_continuous', NULL, NULL)");
    $end = microtime(true);

    $refreshContinuousTime = $end - $start;

    echo "[TimescaleDB] Refresh Continuous: $refreshContinuousTime\r\n";

    $materialized = $IngestionTime + $materializedTime + $refreshMaterializedTime;

    echo "[TimescaleDB] Materialized: $materialized\r\n";

    $continuous = $IngestionTime + $continuousTime + $refreshContinuousTime;

    echo "[TimescaleDB] Continuous: $continuous\r\n";
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
