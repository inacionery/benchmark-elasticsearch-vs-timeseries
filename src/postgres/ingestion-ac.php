<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;
use \JsonMachine\JsonMachine;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=5432');

    $connection = yield Postgres\connect($config);

    yield $connection->query("DROP TABLE IF EXISTS page CASCADE");
    yield $connection->query("DROP TABLESPACE IF EXISTS page");

    yield $connection->query("CREATE TABLESPACE page LOCATION '/var/lib/postgresql/data'");

    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates TIMESTAMPTZ[], engagementScore DECIMAL, entrances BIGINT, eventDate TIMESTAMPTZ, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate TIMESTAMPTZ, formSubmissions BIGINT, id BIGINT, indirectAccess BIGINT, indirectAccessDates TIMESTAMPTZ[], individualId TEXT, interactionDates TIMESTAMPTZ[], knownIndividual BOOLEAN, lastEventDate TIMESTAMPTZ, modifiedDate TIMESTAMPTZ, pageScrolls TEXT[][], platformName TEXT, primaryKey TEXT, reads BIGINT, region TEXT, searchTerm TEXT, segmentNames TEXT[], sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT, missingSessions TEXT GENERATED ALWAYS AS (CASE WHEN individualId IS NULL THEN sessionId ELSE NULL END) STORED, PRIMARY KEY (id)) TABLESPACE page");

    yield $connection->query("CREATE INDEX page_canonicalUrl_title_dataSourceId_idx ON page(canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX page_eventDate_canonicalUrl_title_dataSourceId_sessionId_missing_sessions_idx ON page(eventDate, canonicalUrl, title, dataSourceId, sessionId, missing_sessions)");
    yield $connection->query("CREATE INDEX page_eventDate_idx ON page(eventDate)");

    $start = microtime(true);
    $database[] = JsonMachine::fromFile("src/ac-database/pages-18-02.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-19-08.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-19-09.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-19-10.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-19-11.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-19-12.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-01.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-02.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-03.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-04.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-05.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-06.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-07.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-08.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-09.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-10.json");
    $database[] = JsonMachine::fromFile("src/ac-database/pages-20-11.json");

    foreach ($database as $pages) {
        $count = 0;
        $values = "";
        foreach ($pages as $key => $page) {
            $values .= "(" . validateVar($page['_source'], 'asset') . "," . validateVar($page['_source']['bounce']) . "," . validateVar($page['_source']['browserName']) . "," . validateVar($page['_source']['canonicalUrl']) . "," . validateVar($page['_source']['channelId']) . "," . validateVar($page['_source']['city']) . "," . validateVar($page['_source']['contentLanguageId']) . "," . validateVar($page['_source']['country']) . "," . validateVar($page['_source'], 'ctaClicks') . "," . validateVar($page['_source']['dataSourceId']) . "," . validateVar($page['_source']['deviceType']) . "," . validateVar($page['_source']['directAccess']) . "," . printArray($page['_source']['directAccessDates']) . "," . validateVar($page['_source']['engagementScore']) . "," . validateVar($page['_source'], 'entrances') . ",'" . $page['_source']['eventDate'] . "'," . validateVar($page['_source'], 'exits') . "," . validateVar($page['_source']['experienceId']) . "," . validateVar($page['_source']['experimentId']) . ",'" . $page['_source']['firstEventDate'] . "'," . validateVar($page['_source']['formSubmissions']) . "," . $page['_source']['id'] . "," . validateVar($page['_source']['indirectAccess']) . "," . printArray($page['_source']['indirectAccessDates']) . "," . validateVar($page['_source']['individualId']) . "," . printArray($page['_source']['interactionDates']) . "," . validateVar($page['_source'], 'knownIndividual') . ",'" . $page['_source']['lastEventDate'] . "','" . $page['_source']['modifiedDate'] . "'," . printArray(removeObjects($page['_source']['pageScrolls'])) . "," . validateVar($page['_source']['platformName']) . "," . validateVar($page['_source']['primaryKey']) . "," . validateVar($page['_source'], 'reads') . "," . validateVar($page['_source']['region']) . "," . validateVar($page['_source'], 'searchTerm') . "," . printArray($page['_source']['segmentNames']) . "," . validateVar($page['_source'], 'sessionId') . "," . validateVar($page['_source']['timeOnPage']) . "," . validateVar($page['_source']['title']) . "," . validateVar($page['_source']['url']) . "," . validateVar($page['_source']['userId']) . "," . validateVar($page['_source']['variantId']) . "," . validateVar($page['_source']['views']) . "),";

            $count++;
            if ($count > 10000) {
                $values = substr($values, 0, -1);
                yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, id, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrolls, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
                $count = 0;
                $values = "";
            }
        }

        if ($count > 0) {
            $values = substr($values, 0, -1);
            yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, id, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrolls, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
        }
    }

    $end = microtime(true);
    $IngestionTime = $end - $start;

    echo "[PostgreSQL] Ingestion time: $IngestionTime\r\n";

    $start = microtime(true);

    yield $connection->query("CREATE MATERIALIZED VIEW hour_eventDate_view AS SELECT date_trunc('hour', eventDate) as time, canonicalUrl, title, dataSourceId, ARRAY_AGG(DISTINCT individualId) AS individualIds, ARRAY_AGG(DISTINCT sessionId) AS sessionIds, ARRAY_AGG(DISTINCT missing_sessions) AS missingSessions, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounce, SUM(timeOnPage) AS timeOnPage, SUM(engagementScore) AS engagementScore, COUNT(*) AS total FROM page WHERE sessionId IS NOT NULL GROUP BY time, canonicalUrl, title, dataSourceId");

    yield $connection->query("CREATE MATERIALIZED VIEW day_eventDate_view AS SELECT date_trunc('day', eventDate) as time, canonicalUrl, title, dataSourceId, ARRAY_AGG(DISTINCT individualId) AS individualIds, ARRAY_AGG(DISTINCT sessionId) AS sessionIds, ARRAY_AGG(DISTINCT missing_sessions) AS missingSessions, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounce, SUM(timeOnPage) AS timeOnPage, SUM(engagementScore) AS engagementScore, COUNT(*) AS total FROM page WHERE sessionId IS NOT NULL GROUP BY time, canonicalUrl, title, dataSourceId");

    yield $connection->query("CREATE MATERIALIZED VIEW week_eventDate_view AS SELECT date_trunc('week', eventDate) as time, canonicalUrl, title, dataSourceId, ARRAY_AGG(DISTINCT individualId) AS individualIds, ARRAY_AGG(DISTINCT sessionId) AS sessionIds, ARRAY_AGG(DISTINCT missing_sessions) AS missingSessions, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounce, SUM(timeOnPage) AS timeOnPage, SUM(engagementScore) AS engagementScore, COUNT(*) AS total FROM page WHERE sessionId IS NOT NULL GROUP BY time, canonicalUrl, title, dataSourceId");

    yield $connection->query("CREATE MATERIALIZED VIEW month_eventDate_view AS SELECT date_trunc('month', eventDate) as time, canonicalUrl, title, dataSourceId, ARRAY_AGG(DISTINCT individualId) AS individualIds, ARRAY_AGG(DISTINCT sessionId) AS sessionIds, ARRAY_AGG(DISTINCT missing_sessions) AS missingSessions, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounce, SUM(timeOnPage) AS timeOnPage, SUM(engagementScore) AS engagementScore, COUNT(*) AS total FROM page WHERE sessionId IS NOT NULL GROUP BY time, canonicalUrl, title, dataSourceId");

    yield $connection->query("CREATE INDEX hour_time_canonicalUrl_title_dataSourceId_idx ON hour_eventDate_view(time, canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX day_time_canonicalUrl_title_dataSourceId_idx ON day_eventDate_view(time, canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX week_time_canonicalUrl_title_dataSourceId_idx ON week_eventDate_view(time, canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX month_time_canonicalUrl_title_dataSourceId_idx ON month_eventDate_view(time, canonicalUrl, title, dataSourceId)");

    yield $connection->query("CREATE MATERIALIZED VIEW eventDate_histogram_view AS SELECT eventDate, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not null and knownIndividual is null ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not null and knownIndividual is null ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page GROUP BY eventDate ORDER BY eventDate ASC");

    yield $connection->query("CREATE INDEX eventDate_idx ON eventDate_histogram_view(eventDate)");

    yield $connection->query("CREATE or replace FUNCTION ARRAY_UNION_STEP (s ANYARRAY, n ANYARRAY) RETURNS ANYARRAY AS $$ SELECT s || n; $$ LANGUAGE SQL IMMUTABLE LEAKPROOF PARALLEL SAFE;");
    yield $connection->query("CREATE OR REPLACE FUNCTION COUNT_ARRAY_DISTINCT (s ANYARRAY) RETURNS bigint AS $$ SELECT COUNT(i) FROM (SELECT DISTINCT UNNEST(x) AS i FROM (VALUES(s)) AS v(x)) AS w WHERE i IS NOT NULL; $$ LANGUAGE SQL IMMUTABLE LEAKPROOF PARALLEL SAFE;");
    yield $connection->query("CREATE OR REPLACE AGGREGATE COUNT_ARRAY_AGGREGATE (ANYARRAY) ( SFUNC = ARRAY_UNION_STEP, STYPE = ANYARRAY, FINALFUNC = COUNT_ARRAY_DISTINCT, INITCOND = '{}', PARALLEL = SAFE);");

    $end = microtime(true);

    $materializedTime = $end - $start;

    echo "[PostgreSQL] Create Materialized: $materializedTime\r\n";

    $start = microtime(true);
    yield $connection->query("REFRESH MATERIALIZED VIEW hour_eventDate_view");
    yield $connection->query("REFRESH MATERIALIZED VIEW day_eventDate_view");
    yield $connection->query("REFRESH MATERIALIZED VIEW week_eventDate_view");
    yield $connection->query("REFRESH MATERIALIZED VIEW month_eventDate_view");
    yield $connection->query("REFRESH MATERIALIZED VIEW eventDate_histogram_view");
    $end = microtime(true);

    $refreshMaterializedTime = $end - $start;

    echo "[PostgreSQL] Refresh Materialized: $refreshMaterializedTime\r\n";

    $materialized = $IngestionTime + $materializedTime + $refreshMaterializedTime;

    echo "[PostgreSQL] Materialized: $materialized\r\n";
});

function removeObjects($array)
{
    if (is_array($array)) {
        $aux = [];
        foreach ($array as $key => $value) {
            $aux[] = removeObjects($value);
        }
        return $aux;
    }
    return $array;
}

function validateVar($var, $subVar = null)
{
    if (is_array($var) && isset($subVar)) {
        if (array_key_exists($subVar, $var)) {
            return validateVar($var[$subVar]);
        }
        return "NULL";
    }
    if (is_bool($var)) {
        return $var ? 'true' : 'false';
    }
    if (is_numeric($var)) {
        return $var;
    }
    if (empty($var)) {
        return "NULL";
    }
    return "'" . escapeSlashes($var) . "'";
}

function escapeSlashes($string)
{
    return str_replace("'", "''", $string);
}

function printArray($array)
{
    if (empty($array)) {
        return "NULL";
    }

    return "'" . str_replace("]", "}", str_replace("[", "{", escapeSlashes(json_encode($array)))) . "'";
}
