<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;
use \JsonMachine\JsonMachine;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    yield $connection->query("DROP TABLE IF EXISTS page CASCADE");
    yield $connection->query("DROP TABLESPACE IF EXISTS page");

    yield $connection->query("CREATE TABLESPACE page LOCATION '/var/lib/postgresql/database'");

    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates TIMESTAMPTZ[], engagementScore DECIMAL, entrances BIGINT, eventDate TIMESTAMPTZ, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate TIMESTAMPTZ, formSubmissions BIGINT, id BIGINT, indirectAccess BIGINT, indirectAccessDates TIMESTAMPTZ[], individualId TEXT, interactionDates TIMESTAMPTZ[], knownIndividual BOOLEAN, lastEventDate TIMESTAMPTZ, modifiedDate TIMESTAMPTZ, pageScrolls TEXT[][], platformName TEXT, primaryKey TEXT, reads BIGINT, region TEXT, searchTerm TEXT, segmentNames TEXT[], sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT) TABLESPACE page");

    yield $connection->query("CREATE INDEX page_canonicalUrl_title_dataSourceId_idx ON page(canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX page_eventDate_canonicalUrl_title_dataSourceId_individualId_sessionId_idx ON page(eventDate, canonicalUrl, title, dataSourceId, individualId, sessionId)");
    yield $connection->query("CREATE INDEX page_eventDate_canonicalUrl_title_dataSourceId_sessionId_idx ON page(eventDate, canonicalUrl, title, dataSourceId, sessionId)");
    yield $connection->query("CREATE INDEX page_canonicalUrl_title_dataSourceId_sessionId_idx ON page(canonicalUrl, title, dataSourceId, sessionId)");
    yield $connection->query("CREATE INDEX page_eventDate_canonicalUrl_title_dataSourceId_idx ON page(eventDate, canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX page_eventDate_sessionId_idx ON page(eventDate, sessionId)");

    yield $connection->query("SELECT create_hypertable('page', 'eventdate')");

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

    echo "[TimescaleDB] Ingestion time: $IngestionTime\r\n";

    $start = microtime(true);

    yield $connection->query("CREATE MATERIALIZED VIEW eventDate_one_hour AS SELECT time_bucket( '1 hour', eventDate) AS one_hour, canonicalUrl, title, dataSourceId, individualId, sessionId, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounces, SUM(timeOnPage) AS timeOnPages, SUM(engagementScore) AS engagementScores, COUNT(*) AS total FROM page GROUP BY one_hour, canonicalUrl, title, dataSourceId, individualId, sessionId");

    yield $connection->query("CREATE INDEX canonicalUrl_title_dataSourceId_idx ON eventDate_one_hour(canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX one_hour_canonicalUrl_title_dataSourceId_individualId_sessionId_idx ON eventDate_one_hour(one_hour, canonicalUrl, title, dataSourceId, individualId, sessionId)");
    yield $connection->query("CREATE INDEX one_hour_canonicalUrl_title_dataSourceId_sessionId_idx ON eventDate_one_hour(one_hour, canonicalUrl, title, dataSourceId, sessionId)");
    yield $connection->query("CREATE INDEX canonicalUrl_title_dataSourceId_sessionId_idx ON eventDate_one_hour(canonicalUrl, title, dataSourceId, sessionId)");
    yield $connection->query("CREATE INDEX one_hour_canonicalUrl_title_dataSourceId_idx ON eventDate_one_hour(one_hour, canonicalUrl, title, dataSourceId)");
    yield $connection->query("CREATE INDEX one_hour_sessionId_idx ON eventDate_one_hour(one_hour, sessionId)");

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
    yield $connection->query("REFRESH MATERIALIZED VIEW eventDate_one_hour");
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
