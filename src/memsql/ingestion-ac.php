<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';


use Amp\Mysql;
use \JsonMachine\JsonMachine;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    yield $connection->query("DROP DATABASE IF EXISTS page");
    yield $connection->query("CREATE DATABASE page");
    yield $connection->query("USE page");
    yield $connection->query("DROP TABLE IF EXISTS page");
    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates JSON, engagementScore FLOAT, entrances BIGINT, eventDate DATETIME SERIES TIMESTAMP, eventHour AS time_bucket('1h', eventDate) PERSISTED DATETIME, eventDay AS time_bucket('1d', eventDate) PERSISTED DATETIME, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate DATETIME, formSubmissions BIGINT, id BIGINT, indirectAccess BIGINT, indirectAccessDates JSON, individualId TEXT, interactionDates JSON, knownIndividual BOOLEAN, lastEventDate DATETIME, modifiedDate DATETIME, pageScrolls JSON, platformName TEXT, primaryKey TEXT, _reads BIGINT, region TEXT, searchTerm TEXT, segmentNames JSON, sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT, KEY (`canonicalUrl`, `title`, `dataSourceId`) USING CLUSTERED COLUMNSTORE, SHARD KEY (`canonicalUrl`))");

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
                yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, id, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrolls, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
                $count = 0;
                $values = "";
            }
        }

        if ($count > 0) {
            $values = substr($values, 0, -1);
            yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, id, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrolls, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
        }
    }

    yield $connection->query("OPTIMIZE TABLE page FULL");

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Ingestion time: $executionTime\r\n";
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

    return "'" . escapeSlashes(json_encode($array)) . "'";
}
