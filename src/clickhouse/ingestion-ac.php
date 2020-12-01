<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use ClickHouseDB\Client;
use \JsonMachine\JsonMachine;

$clickHouseClient = new Client([
    'host' => 'localhost',
    'port' => 18123,
    'username' => 'default',
    'password' => ''
]);

$clickHouseClient->write("DROP DATABASE IF EXISTS page");
$clickHouseClient->write("CREATE DATABASE page");

$clickHouseClient->database('page');

$clickHouseClient->write("DROP TABLE IF EXISTS page");
$clickHouseClient->write("CREATE TABLE page (asset Boolean, bounce UInt16, browserName String, canonicalUrl String, channelId String, city String, contentLanguageId String, country String, ctaClicks UInt16, dataSourceId String, deviceType String, directAccess UInt16, directAccessDates Array(DateTime), engagementScore Float32, entrances UInt16, eventDate DateTime, exits UInt16, experienceId String, experimentId String, firstEventDate DateTime, formSubmissions UInt16, id UInt16, indirectAccess UInt16, indirectAccessDates Array(DateTime), individualId String, interactionDates Array(DateTime), knownIndividual BOOLEAN, lastEventDate DateTime, modifiedDate DateTime, pageScrolls Array(Array(String)), platformName String, primaryKey String, reads UInt16, region String, searchTerm String, segmentNames Array(String), sessionId String, timeOnPage UInt16, title String, url String, userId String, variantId String, views UInt16) ENGINE = SummingMergeTree() ORDER BY eventDate");

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
    $values = [];
    foreach ($pages as $key => $page) {
        $values[] = [
            validateVar($page['_source'], 'asset'),
            validateVar($page['_source'], 'bounce', 0),
            validateVar($page['_source'], 'browserName'),
            validateVar($page['_source'], 'canonicalUrl'),
            validateVar($page['_source'], 'channelId'),
            validateVar($page['_source'], 'city'),
            validateVar($page['_source'], 'contentLanguageId'),
            validateVar($page['_source'], 'country'),
            validateVar($page['_source'], 'ctaClicks', 0),
            validateVar($page['_source'], 'dataSourceId'),
            validateVar($page['_source'], 'deviceType'),
            validateVar($page['_source'], 'directAccess', 0),
            array_map(function ($date) {
                return formatDate($date);
            }, $page['_source']['directAccessDates']),
            validateVar($page['_source'], 'engagementScore', 0),
            validateVar($page['_source'], 'entrances', 0),
            formatDate($page['_source']['eventDate']),
            validateVar($page['_source'], 'exits', 0),
            validateVar($page['_source'], 'experienceId'),
            validateVar($page['_source'], 'experimentId'),
            formatDate($page['_source']['firstEventDate']),
            validateVar($page['_source'], 'formSubmissions'),
            $page['_source']['id'],
            validateVar($page['_source'], 'indirectAccess', 0),
            array_map(function ($date) {
                return formatDate($date);
            }, $page['_source']['indirectAccessDates']),
            validateVar($page['_source']['individualId']),
            array_map(function ($date) {
                return formatDate($date);
            }, $page['_source']['interactionDates']),
            validateVar($page['_source'], 'knownIndividual', false),
            formatDate($page['_source']['lastEventDate']),
            formatDate($page['_source']['modifiedDate']),
            removeObjects($page['_source']['pageScrolls']),
            validateVar($page['_source'], 'platformName'),
            validateVar($page['_source'], 'primaryKey'),
            validateVar($page['_source'], 'reads', 0),
            validateVar($page['_source'], 'region'),
            validateVar($page['_source'], 'searchTerm'),
            $page['_source']['segmentNames'],
            validateVar($page['_source'], 'sessionId'),
            validateVar($page['_source'], 'timeOnPage'),
            validateVar($page['_source'], 'title'),
            validateVar($page['_source'], 'url'),
            validateVar($page['_source'], 'userId'),
            validateVar($page['_source'], 'variantId'),
            validateVar($page['_source'], 'views', 0)
        ];

        $count++;
        if ($count > 10000) {
            $clickHouseClient->insert(
                'page',
                $values,
                ['asset', 'bounce', 'browserName', 'canonicalUrl', 'channelId', 'city', 'contentLanguageId', 'country', 'ctaClicks', 'dataSourceId', 'deviceType', 'directAccess', 'directAccessDates', 'engagementScore', 'entrances', 'eventDate', 'exits', 'experienceId', 'experimentId', 'firstEventDate', 'formSubmissions', 'id', 'indirectAccess', 'indirectAccessDates', 'individualId', 'interactionDates', 'knownIndividual', 'lastEventDate', 'modifiedDate', 'pageScrolls', 'platformName', 'primaryKey', 'reads', 'region', 'searchTerm', 'segmentNames', 'sessionId', 'timeOnPage', 'title', 'url', 'userId', 'variantId', 'views']
            );
            $count = 0;
            $values = [];
        }
    }

    if ($count > 0) {
        $clickHouseClient->insert(
            'page',
            $values,
            ['asset', 'bounce', 'browserName', 'canonicalUrl', 'channelId', 'city', 'contentLanguageId', 'country', 'ctaClicks', 'dataSourceId', 'deviceType', 'directAccess', 'directAccessDates', 'engagementScore', 'entrances', 'eventDate', 'exits', 'experienceId', 'experimentId', 'firstEventDate', 'formSubmissions', 'id', 'indirectAccess', 'indirectAccessDates', 'individualId', 'interactionDates', 'knownIndividual', 'lastEventDate', 'modifiedDate', 'pageScrolls', 'platformName', 'primaryKey', 'reads', 'region', 'searchTerm', 'segmentNames', 'sessionId', 'timeOnPage', 'title', 'url', 'userId', 'variantId', 'views']
        );
    }
}

$clickHouseClient->select("OPTIMIZE TABLE page FINAL");

$end = microtime(true);
$executionTime = $end - $start;

echo "[ClickHouseDB] Ingestion time: $executionTime\r\n";

function removeObjects($array)
{
    if (is_array($array)) {
        $aux = [];
        foreach ($array as $key => $value) {
            $aux[] = removeObjects($value);
        }
        return $aux;
    }

    return "'" . $array . "'";
}
function formatDate($date)
{
    return substr($date, 0, -5);
}
function validateVar($var, $subVar = null, $default = "NULL")
{
    if (is_array($var) && isset($subVar)) {
        if (array_key_exists($subVar, $var)) {
            return validateVar($var[$subVar], null, $default);
        }
        return $default;
    }
    if (is_bool($var)) {
        return $var ? true : false;
    }
    if (is_numeric($var)) {
        return $var;
    }
    if (empty($var)) {
        return $default;
    }
    return escapeSlashes($var);
}

function escapeSlashes($string)
{
    return str_replace("\\", "\\\\",  str_replace("'", "''", $string));
}
