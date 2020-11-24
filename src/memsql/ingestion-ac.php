<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';


use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    yield $connection->query("DROP DATABASE IF EXISTS page");
    yield $connection->query("CREATE DATABASE page");
    yield $connection->query("USE page");
    yield $connection->query("DROP TABLE IF EXISTS page");
    yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates JSON, engagementScore DECIMAL, entrances BIGINT, eventDate DATETIME SERIES TIMESTAMP, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate DATETIME, formSubmissions BIGINT, indirectAccess BIGINT, indirectAccessDates JSON, individualId TEXT, interactionDates JSON, knownIndividual BOOLEAN, lastEventDate DATETIME, modifiedDate DATETIME, pageScrollsDepth INTEGER, pageScrollsEventDate DATETIME, platformName TEXT, primaryKey TEXT, _reads BIGINT, region TEXT, searchTerm TEXT, segmentNames JSON, sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT, KEY (`url`, `title`, `dataSourceId`) USING CLUSTERED COLUMNSTORE, SHARD KEY (`eventDate`))");

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
            yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
            $count = 0;
            $values = "";
        }
    }

    if ($count > 0) {
        $values = substr($values, 0, -1);
        yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");
    }

    yield $connection->query("OPTIMIZE TABLE page FULL");

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[MemSQL] Ingestion time: $executionTime\r\n";
});

function escapeSlashes($string)
{
    return str_replace("'", "''", $string);
}

function printArray($array)
{
    return json_encode($array);
}

function formatDate(DateTimeImmutable $date = null)
{
    if ($date) {
        return $date->format(DateTime::RFC3339);
    }
    return NULL;
}
