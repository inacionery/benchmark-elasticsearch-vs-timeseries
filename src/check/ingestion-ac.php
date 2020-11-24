<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

use Amp\Mysql;

use Elastica\Client;
use Elastica\Document;


Amp\Loop::run(function () {
	$elasticaClient = new Client([
		'host' => 'localhost',
		'port' => 19200,
	]);

	$index = $elasticaClient->getIndex('page');

	$index->create([
		'mappings' => [
			'properties' => [
				'asset' => [
					'type' => 'boolean'
				],
				'bounce' => [
					'type' => 'long'
				],
				'browserName' => [
					'type' => 'keyword'
				],
				'canonicalUrl' => [
					'type' => 'keyword',
					'fields' => [
						'search' => [
							'type' => 'keyword',
							'normalizer' => 'folding_normalizer'
						]
					]
				],
				'channelId' => [
					'type' => 'keyword'
				],
				'city' => [
					'type' => 'keyword'
				],
				'contentLanguageId' => [
					'type' => 'keyword'
				],
				'country' => [
					'type' => 'keyword'
				],
				'ctaClicks' => [
					'type' => 'long'
				],
				'dataSourceId' => [
					'type' => 'keyword'
				],
				'deviceType' => [
					'type' => 'keyword'
				],
				'directAccess' => [
					'type' => 'long'
				],
				'directAccessDates' => [
					'type' => 'date'
				],
				'engagementScore' => [
					'type' => 'double'
				],
				'entrances' => [
					'type' => 'long'
				],
				'eventDate' => [
					'type' => 'date'
				],
				'exits' => [
					'type' => 'long'
				],
				'experienceId' => [
					'type' => 'keyword'
				],
				'experimentId' => [
					'type' => 'keyword'
				],
				'firstEventDate' => [
					'type' => 'date'
				],
				'formSubmissions' => [
					'type' => 'long'
				],
				'id' => [
					'type' => 'keyword'
				],
				'indirectAccess' => [
					'type' => 'long'
				],
				'indirectAccessDates' => [
					'type' => 'date'
				],
				'individualId' => [
					'type' => 'keyword'
				],
				'interactionDates' => [
					'type' => 'date'
				],
				'knownIndividual' => [
					'type' => 'boolean'
				],
				'lastEventDate' => [
					'type' => 'date'
				],
				'modifiedDate' => [
					'type' => 'date'
				],
				'pageScrolls' => [
					'type' => 'nested',
					'properties' => [
						'depth' => [
							'type' => 'integer'
						],
						'eventDate' => [
							'type' => 'date'
						]
					]
				],
				'platformName' => [
					'type' => 'keyword'
				],
				'primaryKey' => [
					'type' => 'keyword'
				],
				'reads' => [
					'type' => 'long'
				],
				'region' => [
					'type' => 'keyword'
				],
				'searchTerm' => [
					'type' => 'keyword',
					'fields' => [
						'search' => [
							'type' => 'keyword',
							'normalizer' => 'folding_normalizer'
						]
					]
				],
				'segmentNames' => [
					'type' => 'keyword'
				],
				'sessionId' => [
					'type' => 'keyword'
				],
				'timeOnPage' => [
					'type' => 'long'
				],
				'title' => [
					'type' => 'keyword',
					'fields' => [
						'search' => [
							'type' => 'text',
							'analyzer' => 'folding_analyzer'
						]
					]
				],
				'url' => [
					'type' => 'keyword',
					'fields' => [
						'search' => [
							'type' => 'keyword',
							'normalizer' => 'folding_normalizer'
						]
					]
				],
				'userId' => [
					'type' => 'keyword'
				],
				'variantId' => [
					'type' => 'keyword'
				],
				'views' => [
					'type' => 'long'
				]
			]
		],
		'settings' => [
			'index' => [
				'number_of_shards' => '3',
				'analysis' => [
					'normalizer' => [
						'folding_normalizer' => [
							'filter' => ['asciifolding', 'lowercase', 'trim'],
							'type' => 'custom'
						]
					],
					'analyzer' => [
						'folding_analyzer' => [
							'filter' => ['asciifolding', 'lowercase'],
							'type' => 'custom',
							'tokenizer' => 'standard'
						]
					]
				],
				'max_terms_count' => '2147483647',
				'number_of_replicas' => '0',
			]
		]
	], true);

	$docs = [];

	$config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

	$connection = yield Postgres\connect($config);

	$configMEMSQL = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

	$connectionMEMSQL = yield Mysql\connect($configMEMSQL);

	yield $connection->query("DROP TABLE IF EXISTS page CASCADE");
	yield $connection->query("DROP TABLESPACE IF EXISTS page");
	yield $connection->query("CREATE TABLESPACE page LOCATION '/var/lib/postgresql/database'");
	yield $connection->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates TIMESTAMPTZ[], engagementScore DECIMAL, entrances BIGINT, eventDate TIMESTAMPTZ, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate TIMESTAMPTZ, formSubmissions BIGINT, indirectAccess BIGINT, indirectAccessDates TIMESTAMPTZ[], individualId TEXT, interactionDates TIMESTAMPTZ[], knownIndividual BOOLEAN, lastEventDate TIMESTAMPTZ, modifiedDate TIMESTAMPTZ, pageScrollsDepth INTEGER, pageScrollsEventDate TIMESTAMPTZ, platformName TEXT, primaryKey TEXT, reads BIGINT, region TEXT, searchTerm TEXT, segmentNames TEXT[], sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT) TABLESPACE page");
	yield $connection->query("SELECT create_hypertable('page', 'eventdate')");
	yield $connection->query("CREATE MATERIALIZED VIEW eventDate_one_hour AS SELECT time_bucket( '1 hour', eventDate) AS one_hour, canonicalUrl, title, dataSourceId, individualId, sessionId, SUM(views) AS views, SUM(exits) AS exits, SUM(entrances) AS entrances, SUM(bounce) AS bounces, SUM(timeOnPage) AS timeOnPages, SUM(engagementScore) AS engagementScores, COUNT(*) AS total FROM page GROUP BY one_hour, canonicalUrl, title, dataSourceId, individualId, sessionId");
	yield $connection->query("CREATE MATERIALIZED VIEW histogram_one_day AS SELECT time_bucket( '1 day', eventDate) AS one_day, COUNT(DISTINCT CASE WHEN knownindividual = true or ( individualId is not null and knownIndividual is null ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not null and knownIndividual is null ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page GROUP BY one_day ORDER BY one_day ASC");
	yield $connection->query("CREATE INDEX one_day_idx ON histogram_one_day(one_day)");

	yield $connectionMEMSQL->query("DROP DATABASE IF EXISTS page");
	yield $connectionMEMSQL->query("CREATE DATABASE page");
	yield $connectionMEMSQL->query("USE page");
	yield $connectionMEMSQL->query("DROP TABLE IF EXISTS page");
	yield $connectionMEMSQL->query("CREATE TABLE page (asset BOOLEAN, bounce BIGINT, browserName TEXT, canonicalUrl TEXT, channelId TEXT, city TEXT, contentLanguageId TEXT, country TEXT, ctaClicks BIGINT, dataSourceId TEXT, deviceType TEXT, directAccess BIGINT, directAccessDates JSON, engagementScore FLOAT, entrances BIGINT, eventDate DATETIME SERIES TIMESTAMP, eventHour AS time_bucket('1h', eventDate) PERSISTED DATETIME, eventDay AS time_bucket('1d', eventDate) PERSISTED DATETIME, exits BIGINT, experienceId TEXT, experimentId TEXT, firstEventDate DATETIME, formSubmissions BIGINT, indirectAccess BIGINT, indirectAccessDates JSON, individualId TEXT, interactionDates JSON, knownIndividual BOOLEAN, lastEventDate DATETIME, modifiedDate DATETIME, pageScrollsDepth INTEGER, pageScrollsEventDate DATETIME, platformName TEXT, primaryKey TEXT, _reads BIGINT, region TEXT, searchTerm TEXT, segmentNames JSON, sessionId TEXT, timeOnPage BIGINT, title TEXT, url TEXT, userId TEXT, variantId TEXT, views BIGINT, KEY (`canonicalUrl`, `title`, `dataSourceId`) USING CLUSTERED COLUMNSTORE, SHARD KEY (`canonicalUrl`))");

	$count = 0;
	$values = "";
	$valuesMEMSQL = "";
	$start = microtime(true);
	foreach (generatePages(2, 2 * DURATION_LAST_MONTH, 2) as $page) {

		$values .= "(" . $page['asset'] . "," . $page['bounce'] . ",'" . escapeSlashes($page['browserName']) . "','" . $page['canonicalUrl'] . "','" . $page['channelId'] . "','" . escapeSlashes($page['city']) . "','" . $page['contentLanguageId'] . "','" . escapeSlashes($page['country']) . "'," . $page['ctaClicks'] . ",'" . $page['dataSourceId'] . "','" . escapeSlashes($page['deviceType']) . "'," . $page['directAccess'] . ",'" . printArray(array_map(function ($date) {
			return formatDate($date);
		}, $page['directAccessDates'])) . "'," . $page['engagementScore'] . "," . $page['entrances'] . ",'" . formatDate($page['eventDate']) . "'," . $page['exits'] . ",'" . $page['experienceId'] . "','" . $page['experimentId'] . "','" . formatDate($page['firstEventDate']) . "'," . $page['formSubmissions'] . "," . $page['indirectAccess'] . ",'" . printArray(array_map(function ($date) {
			return formatDate($date);
		}, $page['indirectAccessDates'])) . "','" . $page['individualId'] . "','" . printArray(array_map(function ($date) {
			return formatDate($date);
		}, $page['interactionDates'])) . "'," . $page['knownIndividual'] . ",'" . formatDate($page['lastEventDate']) . "','" . formatDate($page['modifiedDate']) . "'," . (empty($page['pageScrolls']['depth']) ? 'NULL' : $page['pageScrolls']['depth']) . "," . (formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) ? "'" . formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) . "'" : 'NULL') . ",'" . escapeSlashes($page['platformName']) . "','" . $page['primaryKey'] . "'," . $page['reads'] . ",'" . escapeSlashes($page['region']) . "','" . $page['searchTerm'] . "','" . printArray($page['segmentNames']) . "','" . $page['sessionId'] . "'," . $page['timeOnPage'] . ",'" . escapeSlashes($page['title']) . "','" . escapeSlashes($page['url']) . "','" . $page['userId'] . "','" . $page['variantId'] . "'," . $page['views'] . "),";

		$valuesMEMSQL .= "(" . $page['asset'] . "," . $page['bounce'] . ",'" . escapeSlashes($page['browserName']) . "','" . $page['canonicalUrl'] . "','" . $page['channelId'] . "','" . escapeSlashes($page['city']) . "','" . $page['contentLanguageId'] . "','" . escapeSlashes($page['country']) . "'," . $page['ctaClicks'] . ",'" . $page['dataSourceId'] . "','" . escapeSlashes($page['deviceType']) . "'," . $page['directAccess'] . ",'" . printArrayMEMSQL(array_map(function ($date) {
			return formatDate($date);
		}, $page['directAccessDates'])) . "'," . $page['engagementScore'] . "," . $page['entrances'] . ",'" . formatDate($page['eventDate']) . "'," . $page['exits'] . ",'" . $page['experienceId'] . "','" . $page['experimentId'] . "','" . formatDate($page['firstEventDate']) . "'," . $page['formSubmissions'] . "," . $page['indirectAccess'] . ",'" . printArrayMEMSQL(array_map(function ($date) {
			return formatDate($date);
		}, $page['indirectAccessDates'])) . "','" . $page['individualId'] . "','" . printArrayMEMSQL(array_map(function ($date) {
			return formatDate($date);
		}, $page['interactionDates'])) . "'," . $page['knownIndividual'] . ",'" . formatDate($page['lastEventDate']) . "','" . formatDate($page['modifiedDate']) . "'," . (empty($page['pageScrolls']['depth']) ? 'NULL' : $page['pageScrolls']['depth']) . "," . (formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) ? "'" . formatDate((!empty($page['pageScrolls']) && isset($page['pageScrolls']) && is_array($page['pageScrolls'])) ? $page['pageScrolls']['eventDate'] : NULL) . "'" : 'NULL') . ",'" . escapeSlashes($page['platformName']) . "','" . $page['primaryKey'] . "'," . $page['reads'] . ",'" . escapeSlashes($page['region']) . "','" . $page['searchTerm'] . "','" . printArrayMEMSQL($page['segmentNames']) . "','" . $page['sessionId'] . "'," . $page['timeOnPage'] . ",'" . escapeSlashes($page['title']) . "','" . escapeSlashes($page['url']) . "','" . $page['userId'] . "','" . $page['variantId'] . "'," . $page['views'] . "),";

		$docs[] = new Document('', [
			'asset' => $page['asset'],
			'bounce' => $page['bounce'],
			'browserName' => $page['browserName'],
			'canonicalUrl' => $page['canonicalUrl'],
			'channelId' => $page['channelId'],
			'city' => $page['city'],
			'contentLanguageId' => $page['contentLanguageId'],
			'country' => $page['country'],
			'ctaClicks' => $page['ctaClicks'],
			'dataSourceId' => $page['dataSourceId'],
			'deviceType' => $page['deviceType'],
			'directAccess' => $page['directAccess'],
			'directAccessDates' => array_map(function ($date) {
				return $date->format(DateTime::RFC3339);
			}, $page['directAccessDates']),
			'engagementScore' => $page['engagementScore'],
			'entrances' => $page['entrances'],
			'eventDate' => $page['eventDate']->format(DateTime::RFC3339),
			'exits' => $page['exits'],
			'experienceId' => $page['experienceId'],
			'experimentId' => $page['experimentId'],
			'firstEventDate' => $page['firstEventDate']->format(DateTime::RFC3339),
			'formSubmissions' => $page['formSubmissions'],
			'indirectAccess' => $page['indirectAccess'],
			'indirectAccessDates' => array_map(function ($date) {
				return $date->format(DateTime::RFC3339);
			}, $page['indirectAccessDates']),
			'individualId' => $page['individualId'],
			'interactionDates' => array_map(function ($date) {
				return $date->format(DateTime::RFC3339);
			}, $page['interactionDates']),
			'knownIndividual' => $page['knownIndividual'],
			'lastEventDate' => $page['lastEventDate']->format(DateTime::RFC3339),
			'modifiedDate' => $page['modifiedDate']->format(DateTime::RFC3339),
			'pageScrolls' => array_map(function ($pageScroll) {
				if ($pageScroll instanceof DateTimeImmutable) {
					return $pageScroll->format(DateTime::RFC3339);
				}
				return $pageScroll;
			}, $page['pageScrolls']),
			'platformName' => $page['platformName'],
			'primaryKey' => $page['primaryKey'],
			'reads' => $page['reads'],
			'region' => $page['region'],
			'searchTerm' => $page['searchTerm'],
			'segmentNames' => $page['segmentNames'],
			'sessionId' => $page['sessionId'],
			'timeOnPage' => $page['timeOnPage'],
			'title' => $page['title'],
			'url' => $page['url'],
			'userId' => $page['userId'],
			'variantId' => $page['variantId'],
			'views' => $page['views'],
		]);

		$count++;

		if ($count > 10000) {
			$values = substr($values, 0, -1);
			yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");

			$valuesMEMSQL = substr($valuesMEMSQL, 0, -1);
			yield $connectionMEMSQL->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $valuesMEMSQL");

			$count = 0;
			$values = "";
			$valuesMEMSQL = "";
			$index->addDocuments($docs);
			$docs = [];
		}
	}

	if ($count > 0) {
		$values = substr($values, 0, -1);
		yield $connection->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $values");

		$valuesMEMSQL = substr($valuesMEMSQL, 0, -1);
		yield $connectionMEMSQL->query("INSERT INTO page(asset, bounce, browserName, canonicalUrl, channelId, city, contentLanguageId, country, ctaClicks, dataSourceId, deviceType, directAccess, directAccessDates, engagementScore, entrances, eventDate, exits, experienceId, experimentId, firstEventDate, formSubmissions, indirectAccess, indirectAccessDates, individualId, interactionDates, knownIndividual, lastEventDate, modifiedDate, pageScrollsDepth, pageScrollsEventDate, platformName, primaryKey, _reads, region, searchTerm, segmentNames, sessionId, timeOnPage, title, url, userId, variantId, views) VALUES $valuesMEMSQL");

		$index->addDocuments($docs);
	}

	yield $connection->query("REFRESH MATERIALIZED VIEW eventDate_one_hour");
	yield $connection->query("REFRESH MATERIALIZED VIEW histogram_one_day");

	yield $connectionMEMSQL->query("OPTIMIZE TABLE page FULL");

	$index->refresh();

	$end = microtime(true);
	$executionTime = $end - $start;

	echo "[Check] ingestion: $executionTime\r\n";
});

function escapeSlashes($string)
{
	return str_replace("'", "''", $string);
}

function printArray($array)
{
	return str_replace("]", "}", str_replace("[", "{", json_encode($array)));
}

function printArrayMEMSQL($array)
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
