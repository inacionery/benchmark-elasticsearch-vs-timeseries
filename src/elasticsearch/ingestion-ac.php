<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Elastica\Client;
use Elastica\Document;

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
			'search' => [
				'slowlog' => [
					'threshold' => [
						'fetch' => [
							'debug' => '0s'
						],
						'query' => [
							'debug' => '0s'
						]
					]
				]
			],
			'indexing' => [
				'slowlog' => [
					'threshold' => [
						'index' => [
							'debug' => '0s'
						]
					]
				]
			],
			'number_of_shards' => '1',
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
$start = microtime(true);
foreach (generatePages(1, DURATION_LAST_MONTH, 1) as $page) {
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

	if (count($docs) > 10000) {
		$index->addDocuments($docs);
		$docs = [];
	}
}

if ($docs) {
	$index->addDocuments($docs);
}

$index->refresh();

$end = microtime(true);
$executionTime = getTime($end - $start);

echo "[Elasticsearch] Ingestion time: $executionTime\r\n";
