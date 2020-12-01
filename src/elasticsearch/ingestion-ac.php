<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Elastica\Client;
use Elastica\Document;
use \JsonMachine\JsonMachine;

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

$start = microtime(true);
populate($index, JsonMachine::fromFile("src/ac-database/pages-18-02.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-19-08.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-19-09.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-19-10.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-19-11.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-19-12.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-01.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-02.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-03.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-04.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-05.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-06.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-07.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-08.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-09.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-10.json"));
populate($index, JsonMachine::fromFile("src/ac-database/pages-20-11.json"));
$index->refresh();

$end = microtime(true);
$executionTime = $end - $start;

echo "[Elasticsearch] Ingestion time: $executionTime\r\n";

function populate($index, $pages)
{
	$docs = [];
	foreach ($pages as $key => $page) {
		$doc = [];
		foreach ($page['_source'] as $id => $source) {
			$doc[$id] = $source;
		}

		$docs[] = new Document($page['_id'], $doc);

		if (count($docs) > 10000) {
			$index->addDocuments($docs);
			$docs = [];
		}
	}

	if ($docs) {
		$index->addDocuments($docs);
	}
}
