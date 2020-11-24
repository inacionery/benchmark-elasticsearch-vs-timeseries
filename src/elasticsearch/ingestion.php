<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Elastica\Client;
use Elastica\Document;

$elasticaClient = new Client([
    'host' => 'localhost',
    'port' => 19200,
]);

$index = $elasticaClient->getIndex('statistic');

$index->create([
    'settings' => [
        'number_of_shards' => 5,
        'number_of_replicas' => 0,
        'refresh_interval' => -1,
        'codec' => 'best_compression',
    ],
    'mappings' => [
        '_source' => ['enabled' => false],
        'dynamic' => false,
        'properties' => [
            '@timestamp' => [
                'type' => 'date',
            ],
            'count' => [
                'type' => 'integer',
                'index' => false,
                'doc_values' => false,
            ],
            'project' => [
                'type' => 'keyword',
                'norms' => false,
            ],
            'statusCode' => [
                'type' => 'short',
            ],
            'statusCodeType' => [
                'type' => 'keyword',
                'norms' => false,
            ],
            'userAgent' => [
                'type' => 'keyword',
                'norms' => false,
            ],
            'userAgentType' => [
                'type' => 'short',
            ],
        ],
    ],
], true);

$docs = [];
$start = microtime(true);
foreach (generateFixtures(1, DURATION_LAST_MONTH, 100, 50, ELASTICSEARCH) as $fixtures) {
    foreach ($fixtures as $doc) {
        $docs[] = new Document('', [
            '@timestamp' => $doc['date']->format(DateTime::RFC3339),
            'count' => $doc['value'],
            'project' => $doc['tags']['project'],
            'statusCode' => $doc['tags']['statusCode'],
            'statusCodeType' => $doc['tags']['statusCodeType'],
            'userAgent' => $doc['tags']['userAgent'],
            'userAgentType' => $doc['tags']['userAgentType'],
        ]);
    }

    if (count($docs) > 80000) {
        $index->addDocuments($docs);
        $docs = [];
    }
}

if ($docs) {
    $index->addDocuments($docs);
}

$index->refresh();

$end = microtime(true);
$executionTime = $end - $start;

echo "[Elasticsearch] Ingestion time: $executionTime\r\n";
