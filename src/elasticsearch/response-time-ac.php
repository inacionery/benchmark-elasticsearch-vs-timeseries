<?php

require_once __DIR__ . '/../utils.php';

use Elastica\Client;
use Elastica\Query;
use Elastica\Search;

$elasticaClient = new Client([
    'host' => 'localhost',
    'port' => 19200,
]);

$search = new Search($elasticaClient);

$index = $elasticaClient->getIndex('page');
$search->addIndex($index);

$hours = generateHours(100);
$start = microtime(true);

foreach ($hours as $i => $hour) {
    $from = $hour->modify("-30 day")->format(DateTime::RFC3339);
    $to = $hour->format(DateTime::RFC3339);

    $query = new Query([
        'size' => 0,
        'query' => [
            'bool' => [
                'filter' => [
                    [
                        'exists' => [
                            'field' => 'sessionId',
                            'boost' => 1
                        ]
                    ],
                    [
                        'match_all' => [
                            'boost' => 1
                        ]
                    ],
                    [
                        'range' => [
                            'eventDate' => [
                                'from' => $from,
                                'to' => $to,
                                'include_lower' => true,
                                'include_upper' => false,
                                'time_zone' => 'UTC',
                                'boost' => 1
                            ]
                        ]
                    ]
                ],
                'adjust_pure_negative' => true,
                'boost' => 1
            ]
        ],
        'track_total_hits' => 2147483647,
        'aggregations' => [
            'ranges' => [
                'date_range' => [
                    'field' => 'eventDate',
                    'time_zone' => 'UTC',
                    'ranges' => [
                        [
                            'key' => 'current',
                            'from' => $from,
                            'to' => $to
                        ]
                    ],
                    'keyed' => false
                ],
                'aggregations' => [
                    'terms' => [
                        'terms' => [
                            'script' => [
                                'source' => 'doc.canonicalUrl.value + \'@\' + doc.title.value + \'@\' + doc.dataSourceId.value',
                                'lang' => 'painless'
                            ],
                            'size' => 2147483647,
                            'min_doc_count' => 1,
                            'shard_min_doc_count' => 0,
                            'show_term_doc_count_error' => false,
                            'order' => [
                                [
                                    '_count' => 'desc'
                                ],
                                [
                                    '_key' => 'asc'
                                ]
                            ]
                        ],
                        'aggregations' => [
                            'entrances' => [
                                'sum' => [
                                    'field' => 'entrances'
                                ]
                            ],
                            'exits_field' => [
                                'sum' => [
                                    'field' => 'exits'
                                ]
                            ],
                            'bounce_field' => [
                                'sum' => [
                                    'field' => 'bounce'
                                ]
                            ],
                            'avgEngagementScore' => [
                                'avg' => [
                                    'field' => 'engagementScore'
                                ]
                            ],
                            'views' => [
                                'sum' => [
                                    'field' => 'views'
                                ]
                            ],
                            'total' => [
                                'filter' => [
                                    'bool' => [
                                        'filter' => [
                                            [
                                                'range' => [
                                                    'views' => [
                                                        'from' => 0,
                                                        'to' => null,
                                                        'include_lower' => false,
                                                        'include_upper' => true,
                                                        'boost' => 1
                                                    ]
                                                ]
                                            ]
                                        ],
                                        'adjust_pure_negative' => true,
                                        'boost' => 1
                                    ]
                                ],
                                'aggregations' => [
                                    'users_count' => [
                                        'cardinality' => [
                                            'field' => 'individualId',
                                            'precision_threshold' => 2000
                                        ]
                                    ],
                                    'missing_count' => [
                                        'missing' => [
                                            'field' => 'individualId'
                                        ],
                                        'aggregations' => [
                                            'sessions_count' => [
                                                'cardinality' => [
                                                    'field' => 'sessionId',
                                                    'precision_threshold' => 1000
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            'sessions_count' => [
                                'cardinality' => [
                                    'field' => 'sessionId',
                                    'precision_threshold' => 2000
                                ]
                            ],
                            'avgTimeOnPage' => [
                                'avg' => [
                                    'field' => 'timeOnPage'
                                ]
                            ],
                            'exits' => [
                                'bucket_script' => [
                                    'buckets_path' => [
                                        'sessions_count' => 'sessions_count',
                                        'exits_field' => 'exits_field'
                                    ],
                                    'script' => [
                                        'source' => 'params.sessions_count > 0 ? params.exits_field / params.sessions_count : 0',
                                        'lang' => 'painless'
                                    ],
                                    'gap_policy' => 'skip'
                                ]
                            ],
                            'visitors' => [
                                'bucket_script' => [
                                    'buckets_path' => [
                                        'sessions_count' => 'total > missing_count > sessions_count',
                                        'total_users_count' => 'total > users_count'
                                    ],
                                    'script' => [
                                        'source' => 'params.sessions_count + params.total_users_count',
                                        'lang' => 'painless'
                                    ],
                                    'gap_policy' => 'skip'
                                ]
                            ],
                            'bounce' => [
                                'bucket_script' => [
                                    'buckets_path' => [
                                        'sessions_count' => 'sessions_count',
                                        'bounce_field' => 'bounce_field'
                                    ],
                                    'script' => [
                                        'source' => 'params.sessions_count > 0 ? params.bounce_field / params.sessions_count  : 0',
                                        'lang' => 'painless'
                                    ],
                                    'gap_policy' => 'skip'
                                ]
                            ],
                            'bucket_sort_agg' => [
                                'bucket_sort' => [
                                    'sort' => [
                                        [
                                            'visitors' => [
                                                'order' => 'DESC',
                                                'unmapped_type' => 'long'
                                            ]
                                        ]
                                    ],
                                    'from' => 0,
                                    'size' => 20,
                                    'gap_policy' => 'SKIP'
                                ]
                            ]
                        ]
                    ]
                ]
            ]
        ]
    ]);

    $search->setQuery($query);
    $search->search()->getAggregations();
}

$end = microtime(true);
$executionTime = $end - $start;

echo "[Elasticsearch] Response time: $executionTime\r\n";
