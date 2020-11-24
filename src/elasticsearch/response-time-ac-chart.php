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

$days = generateDays(100);
$start = microtime(true);

foreach ($days as $i => $day) {
    $from = $day->modify("-60 day")->format(DateTime::RFC3339);
    $middle = $day->modify("-30 day")->format(DateTime::RFC3339);
    $to = $day->format(DateTime::RFC3339);

    $query = new Query([
        "size" => 0,
        "query" => [
            "bool" => [
                "filter" => [
                    [
                        "range" => [
                            "eventDate" => [
                                'from' => $from,
                                'to' => $to,
                                "include_lower" => true,
                                "include_upper" => false,
                                "time_zone" => "UTC",
                                "boost" => 1
                            ]
                        ]
                    ]
                ],
                "adjust_pure_negative" => true,
                "boost" => 1
            ]
        ],
        "track_total_hits" => 2147483647,
        "aggregations" => [
            "period_ranges" => [
                "date_range" => [
                    "field" => "eventDate",
                    "time_zone" => "UTC",
                    "ranges" => [
                        [
                            "key" => "current",
                            'from' => $middle,
                            'to' => $to,
                        ],
                        [
                            "key" => "previous",
                            'from' => $from,
                            'to' => $middle,
                        ]
                    ],
                    "keyed" => false
                ],
                "aggregations" => [
                    "metric_over_time" => [
                        "date_histogram" => [
                            "field" => "eventDate",
                            "time_zone" => "UTC",
                            "calendar_interval" => "1d",
                            "offset" => -86400000,
                            "order" => [
                                "_key" => "asc"
                            ],
                            "keyed" => false,
                            "min_doc_count" => 0
                        ],
                        "aggregations" => [
                            "known" => [
                                "filter" => [
                                    "bool" => [
                                        "filter" => [
                                            [
                                                "bool" => [
                                                    "should" => [
                                                        [
                                                            "term" => [
                                                                "knownIndividual" => [
                                                                    "value" => true,
                                                                    "boost" => 1
                                                                ]
                                                            ]
                                                        ],
                                                        [
                                                            "bool" => [
                                                                "filter" => [
                                                                    [
                                                                        "exists" => [
                                                                            "field" => "individualId",
                                                                            "boost" => 1
                                                                        ]
                                                                    ]
                                                                ],
                                                                "must_not" => [
                                                                    [
                                                                        "exists" => [
                                                                            "field" => "q   ",
                                                                            "boost" => 1
                                                                        ]
                                                                    ]
                                                                ],
                                                                "adjust_pure_negative" => true,
                                                                "boost" => 1
                                                            ]
                                                        ]
                                                    ],
                                                    "adjust_pure_negative" => true,
                                                    "boost" => 1
                                                ]
                                            ],
                                            [
                                                "range" => [
                                                    "views" => [
                                                        "from" => 0,
                                                        "to" => null,
                                                        "include_lower" => false,
                                                        "include_upper" => true,
                                                        "boost" => 1
                                                    ]
                                                ]
                                            ]
                                        ],
                                        "adjust_pure_negative" => true,
                                        "boost" => 1
                                    ]
                                ],
                                "aggregations" => [
                                    "users_count" => [
                                        "cardinality" => [
                                            "field" => "individualId",
                                            "precision_threshold" => 1000
                                        ]
                                    ]
                                ]
                            ],
                            "total" => [
                                "filter" => [
                                    "bool" => [
                                        "filter" => [
                                            [
                                                "range" => [
                                                    "views" => [
                                                        "from" => 0,
                                                        "to" => null,
                                                        "include_lower" => false,
                                                        "include_upper" => true,
                                                        "boost" => 1
                                                    ]
                                                ]
                                            ]
                                        ],
                                        "adjust_pure_negative" => true,
                                        "boost" => 1
                                    ]
                                ],
                                "aggregations" => [
                                    "users_count" => [
                                        "cardinality" => [
                                            "field" => "individualId",
                                            "precision_threshold" => 1000
                                        ]
                                    ],
                                    "missing_count" => [
                                        "missing" => [
                                            "field" => "individualId"
                                        ],
                                        "aggregations" => [
                                            "sessions_count" => [
                                                "cardinality" => [
                                                    "field" => "sessionId",
                                                    "precision_threshold" => 1000
                                                ]
                                            ]
                                        ]
                                    ]
                                ]
                            ],
                            "anonymousVisitors" => [
                                "bucket_script" => [
                                    "buckets_path" => [
                                        "sessions_count" => "total > missing_count > sessions_count",
                                        "known_users_count" => "known > users_count",
                                        "total_users_count" => "total > users_count"
                                    ],
                                    "script" => [
                                        "source" => "params.sessions_count + params.total_users_count - params.known_users_count",
                                        "lang" => "painless"
                                    ],
                                    "gap_policy" => "skip"
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
