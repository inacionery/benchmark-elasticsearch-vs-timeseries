<?php

require_once __DIR__ . '/../utils.php';

class Page
{
    public $range;
    public $anonymousVisitors;
    public $eventDate;
    public $known_users_count;
    public $missing_sessions_count;
    public $total_users_count;

    public function isEqual($other): bool
    {
        return $other instanceof $this && $other->range === $this->range && $other->anonymousVisitors === $this->anonymousVisitors && $other->eventDate === $this->eventDate && $other->known_users_count === $this->known_users_count && $other->missing_sessions_count === $this->missing_sessions_count && $other->total_users_count === $this->total_users_count;
    }
}

use Amp\Postgres;

use Amp\Mysql;

use Elastica\Client;
use Elastica\Query;
use Elastica\Search;

Amp\Loop::run(function () {
    $elasticaClient = new Client([
        'host' => 'localhost',
        'port' => 19200,
    ]);

    $search = new Search($elasticaClient);

    $index = $elasticaClient->getIndex('page');
    $search->addIndex($index);

    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=15432');

    $connection = yield Postgres\connect($config);

    $configMEMSQL = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connectionMEMSQL = yield Mysql\connect($configMEMSQL);

    yield $connectionMEMSQL->query("USE page");

    $hours = generateDays(1);
    $start = microtime(true);

    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-60 day")->format(DateTime::RFC3339);
        $middle = $hour->modify("-30 day")->format(DateTime::RFC3339);
        $to = $hour->format(DateTime::RFC3339);

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
                                                                                "field" => "knownIndividual",
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
                                                            "to" => NULL,
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
                                                            "to" => NULL,
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
        $aggregations = $search->search()->getAggregations();
        $previous = $aggregations['period_ranges']["buckets"][0];
        $current = $aggregations['period_ranges']["buckets"][1];

        $buckets =  $previous["metric_over_time"]["buckets"];
        $elasticPages = array();
        foreach ($buckets as &$bucket) {
            $page = new Page();

            $page->range = strval($previous["key"]);
            $page->eventDate = strval($bucket["key_as_string"]);
            $page->missing_sessions_count = strval($bucket["total"]["missing_count"]["sessions_count"]["value"]);
            $page->total_users_count = strval($bucket["total"]["users_count"]["value"]);
            $page->known_users_count = strval($bucket["known"]["users_count"]["value"]);
            $page->anonymousVisitors = strval($bucket["anonymousVisitors"]["value"]);

            array_push($elasticPages, $page);
        }
        $buckets =  $current["metric_over_time"]["buckets"];
        foreach ($buckets as &$bucket) {
            $page = new Page();

            $page->range = strval($current["key"]);
            $page->eventDate = strval($bucket["key_as_string"]);
            $page->missing_sessions_count = strval($bucket["total"]["missing_count"]["sessions_count"]["value"]);
            $page->total_users_count = strval($bucket["total"]["users_count"]["value"]);
            $page->known_users_count = strval($bucket["known"]["users_count"]["value"]);
            $page->anonymousVisitors = strval($bucket["anonymousVisitors"]["value"]);

            array_push($elasticPages, $page);
        }

        $result = yield $connection->query("SELECT * FROM histogram_one_day WHERE one_day >= '$from' AND one_day < '$middle'");

        $timescalePages = array();
        while (yield $result->advance()) {
            $row = $result->getCurrent();

            $page = new Page();

            $page->range = strval("previous");
            $page->eventDate = date('Y-m-d\TH:i:s.v\Z', strtotime($row["one_day"]));
            $page->missing_sessions_count = strval($row["sessions_count"]);
            $page->total_users_count = strval($row["users_count"]);
            $page->known_users_count = strval($row["known_users_count"]);
            $page->anonymousVisitors = strval($row["anonymousvisitors"]);

            array_push($timescalePages, $page);
        }

        $result = yield $connection->query("SELECT * FROM histogram_one_day WHERE one_day >= '$middle' AND one_day < '$to'");

        while (yield $result->advance()) {
            $row = $result->getCurrent();

            $page = new Page();

            $page->range = strval("current");
            $page->eventDate = date('Y-m-d\TH:i:s.v\Z', strtotime($row["one_day"]));
            $page->missing_sessions_count = strval($row["sessions_count"]);
            $page->total_users_count = strval($row["users_count"]);
            $page->known_users_count = strval($row["known_users_count"]);
            $page->anonymousVisitors = strval($row["anonymousvisitors"]);

            array_push($timescalePages, $page);
        }

        $resultMEMSQL = yield $connectionMEMSQL->query("SELECT eventDay, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page WHERE eventDay >= '$from' AND eventDay < '$middle' GROUP BY eventDay ORDER BY eventDay ASC");

        $memsqlPages = array();
        while (yield $resultMEMSQL->advance()) {
            $row = $resultMEMSQL->getCurrent();

            $page = new Page();

            $page->range = strval("previous");
            $page->eventDate = date('Y-m-d\TH:i:s.v\Z', strtotime($row["eventDay"]));
            $page->missing_sessions_count = strval($row["sessions_count"]);
            $page->total_users_count = strval($row["users_count"]);
            $page->known_users_count = strval($row["known_users_count"]);
            $page->anonymousVisitors = strval($row["anonymousVisitors"]);

            array_push($memsqlPages, $page);
        }

        $resultMEMSQL = yield $connectionMEMSQL->query("SELECT eventDay, COUNT(DISTINCT CASE WHEN knownIndividual = true or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) AS known_users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) AS sessions_count, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN ( individualId is NULL ) THEN sessionId END) + COUNT(DISTINCT  CASE WHEN knownIndividual or ( individualId is not NULL and knownIndividual is NULL ) THEN individualId END) - COUNT(DISTINCT individualId) AS anonymousVisitors FROM page WHERE eventDay >= '$middle' AND eventDay < '$to' GROUP BY eventDay ORDER BY eventDay ASC");

        while (yield $resultMEMSQL->advance()) {
            $row = $resultMEMSQL->getCurrent();

            $page = new Page();

            $page->range = strval("current");
            $page->eventDate = date('Y-m-d\TH:i:s.v\Z', strtotime($row["eventDay"]));
            $page->missing_sessions_count = strval($row["sessions_count"]);
            $page->total_users_count = strval($row["users_count"]);
            $page->known_users_count = strval($row["known_users_count"]);
            $page->anonymousVisitors = strval($row["anonymousVisitors"]);

            array_push($memsqlPages, $page);
        }

        for ($i = 0; $i < sizeof($elasticPages); $i++) {
            if (!$elasticPages[$i]->isEqual($timescalePages[$i])) {
                var_dump("timescale");
                var_dump($i);
                var_dump($elasticPages[$i]);
                var_dump($timescalePages[$i]);
            }

            if (!$elasticPages[$i]->isEqual($memsqlPages[$i])) {
                var_dump("memsql");
                var_dump($i);
                var_dump($elasticPages[$i]);
                var_dump($memsqlPages[$i]);
            }
        }
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "Response time: $executionTime\r\n";
});
