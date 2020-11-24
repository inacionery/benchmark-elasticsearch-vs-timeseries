<?php

require_once __DIR__ . '/../utils.php';

class Page
{

    public $canonicalUrl;
    public $title;
    public $dataSourceId;
    public $users_count;
    public $missing_sessions_count;
    public $avgTimeOnPage;
    public $avgEngagementScore;
    public $sessions_count;
    public $bounce_field;
    public $exits_field;
    public $entrances;
    public $views;
    public $exits;
    public $visitors;
    public $bounce;

    public function isEqual($other): bool
    {
        return $other instanceof $this && $other->canonicalUrl === $this->canonicalUrl && $other->title === $this->title && $other->dataSourceId === $this->dataSourceId && $other->users_count === $this->users_count && $other->missing_sessions_count === $this->missing_sessions_count && $other->avgTimeOnPage === $this->avgTimeOnPage && $other->avgEngagementScore === $this->avgEngagementScore && $other->sessions_count === $this->sessions_count && $other->bounce_field === $this->bounce_field && $other->exits_field === $this->exits_field && $other->entrances === $this->entrances && $other->views === $this->views && $other->exits === $this->exits && $other->visitors === $this->visitors && $other->bounce === $this->bounce;
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

    $hours = generateHours(10);
    $start = microtime(true);

    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-30 day")->format(DateTime::ISO8601);
        $to = $hour->format(DateTime::ISO8601);

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
        $aggregations = $search->search()->getAggregations();
        $buckets = $aggregations['ranges']["buckets"][0]["terms"]["buckets"];
        $elasticPages = array();
        foreach ($buckets as &$bucket) {
            $key = explode('@', $bucket["key"]);

            $page = new Page();
            $page->avgEngagementScore = strval(round(floatval($bucket["avgEngagementScore"]["value"]), 4));
            $page->avgTimeOnPage = strval(round(floatval($bucket["avgTimeOnPage"]["value"]), 4));
            $page->bounce = strval(round(floatval($bucket["bounce"]["value"]), 0));
            $page->bounceField = strval($bucket["bounce_field"]["value"]);
            $page->canonicalUrl = strval($key[0]);
            $page->dataSourceId = strval($key[2]);
            $page->entrances = strval($bucket["entrances"]["value"]);
            $page->exits = strval(round(floatval($bucket["exits"]["value"]), 0));
            $page->exitsField = strval($bucket["exits_field"]["value"]);
            $page->missingSessionsCount = strval($bucket["total"]["missing_count"]["sessions_count"]["value"]);
            $page->sessionsCount = strval($bucket["sessions_count"]["value"]);
            $page->title = strval($key[1]);
            $page->usersCount = strval($bucket["total"]["users_count"]["value"]);
            $page->views = strval($bucket["views"]["value"]);
            $page->visitors = strval($bucket["visitors"]["value"]);

            array_push($elasticPages, $page);
        }

        $result = yield $connection->query("WITH top AS ( SELECT canonicalUrl, title, dataSourceId, ( COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END ) + COUNT(DISTINCT individualId) ) AS visitors, SUM(total) AS total FROM eventDate_one_hour WHERE one_hour >= '$from' AND one_hour < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20 ) SELECT canonicalUrl, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END ) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounces) AS bounce_field, SUM(timeOnPages)/top.total AS avgTimeOnPage, SUM(engagementScores)/top.total AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounces) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, visitors, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM eventDate_one_hour JOIN top USING (canonicalUrl, title, dataSourceId) WHERE one_hour >= '$from' AND one_hour < '$to' AND sessionId IS NOT NULL GROUP BY top.total, visitors, canonicalUrl, title, dataSourceId ORDER BY visitors DESC, top.total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC;");

        $timescalePages = array();
        while (yield $result->advance()) {
            $row = $result->getCurrent();

            $page = new Page();
            $page->avgEngagementScore = strval(round(floatval($row["avgengagementscore"]), 4));
            $page->avgTimeOnPage = strval(round(floatval($row["avgtimeonpage"]), 4));
            $page->bounce = strval(round(floatval($row["bounce"]), 0));
            $page->bounceField = strval($row["bounce_field"]);
            $page->canonicalUrl = strval($row["canonicalurl"]);
            $page->dataSourceId = strval($row["datasourceid"]);
            $page->entrances = strval($row["entrances"]);
            $page->exits = strval(round(floatval($row["exits"]), 0));
            $page->exitsField = strval($row["exits_field"]);
            $page->missingSessionsCount = strval($row["missing_sessions_count"]);
            $page->sessionsCount = strval($row["sessions_count"]);
            $page->title = strval($row["title"]);
            $page->usersCount = strval($row["users_count"]);
            $page->views = strval($row["views"]);
            $page->visitors = strval($row["visitors"]);

            array_push($timescalePages, $page);
        }

        $resultMEMSQL = yield $connectionMEMSQL->query("WITH top AS (SELECT canonicalUrl, title, dataSourceId, (COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) + COUNT(DISTINCT individualId)) AS visitors, count(*) as total FROM page WHERE eventHour >= '$from' AND eventHour < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20) SELECT canonicalUrl, title, dataSourceId, SUM(views) AS views, COUNT(DISTINCT sessionId) AS sessions_count, SUM(exits) AS exits_field, COUNT(DISTINCT individualId) AS users_count, COUNT(DISTINCT CASE WHEN individualId IS NULL THEN sessionId END) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, AVG(timeOnPage) AS avgTimeOnPage, AVG(engagementScore) AS avgEngagementScore, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(bounce) / COUNT(DISTINCT sessionId) ELSE 0 END AS bounce, visitors, total, CASE WHEN COUNT(DISTINCT sessionId) > 0 THEN SUM(exits) / COUNT(DISTINCT sessionId) ELSE 0 END AS exits FROM page JOIN top USING (canonicalUrl, title, dataSourceId) WHERE eventHour >= '$from' AND eventHour < '$to' AND sessionId IS NOT NULL GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC;");

        $memsqlPages = array();
        while (yield $resultMEMSQL->advance()) {
            $row = $resultMEMSQL->getCurrent();

            $page = new Page();
            $page->avgEngagementScore = strval(round(floatval($row["avgEngagementScore"]), 4));
            $page->avgTimeOnPage = strval(round(floatval($row["avgTimeOnPage"]), 4));
            $page->bounce = strval(round(floatval($row["bounce"]), 0));
            $page->bounceField = strval($row["bounce_field"]);
            $page->canonicalUrl = strval($row["canonicalUrl"]);
            $page->dataSourceId = strval($row["dataSourceId"]);
            $page->entrances = strval($row["entrances"]);
            $page->exits = strval(round(floatval($row["exits"]), 0));
            $page->exitsField = strval($row["exits_field"]);
            $page->missingSessionsCount = strval($row["missing_sessions_count"]);
            $page->sessionsCount = strval($row["sessions_count"]);
            $page->title = strval($row["title"]);
            $page->usersCount = strval($row["users_count"]);
            $page->views = strval($row["views"]);
            $page->visitors = strval($row["visitors"]);

            array_push($memsqlPages, $page);
        }

        for ($i = 0; $i < 20; $i++) {

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
