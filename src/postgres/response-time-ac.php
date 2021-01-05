<?php

require_once __DIR__ . '/../utils.php';

use Amp\Postgres;

Amp\Loop::run(function () {
    $config = Postgres\ConnectionConfig::fromString('host=localhost user=postgres password=password port=5432');

    $connection = yield Postgres\connect($config);

    $hours = generateHours(100);
    $start = microtime(true);
    foreach ($hours as $i => $hour) {
        $from = $hour->modify("-90 day");
        $to = $hour->modify("-60 day");

        $map = new Ds\Map();
        dateDifference($map, $from, $to);

        $map->keys();
        $keys = $map->keys()->toArray();
        $from_query = "(";
        for ($k = 0; $k < count($keys); $k++) {
            if ($k > 0) {
                $from_query .= " union all ";
            }

            $values = $map->get($keys[$k]);

            $from_query .= "select * from $keys[$k]_eventDate_view WHERE ";

            for ($j = 0; $j < count($values); $j += 2) {
                if ($j > 0) {
                    $from_query .= " OR ";
                }
                $start_date = $values[$j]->format(DateTime::ISO8601);
                $end_date = $values[$j + 1]->format(DateTime::ISO8601);
                $from_query .= "(time >= '$start_date' AND time < '$end_date')";
            }
        }
        $from_query .= ") as eventDate_view";

        yield $connection->query("SELECT canonicalUrl, title, dataSourceId, SUM(total) as total, COUNT_ARRAY_AGGREGATE(sessionIds) AS sessions_count, COUNT_ARRAY_AGGREGATE(individualIds) AS users_count, SUM(views) AS views, SUM(exits) AS exits_field, COUNT_ARRAY_AGGREGATE(missingSessions) AS missing_sessions_count, SUM(entrances) AS entrances, SUM(bounce) AS bounce_field, SUM(timeOnPage) / SUM(total) AS avgTimeOnPage, SUM(engagementScore) / SUM(total) AS avgEngagementScore, CASE WHEN COUNT_ARRAY_AGGREGATE(sessionIds) > 0 THEN SUM(bounce) / COUNT_ARRAY_AGGREGATE(sessionIds) ELSE 0 END AS bounces, CASE WHEN COUNT_ARRAY_AGGREGATE(sessionIds) > 0 THEN SUM(exits) / COUNT_ARRAY_AGGREGATE(sessionIds) ELSE 0 END AS exit, COUNT_ARRAY_AGGREGATE(missingSessions) + COUNT_ARRAY_AGGREGATE(individualIds) AS visitors FROM $from_query GROUP BY canonicalUrl, title, dataSourceId ORDER BY visitors DESC, total DESC, canonicalUrl ASC, title ASC, dataSourceId ASC LIMIT 20");
    }

    $end = microtime(true);
    $executionTime = $end - $start;

    echo "[PostgreSQL] Materialized Response time: $executionTime\r\n";
});


function
dateDifference($map, $date_1, $date_2)
{

    if ($date_1 == $date_1->modify('first day of this month today') && $date_2 == $date_2->modify('first day of this month today')) {
        $value = $map->get('month', []);
        $value[] = $date_1;
        $value[] = $date_2;
        $map->put('month', $value);

        return;
    }

    if ($date_1 == $date_1->modify('first day of this month today') && $date_2 > $date_1->modify('first day of next month today')) {
        $value = $map->get('month', []);
        $value[] = $date_1;
        $value[] = $date_1->modify('first day of next month today');
        $map->put('month', $value);

        return dateDifference($map, $date_1->modify('first day of next month today'), $date_2);
    }

    if ($date_2 > $date_1->modify('first day of next month today')) {
        dateDifference($map, $date_1, $date_1->modify('first day of next month today'));
        dateDifference($map, $date_1->modify('first day of next month today'), $date_2);

        return;
    }

    if ($date_1 == $date_1->modify('Monday this week today') && $date_2 == $date_2->modify('Monday this week today')) {
        $value = $map->get('week', []);
        $value[] = $date_1;
        $value[] = $date_2;
        $map->put('week', $value);

        return;
    }

    if ($date_1 == $date_1->modify('Monday this week today') && $date_2 > $date_1->modify('Monday next week today')) {
        $value = $map->get('week', []);
        $value[] = $date_1;
        $value[] = $date_1->modify('Monday next week today');
        $map->put('week', $value);

        return dateDifference($map, $date_1->modify('Monday next week today'), $date_2);
    }

    if ($date_2 > $date_1->modify('Monday next week today')) {
        dateDifference($map, $date_1, $date_1->modify('Monday next week today'));
        dateDifference($map, $date_1->modify('Monday next week today'), $date_2);

        return;
    }

    if ($date_1 == $date_1->modify('today') && $date_2 == $date_2->modify('today')) {
        $value = $map->get('day', []);
        $value[] = $date_1;
        $value[] = $date_2;
        $map->put('day', $value);

        return;
    }

    if ($date_1->diff($date_1->modify('tomorrow'))->h > 0) {
        $value = $map->get('hour', []);
        $value[] = $date_1;
        $value[] = $date_1->modify('tomorrow');
        $map->put('hour', $value);

        return dateDifference($map, $date_1->modify('tomorrow'), $date_2);
    }

    if ($date_2->modify('today')->diff($date_2)->h > 0) {
        $value = $map->get('hour', []);
        $value[] = $date_2->modify('today');
        $value[] = $date_2;
        $map->put('hour', $value);

        return dateDifference($map, $date_1, $date_2->modify('today'));
    }

    $value = $map->get('hour', []);
    $value[] = $date_1;
    $value[] = $date_2;
    return $map->put('hour', $value);
}
