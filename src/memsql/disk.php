<?php

ini_set('memory_limit', '-1');

require_once __DIR__ . '/../utils.php';

use Amp\Mysql;

Amp\Loop::run(function () {
    $config = Mysql\ConnectionConfig::fromString('host=localhost user=root port=13306');

    $connection = yield Mysql\connect($config);

    $result =  yield $connection->query("SELECT SUM(MEMORY_USE)/1024/1024 m FROM information_schema.TABLE_STATISTICS WHERE DATABASE_NAME = 'statistic' GROUP BY DATABASE_NAME");
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        echo $row['m'] . " ram\n";
    }

    $result =  yield $connection->query("SELECT sum(compressed_size)/1024/1024 size FROM information_schema.COLUMNAR_SEGMENTS WHERE DATABASE_NAME = 'statistic' GROUP BY DATABASE_NAME");
    while (yield $result->advance()) {
        $row = $result->getCurrent();
        echo $row['size'] . " disk\n";
    }
});
