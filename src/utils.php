<?php

require_once __DIR__ . '/../vendor/autoload.php';

const CLICKHOUSEDB = 'ClickHouseDB';
const ELASTICSEARCH = 'Elasticsearch';
const MEMSQL = 'MemSQL';
const TIMESCALEDB = 'TimescaleDB';

const STATUS_CODES_TYPE_INFORMATION = '1xx';
const STATUS_CODES_TYPE_SUCCESS = '2xx';
const STATUS_CODES_TYPE_REDIRECTION = '3xx';
const STATUS_CODES_TYPE_CLIENT_ERROR = '4xx';
const STATUS_CODES_TYPE_SERVER_ERROR = '5xx';
const STATUS_CODES_TYPE_UNKNOWN = '6xx';

const STATUS_CODES = [
    STATUS_CODES_TYPE_INFORMATION,
    STATUS_CODES_TYPE_SUCCESS,
    STATUS_CODES_TYPE_REDIRECTION,
    STATUS_CODES_TYPE_CLIENT_ERROR,
    STATUS_CODES_TYPE_SERVER_ERROR,
    STATUS_CODES_TYPE_UNKNOWN,
];

const DURATION_LAST_DAY = 24;
const DURATION_LAST_MONTH = 24 * 31;
const DURATION_LAST_YEAR = 24 * 31 * 12;

$devices = json_decode(file_get_contents(__DIR__ . '/fixtures/devices.json'), true);
$languages = file(__DIR__ . '/fixtures/language.txt', FILE_IGNORE_NEW_LINES);
$pages = json_decode(file_get_contents(__DIR__ . '/fixtures/pages.json'), true);
$places = json_decode(file_get_contents(__DIR__ . '/fixtures/places.json'), true);
$statusCodes = array_map(function ($statusCode) {
    return (int) $statusCode;
}, file(__DIR__ . '/fixtures/status-codes.txt', FILE_IGNORE_NEW_LINES));
$userAgents = file(__DIR__ . '/fixtures/user-agents.txt', FILE_IGNORE_NEW_LINES);
$project = uuid();

function findStatusCodeType(int $statusCode): string
{
    $search = ((string) $statusCode)[0];

    foreach (STATUS_CODES as $type) {
        if ($type[0] === $search) {
            return $type;
        }
    }
}

function getRandomValue(array $array = [])
{
    return $array[rand(0, count($array) - 1)];
}

function uuid(): string
{
    $data = random_bytes(16);
    $data[6] = chr(ord($data[6]) & 0x0f | 0x40);
    $data[8] = chr(ord($data[8]) & 0x3f | 0x80);

    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}

function generatePages(int $userCount = 1, int $duration = DURATION_LAST_DAY, int $perHourPagesCount = 10): Iterator
{
    $userIds = generateProjects($userCount);
    $hours = generateHours($duration);

    foreach ($userIds as $userId) {
        foreach ($hours as $hour) {
            for ($i = 0; $i < $perHourPagesCount; $i++) {
                yield generatePage($userId, $hour);
            }
        }
    }
}

function generatePage(String $userId, DateTimeImmutable $hour = null): array
{
    global $devices, $languages, $pages, $places;

    if (!$hour) {
        $hour = generateHour();
    }

    $country = $region = $city = "";

    foreach (getRandomValue($places) as $country => $regions) {
        if (!empty($regions) && isset($regions) && is_array($regions)) {
            foreach (getRandomValue($regions) as $region => $cities) {
                if (!empty($cities) && isset($cities) && is_array($cities)) {
                    $city = getRandomValue($cities);
                }
            }
        }
    }

    $platformName = $deviceType = $browserName = "";

    foreach (getRandomValue($devices) as $platformName => $deviceTypes) {
        if (!empty($deviceTypes) && isset($deviceTypes) && is_array($deviceTypes)) {
            foreach (getRandomValue($deviceTypes) as $deviceType => $browserNames) {
                if (!empty($browserNames) && isset($browserNames) && is_array($browserNames)) {
                    $browserName = getRandomValue($browserNames);
                }
            }
        }
    }

    $canonicalUrl = $title = $url = "";

    foreach (getRandomValue($pages) as $canonicalUrl => $titles) {
        if (!empty($titles) && isset($titles) && is_array($titles)) {
            foreach (getRandomValue($titles) as $title => $urls) {
                if (!empty($urls) && isset($urls) && is_array($urls)) {
                    $url = getRandomValue($urls);
                }
            }
        }
    }

    $language = getRandomValue($languages);

    $directAccess = rand(0, 3);
    $directAccessDates = generateHours($directAccess, $hour);

    $indirectAccess = rand(0, 3);
    $indirectAccessDates = generateHours($indirectAccess, $hour);

    $firstEventDate = generateHour($hour);

    $interaction = rand(0, 3);
    $interactionDates = generateHours($interaction, $hour);

    $lastEventDate = generateHour($firstEventDate);

    return [
        'asset' => var_export((bool)rand(0, 1), true),
        'bounce' => rand(0, 100),
        'browserName' => $browserName,
        'canonicalUrl' => $canonicalUrl,
        'channelId' => "123456789012345678",
        'city' => $city,
        'contentLanguageId' => $language,
        'country' => $country,
        'ctaClicks' => rand(0, 100),
        'dataSourceId' => "123456789012345678",
        'deviceType' => $deviceType,
        'directAccess' => $directAccess,
        'directAccessDates' => $directAccessDates,
        'engagementScore' => mt_rand() / mt_getrandmax(),
        'entrances' => rand(0, 100),
        'eventDate' => $hour,
        'exits' => rand(0, 100),
        'experienceId' => "DEFAULT",
        'experimentId' => "",
        'firstEventDate' => $firstEventDate,
        'formSubmissions' => rand(0, 100),
        'indirectAccess' => $indirectAccess,
        'indirectAccessDates' => $indirectAccessDates,
        'individualId' => $userId,
        'interactionDates' => $interactionDates,
        'knownIndividual' => var_export((bool)rand(0, 1), true),
        'lastEventDate' => $lastEventDate,
        'modifiedDate' => $lastEventDate,
        'pageScrolls' => generatePageScrolls($hour),
        'platformName' => $platformName,
        'primaryKey' => uuid(),
        'reads' => rand(0, 100),
        'region' => $region,
        'searchTerm' => "",
        'segmentNames' => [],
        'sessionId' => $userId,
        'timeOnPage' => rand(1000, 100000),
        'title' => $title,
        'url' => $url,
        'userId' => $userId,
        'variantId' => "",
        'views' => rand(0, 100),
    ];
}

function generatePageScrolls(DateTimeImmutable $hour): array
{
    $depths = array(0, 25, 50, 75, 100);

    $depth = getRandomValue($depths);

    if ($depth == 0) {
        return [];
    }

    return [
        'depth' => $depth,
        'eventDate' => generateHour($hour),
    ];
}

function generatePoint(string $project = null, DateTimeImmutable $hour = null): array
{
    global $statusCodes, $userAgents;

    if (!$project) {
        global $project;
    }

    if (!$hour) {
        $hour = generateHour();
    }

    $statusCode = getRandomValue($statusCodes);
    $userAgent = getRandomValue($userAgents);

    $statusCodeType = findStatusCodeType((int) $statusCode);

    return [
        'measurement' => 'statistic',
        'value' => rand(0, 1000000),
        'tags' => [
            'project' => $project,
            'statusCode' => $statusCode,
            'statusCodeType' => $statusCodeType,
            'userAgent' => substr($userAgent, 0, 25),
            'userAgentType' => rand(1, 4),
        ],
        'date' => $hour,
    ];
}

function generatePoints(string $project = null, DateTimeImmutable $hour = null, int $size = 10): array
{
    $points = [];
    for ($i = 0; $i < $size; $i++) {
        $points[] = generatePoint($project, $hour);
    }

    return $points;
}

function generateProjects(int $size = 10): array
{
    $projects = [];
    for ($i = 0; $i < $size; $i++) {
        $projects[] = uuid();
    }

    return $projects;
}

function generateHour(DateTimeImmutable $hour = null): DateTimeImmutable
{
    if (!$hour) {
        $today = new DateTimeImmutable();
        return $today->setTime((int) $today->format('H'), 0);
    }

    $minute = rand(0, 24);
    $second = rand(0, 60);

    return $hour->modify("+$minute minute +$second second");
}
function generateDays(int $type = 1)
{
    $days = [];

    $today = new DateTimeImmutable();

    for ($i = 0; $i < $type; $i++) {
        $days[] = $today->setTime(0, 0)->modify("-$i day");
    }

    return $days;
}

function generateHours(int $type = DURATION_LAST_DAY, DateTimeImmutable $hour = null)
{
    $hours = [];
    for ($i = 0; $i < $type; $i++) {
        $hours[] = generateHour($hour)->modify("-$i hour");
    }

    return $hours;
}

function generateFixtures(int $projectCount = 1, int $duration = DURATION_LAST_DAY, int $perHourUserAgentsCount = 10, int $perHourStatusCodesCount = 5, string $tsdb = ELASTICSEARCH): Iterator
{
    $projects = generateProjects($projectCount);
    $hours = generateHours($duration);
    $perHourPointsCount = $perHourUserAgentsCount * $perHourStatusCodesCount;

    foreach ($projects as $project) {
        foreach ($hours as $hour) {
            yield generatePoints($project, $hour, $perHourPointsCount);
        }
    }
}
