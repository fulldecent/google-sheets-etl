<?php
namespace fulldecent\GoogleSheetsEtl;
require_once __DIR__ . '/../vendor/autoload.php';
set_time_limit(300);
error_reporting(E_ALL);

const CREDENTIALS_FILE = __DIR__ . '/google-shared-team-dashboard-a38a4e97c700.json';
//date_default_timezone_set('UTC'); // Bug in Google libraries

$databaseFile = __DIR__ . '/../local/example-data-mart.db';
$database = new \PDO('sqlite:' . $databaseFile, null, null, [
    \PDO::ATTR_PERSISTENT => false,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
]);
//  [\PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8"
/*$database->set_error_handler(function($error) {
    throw $error;
});
*/

## Banner #####################################################################@
echo 'GOOGLE SHEETS ETL TOOL' . PHP_EOL;

$serviceAccountConfigurations = glob(__DIR__ . '/../local/*-*.json');
if (count($serviceAccountConfigurations) !== 1) {
    echo 'Production service account configuration not found. Skipping test.' . PHP_EOL;
    exit(0);
}

$tasks = new Tasks($serviceAccountConfigurations[0], $database);

echo 'Service account: ' . $tasks->googleSheetsAgent->getAccountName() . PHP_EOL;
echo 'Database: ' . $databaseFile . PHP_EOL;

/*
$oldestSpreadsheets = $tasks->googleSheetsAgent->getOldestSpreadsheets();
echo json_encode($oldestSpreadsheets, JSON_PRETTY_PRINT) . PHP_EOL;

$sheetTitles = $tasks->googleSheetsAgent->getGridSheetTitles('1n6BuHFHy_p-Mjv7YtbPDz65SceRvyAwbshy3FAUhvwU');
echo json_encode($sheetTitles, JSON_PRETTY_PRINT) . PHP_EOL;

$tasks->loadSheet('1n6BuHFHy_p-Mjv7YtbPDz65SceRvyAwbshy3FAUhvwU', 'Sheet1', rfc3339);
*/

/*
$spreadsheetId = '1n6BuHFHy_p-Mjv7YtbPDz65SceRvyAwbshy3FAUhvwU';
$tasks->loadSpreadsheet($spreadsheetId, $modified);
*/

$tasks->loadSomeNewerSpreadsheets();