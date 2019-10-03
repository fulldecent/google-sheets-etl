<?php
namespace fulldecent\GoogleSheetsEtl;

require_once __DIR__ . '/../vendor/autoload.php';

set_time_limit(300);

const CREDENTIALS_FILE = __DIR__ . '/google-shared-team-dashboard-a38a4e97c700.json';
date_default_timezone_set('UTC'); // Bug in Google libraries

$database = new \PDO('sqlite:./example-data-mart.db', null, null, [
    \PDO::ATTR_PERSISTENT => false,
    \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
]);
//  [\PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8"
/*$database->set_error_handler(function($error) {
    throw $error;
});
*/

# Banner
echo 'GOOGLE SHEETS ETL TOOL' . PHP_EOL;

# testing google sheets
$googleSheetsAgent = new GoogleSheetsAgent(CREDENTIALS_FILE);
var_dump($googleSheetsTitles->getGridSheetNames('1-Dcs8ZYoyz82rkjkv3tIBSCAJOTpouXor3dwql4TqiY'));





//$googleSheetsEtl = new GoogleSheetsEtl(CREDENTIALS_FILE, $database);
#$googleSheetsEtl->setSchema('etl_google_sheets');
#$configuration = json_decode(file_get_contents(__DIR__ . '/etl-google-sheets.json'));
#$googleSheetsEtl->setConfiguration($configuration->spreadsheets);
//$googleSheetsEtl->synchronizeSomeSpreadsheets();


#$databaseAgent = new DatabaseAgent($database);
#$databaseAgent->setupDatabaseMySql();
#$databaseAgent->setupDatabaseSqlite();