<?php
namespace fulldecent\GoogleSheetsEtl;

require_once __DIR__ . '/../vendor/autoload.php';

$database = new \PDO('sqlite::memory:', null, null, [
  \PDO::ATTR_PERSISTENT => false,
  \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
]);

$databaseAgent = new DatabaseAgentSqlite($database);
$databaseAgent->setupDatabase();

## Test ########################################################################
echo "Checking table created" . PHP_EOL;
$tableCount = $database->query('SELECT COUNT(*) FROM sqlite_master WHERE name ="__meta_table_index"')->fetchColumn();
assert ($tableCount == 1);
echo "~~ Passed" . PHP_EOL . PHP_EOL;

## Test ########################################################################
$headers = ['col 1', 'col 2'];
$data = [
  ['a', 'b'],
  ['c', 'd']
];
$tableName = '17azPU9lRfRMzFFbwj4SREN-jbzCalUvUWpDHAYCZ1vs-sname';
$databaseAgent->loadSheet('17azPU9lRfRMzFFbwj4SREN-jbzCalUvUWpDHAYCZ1vs', 'sname', 'now', $headers, $data, $tableName);

var_dump($database->query('SELECT * FROM __meta_table_index')->fetchAll(\PDO::FETCH_OBJ));
var_dump($database->query("SELECT * FROM `$tableName`")->fetchAll(\PDO::FETCH_OBJ));
