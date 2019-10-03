<?php
namespace fulldecent\GoogleSheetsEtl;

require_once __DIR__ . '/../vendor/autoload.php';

$database = new \PDO('sqlite::memory:', null, null, [
  \PDO::ATTR_PERSISTENT => false,
  \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
]);

$databaseAgent = new DatabaseAgent($database);
$databaseAgent->setupDatabaseSqlite();

/*
var_dump($database->query('SELECT * FROM sqlite_master')->fetchAll(\PDO::FETCH_OBJ));
var_dump($databaseAgent->getLatestMotidifedTime());
var_dump($databaseAgent->getGreatestIdWithAuthorizationCheckedSince('2000-02-03'));
*/
echo '--- ---' . PHP_EOL;
echo '--- ---' . PHP_EOL;

$headers = ['col 1', 'col 2'];
$data = [
  ['a', 'b'],
  ['c', 'd']
];
$databaseAgent->loadSheet('17azPU9lRfRMzFFbwj4SREN-jbzCalUvUWpDHAYCZ1vs', 'sname', 'now', $headers, $data);

var_dump($database->query('SELECT * FROM __meta_table_index')->fetchAll(\PDO::FETCH_OBJ));
var_dump($database->query('SELECT * FROM `17azPU9lRfRMzFFbwj4SREN-jbzCalUvUWpDHAYCZ1vs-sname`')->fetchAll(\PDO::FETCH_OBJ));
