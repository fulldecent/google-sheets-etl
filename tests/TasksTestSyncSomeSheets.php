<?php
namespace fulldecent\GoogleSheetsEtl;

require_once __DIR__ . '/../vendor/autoload.php';

$serviceAccountConfigurations = glob(__DIR__ . '/../local/*-*.json');

if (count($serviceAccountConfigurations) !== 1) {
  echo 'Production service account configuration not found. Skipping test.' . PHP_EOL;
  exit(0);
}

## Test ########################################################################
echo "Checking service account configuration" . PHP_EOL;
$credentialsFile = $serviceAccountConfigurations[0];
$configuration = json_decode(file_get_contents($credentialsFile));
assert(!empty($configuration->client_email));
echo "Found service account: $configuration->client_email" . PHP_EOL;
echo "~~ Passed" . PHP_EOL . PHP_EOL;

## Test ########################################################################
echo "Creating a testing SQLite database on disk";
$databaseFile = __DIR__ . '/../local/test.db';
unlink($databaseFile);
$database = new \PDO('sqlite:' . $database, null, null, [
  \PDO::ATTR_PERSISTENT => false,
  \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION
]);
echo "~~ Passed" . PHP_EOL . PHP_EOL;

## Creating tasks ##############################################################
$tasks = new Tasks($credentialsFile, $database);

## Test ########################################################################
echo "Printing some files the service account can access";

echo "~~ Passed" . PHP_EOL . PHP_EOL;

