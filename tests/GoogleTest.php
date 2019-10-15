<?php
namespace fulldecent\GoogleSheetsEtl;

/*
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
echo "Initializing agent" . PHP_EOL;
$googleSheetsAgent = new GoogleSheetsAgent($credentialsFile);
echo "~~ Passed" . PHP_EOL . PHP_EOL;

## Test ########################################################################
echo "Printing some files the service account can access" . PHP_EOL;
$someRecentFiles = $googleSheetsAgent->listSomeSpreadsheets('2001-01-01T12:00:00', 5);
if (!count($someRecentFiles)) {
  echo "The service account cannot access any file." . PHP_EOL;
  echo "Trying opening a folder in Google Drive or a Google Sheet and share with: " . $configuration->client_email . PHP_EOL;
  echo "~~ FAILED" . PHP_EOL . PHP_EOL;
}
foreach ($someRecentFiles as $spreadsheetId => $modificationTime) {
    echo ' - Can access: https://docs.google.com/spreadsheets/d/' . $spreadsheetId . PHP_EOL;
}
echo "~~ Passed" . PHP_EOL . PHP_EOL;

*/