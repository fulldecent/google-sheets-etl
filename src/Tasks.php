<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * Turn-key applications of the Google Sheets ETL operations
 */
class Tasks
{
    public /* GoogleSheetsAgent */ $googleSheetsAgent;
    public /* DatabaseAgent */ $databaseAgent;

    /**
     * Example:
     * {
     *     "$schema": "./config-schema.json",
     *     "1b39RL2nQJxdhHYxVmkk4lo3K1IKjSD3_ggnokrZCkx8": {
     *         "2019 Expirations": {
     *             "targetTable": "certification-course-renewals-2019",
     *             "columnMapping": {"out1": "in1", "out2": 2},
     *             "headerRow": 0,
     *             "skipRows": 1
     *         }
     *     }
     * }
     */
    private /* array */ $configurationForSpreadsheetSheet;

    public function __construct(string $credentialsFile, \PDO $database)
    {
        $this->googleSheetsAgent =  new GoogleSheetsAgent($credentialsFile);
        $this->databaseAgent = DatabaseAgent::agentForPdo($database);
    }

    public function setConfiguration(\stdClass $configuration)
    {
        foreach ($configuration as $googleSpreadsheetId => $spreadsheetConfiguration) {
            if ($googleSpreadsheetId == '$schema') continue;
            foreach ($spreadsheetConfiguration as $sheetName => $configuration) {
                $this->configurationForSpreadsheetSheet[$googleSpreadsheetId][$sheetName] = (object)[
                    'targetTable' => $configuration->targetTable, # Required property
                    'columnMapping' => (array)($configuration->columnMapping), # Optional
                    'headerRow' => $configuration->headerRow ?? 0, # Optional
                    'skipRows' => $configuration->skipRows ?? 1 # Optional
                ];
            }
        }
    }

    /**
     * Load one sheet to database
     *
     * @implNote: This could reduce the transaction locking time by using a
     *            temporary table to stage incoming data.
     *
     * @param string $googleSpreadsheetId
     * @param string $sheetName
     * @param string $googleModified
     * @return void
     */
    public function loadSheet(string $googleSpreadsheetId, string $sheetName, string $googleModified)
    {
        echo '  Loading speadsheetId ' . $googleSpreadsheetId . ' sheet ' . $sheetName . PHP_EOL;
        $configuration = $this->configurationForSpreadsheetSheet[$googleSpreadsheetId][$sheetName];
        $targetTable = $configuration->targetTable;

        echo '    Getting sheet rows';
        $rowsOfColumns = $this->googleSheetsAgent->getSheetRows($googleSpreadsheetId, $sheetName);
        echo '    Selecting columns';
        try {
            $selectors = $rowsOfColumns->getColumnSelectorsFromHeaderRow($configuration->columnMapping, $configuration->headerRow);
        } catch (\Exception $exception) {
            echo '   ERROR: ' . $exception->getMessage();
            throw new \Exception('With spreadsheet ' . $googleSpreadsheetId . "\n" . $exception->getMessage());
        }
        echo '     Loading';
        $headers = array_keys($configuration->columnMapping);
        $dataRows = $rowsOfColumns->getRows($selectors, $configuration->skipRows);
        $this->databaseAgent->accountSpreadsheetSeen($googleSpreadsheetId, $googleModified);
        $this->databaseAgent->loadAndAccountSheet($googleSpreadsheetId, $sheetName, $targetTable, $googleModified, $headers, $dataRows);
    }

    /**
     * Inhale new sheets from spreadsheet to database, overwriting any existing
     * loaded data
     *
     * Prerequisite: have already run accountSpreadsheetSeen
     *
     * @param string $googleSpreadsheetId Google spreadesheet ID
     * @param string $googleModified RFC 3339 modified time
     * @return void
     */
    public function loadSpreadsheet(string $googleSpreadsheetId, string $googleModified)
    {
        echo 'Loading speadsheetId ' . $googleSpreadsheetId . ' modified ' . $googleModified . PHP_EOL;
        /*
        $sheetsToLoad = $this->googleSheetsAgent->getGridSheetTitles($googleSpreadsheetId);
        foreach ($sheetsToLoad as $sheetName) {
            if (!isset($this->configurationForSpreadsheetSheet[$googleSpreadsheetId][$sheetName])) {
                echo 'Skipping speadsheetId ' . $googleSpreadsheetId . ' sheet ' . $sheetName . PHP_EOL;
                continue;
            }
            $this->loadSheet($googleSpreadsheetId, $sheetName, $googleModified);
        }
        */
        foreach ($this->configurationForSpreadsheetSheet[$googleSpreadsheetId] as $sheetName => $configuration) {
            $etlJob = $this->databaseAgent->getEtl($googleSpreadsheetId, $sheetName);
            if (!is_null($etlJob) && $etlJob->loaded_google_modified === $etlJob->latest_google_modified) {
                continue; // Skip, already loaded this sheet version
            }
            $this->loadSheet($googleSpreadsheetId, $sheetName, $googleModified); 
        }
    }

    /**
     * Load some spreadsheets that were not completely loaded already
     */
    public function loadSomeNewerSpreadsheets()
    {
        $lastModified = '2001-01-01T00:00:00Z'; // Before Google Drive started
        $googleSpreadsheetId = ''; // The lexically lowest spreadsheet ID
        $result = $this->databaseAgent->getGreatestModifiedAndIdLoaded();
        if (!is_null($result)) {
            list($lastModified, $googleSpreadsheetId) = $result;
        }
        echo 'Prior ETL is synchronized up to: ' . $lastModified . PHP_EOL . PHP_EOL;
        $someNewSpreadsheetsIds = $this->googleSheetsAgent->getOldestSpreadsheets($lastModified, $googleSpreadsheetId);
        foreach ($someNewSpreadsheetsIds as $googleSpreadsheetId => $googleModified) {
            $this->databaseAgent->accountSpreadsheetSeen($googleSpreadsheetId, $googleModified);
            if (!isset($this->configurationForSpreadsheetSheet[$googleSpreadsheetId])) {
                echo 'Skipping speadsheetId ' . $googleSpreadsheetId . ' modified ' . $googleModified . PHP_EOL;
                continue;
            }
            $this->loadSpreadsheet($googleSpreadsheetId, $googleModified);
        }
    }
}