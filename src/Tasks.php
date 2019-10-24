<?php
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
     *             "tableName": "certification-course-renewals-2019",
     *             "columnMapping": {"out1": "in1", "out2": 2},
     *             "headerRow": 0,
     *             "skipRows": 1
     *         }
     *     }
     * }
     */
    private /* array */ $configurationForSpreadsheetSheet;

    function __construct(string $credentialsFile, \PDO $database)
    {
        $this->googleSheetsAgent =  new GoogleSheetsAgent($credentialsFile);
        $this->databaseAgent = DatabaseAgent::AgentForPDO($database);
    }

    function setConfiguration(string $configurationFileName)
    {
        $allConfiguration = json_decode(file_get_contents($configurationFileName));
        foreach ($allConfiguration as $spreadsheetId => $spreadsheetConfiguration) {
            if ($spreadsheetId == '$schema') continue;
            foreach ($spreadsheetConfiguration as $sheetName => $configuration) {
                $this->configurationForSpreadsheetSheet[$spreadsheetId][$sheetName] = (object)[
                    'tableName' => $configuration->tableName, # Required property
                    'columnMapping' => (array)($configuration->columnMapping ?? []), # Optional
                    'headerRow' => $configuration->headerRow ?? null, # Optional
                    'skipRows' => $configuration->skipRows ?? null # Optional
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
     * @param string $spreadsheetId
     * @param string $sheetName
     * @param string $modifiedTime
     * @return void
     */
    function loadSheet(string $spreadsheetId, string $sheetName, string $modifiedTime)
    {
        echo '  Loading speadsheetId ' . $spreadsheetId . ' sheet ' . $sheetName . PHP_EOL;
        $configuration = $this->configurationForSpreadsheetSheet[$spreadsheetId][$sheetName];
        $tableName = $configuration->tableName;

        $rowsOfColumns = $this->googleSheetsAgent->getSheetRows($spreadsheetId, $sheetName);
        $selectors = $rowsOfColumns->getColumnSelectorsFromHeaderRow($configuration->columnMapping, $configuration->headerRow);
        $this->databaseAgent->accountSpreadsheetAuthorized($spreadsheetId, $modifiedTime);
        $headers = array_keys($configuration->columnMapping);
        $dataRows = $rowsOfColumns->getRows($selectors, $configuration->skipRows);
        $this->databaseAgent->loadAndAccountSheet($spreadsheetId, $sheetName, $tableName, $modifiedTime, $headers, $dataRows);
    }

    /**
     * Inhale spreadsheet to database, overwriting any existing sheets
     * 
     * Prerequesite: have already run accountSpreadsheetAuthorized
     *
     * @param string $spreadsheetId Google spreadesheet ID
     * @param string $modifiedTime RFC 3339 modified time
     * @return void
     */
    function loadSpreadsheet(string $spreadsheetId, string $modifiedTime)
    {
        echo 'Loading speadsheetId ' . $spreadsheetId . ' modified ' . $modifiedTime . PHP_EOL;
        /*
        $sheetsToLoad = $this->googleSheetsAgent->getGridSheetTitles($spreadsheetId);
        foreach ($sheetsToLoad as $sheetName) {
            if (!isset($this->configurationForSpreadsheetSheet[$spreadsheetId][$sheetName])) {
                echo 'Skipping speadsheetId ' . $spreadsheetId . ' sheet ' . $sheetName . PHP_EOL;
                continue;
            }
            $this->loadSheet($spreadsheetId, $sheetName, $modifiedTime);
        }
        */
        foreach ($this->configurationForSpreadsheetSheet[$spreadsheetId] as $sheetName => $configuration) {
            $this->loadSheet($spreadsheetId, $sheetName, $modifiedTime); 
        }
        $this->databaseAgent->removeOutdatedSheets($spreadsheetId);
        $this->databaseAgent->accountSpreadsheetLoaded($spreadsheetId, $modifiedTime);
    }

    /**
     * Load some spreadsheets that were modified right after the latest modified
     * spreadsheets already in the database.
     */
    public function loadSomeNewerSpreadsheets()
    {
        $lastModified = '2001-01-01T00:00:00Z'; // Before Google Drive started
        $spreadsheetId = ''; // The lexically lowest spreadsheet ID
        $result = $this->databaseAgent->getGreatestModifiedAndIdLoaded();
        if (!is_null($result)) {
            list($lastModified, $spreadsheetId) = $result;
        }
        echo 'Prior ETL is synchronized up to: ' . $lastModified . PHP_EOL . PHP_EOL;
        $someNewSpreadsheetsIds = $this->googleSheetsAgent->getOldestSpreadsheets($lastModified, $spreadsheetId);
        foreach ($someNewSpreadsheetsIds as $spreadsheetId => $modifiedTime) {
            $this->databaseAgent->accountSpreadsheetAuthorized($spreadsheetId, $modifiedTime);
            if (!isset($this->configurationForSpreadsheetSheet[$spreadsheetId])) {
                echo 'Skipping speadsheetId ' . $spreadsheetId . ' modified ' . $modifiedTime . PHP_EOL;
                continue;
            }
            $this->loadSpreadsheet($spreadsheetId, $modifiedTime);
        }
    }

    /**
     * Reviews 
     */
    public function deleteSomeGoneSpreadsheets(string $since=null)
    {
        assert(0);
        //TODO: implement this
    }
}