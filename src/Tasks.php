<?php
namespace fulldecent\GoogleSheetsEtl;

/**
 * Turn-key applications of the Google Sheets ETL operations
 */
class Tasks
{
    public /* GoogleSheetsAgent */ $googleSheetsAgent;
    public /* DatabaseAgent */ $databaseAgent;
    private /* array */ $mappingSpreadsheetsSheetsToTableName;    

    function __construct(string $credentialsFile, \PDO $database)
    {
        $this->googleSheetsAgent =  new GoogleSheetsAgent($credentialsFile);
        $this->databaseAgent = DatabaseAgent::AgentForPDO($database);
    }

    function setConfiguration(string $configurationFileName)
    {
        $configuration = json_decode(file_get_contents($configurationFileName));
        foreach ($configuration as $spreadsheetId => $spreadsheetConfiguration) {
            if ($spreadsheetId == '$schema') continue;
            foreach ($spreadsheetConfiguration as $sheetName => $tableName) {
                $this->mappingSpreadsheetsSheetsToTableName[$spreadsheetId][$sheetName] = $tableName;
            }
        }
    }

    function tableNameForSheet($spreadsheetId, $sheetName): string
    {
        if (isset($this->mappingSpreadsheetsSheetsToTableName[$spreadsheetId][$sheetName])) {
            return $this->mappingSpreadsheetsSheetsToTableName[$spreadsheetId][$sheetName];
        }
        return $spreadsheetId . '-' . $sheetName;
        /*
        return $this->mappingSpreadsheetsSheetsToTableName[$spreadsheetId][$sheetName]
            ?? $spreadsheetId . '-' . $sheetName;
        */
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
        $tableName = $this->tableNameForSheet($spreadsheetId, $sheetName);

        $rows = $this->googleSheetsAgent->getSheetRows($spreadsheetId, $sheetName);
        assert(count($rows) >= 1);
        assert(count($rows[0]) >= 1);
        $headerRow = $rows[0];
        $dataRows = $this->normalizedArraysOfLength(array_slice($rows, 1), count($headerRow));

        $this->databaseAgent->accountSpreadsheetAuthorized($spreadsheetId, $modifiedTime);
        $this->databaseAgent->loadAndAccountSheet($spreadsheetId, $sheetName, $tableName, $modifiedTime, $headerRow, $dataRows);
    }

    /**
     * Inhale spreadsheet to database, overwriting any existing sheets
     *
     * @param string $spreadsheetId Google spreadesheet ID
     * @param string $modifiedTime RFC 3339 modified time
     * @return void
     */
    function loadSpreadsheet(string $spreadsheetId, string $modifiedTime)
    {
        $this->databaseAgent->accountSpreadsheetAuthorized($spreadsheetId, $modifiedTime);
        $sheetsToLoad = $this->googleSheetsAgent->getGridSheetTitles($spreadsheetId);
        foreach ($sheetsToLoad as $sheetName) {
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
        echo '- Prior ETL is synchronized up to: ' . $lastModified . PHP_EOL;
        $someNewSpreadsheetsIds = $this->googleSheetsAgent->getOldestSpreadsheets($lastModified, $spreadsheetId);
        foreach ($someNewSpreadsheetsIds as $spreadsheetId => $modifiedTime) {
            echo '  Loading speadsheetId ' . $spreadsheetId . ' modified ' . $modifiedTime . PHP_EOL;
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

    // PRIVATE /////////////////////////////////////////////////////////////////

    /**
     * Truncate/pad all arrays in row to a given length
     *
     * @param array $rows
     * @param integer $length
     * @return array array containing arrays each with size LENGTH
     */
    private function normalizedArraysOfLength(array $rows, int $length): array
    {
        $retval = [];
        foreach ($rows as $row) {
            $row = array_slice($row, 0, $length);
            $row = array_pad($row, $length, null);
            $retval[] = $row;
        }
        return $retval;
    }
}