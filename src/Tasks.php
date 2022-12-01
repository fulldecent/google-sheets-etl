<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * Turn-key applications of the Google Sheets ETL operations
 */
class Tasks
{
    public GoogleSheetsAgent $googleSheetsAgent;
    public DatabaseAgent $databaseAgent;

    /**
     * @var array<EtlConfig>
     */
    public array $etlConfig = [];

    public function __construct(string $credentialsFile, \PDO $database)
    {
        $this->googleSheetsAgent =  new GoogleSheetsAgent($credentialsFile);
        $this->databaseAgent = DatabaseAgent::agentForPdo($database);
    }

    public function loadConfiguration(string $file): void
    {
        $this->etlConfig = EtlConfig::fromFile($file);
    }

    /**
     * Find and account for some updated spreadsheets
     */
    public function findSomeUpdatedSpreadsheets()
    {
        $lastModified = '2001-01-01T00:00:00Z'; // A time before Google Drive started
        $highestSpreadsheetIdLoadedAtThatTime = '';
        $result = $this->databaseAgent->getGreatestModified();
        if (!is_null($result)) {
            list($lastModified, $highestSpreadsheetIdLoadedAtThatTime) = $result;
        }
        echo 'Previously found spreadsheets updated up to: ' . $lastModified . PHP_EOL . PHP_EOL;
        $someUpdatedSpreadsheets = $this->googleSheetsAgent->getOldestSpreadsheets(
            $lastModified,
            $highestSpreadsheetIdLoadedAtThatTime,
            200,
        );
        foreach ($someUpdatedSpreadsheets as $googleSpreadsheetId => $spreadsheet) {
            $this->databaseAgent->setSpreadsheetSeen(
                $googleSpreadsheetId,
                $spreadsheet->modifiedTime,
                $spreadsheet->name
            );
            echo "Saw update $googleSpreadsheetId {$spreadsheet->modifiedTime} $spreadsheet->name" . PHP_EOL;
        }
    }

    public function loadSomeUpdatedSpreadsheets()
    {
        $loadableEtlConfigs = $this->databaseAgent->filterExtractable($this->etlConfig);
        foreach ($loadableEtlConfigs as $etlConfig) {
            $this->loadSheet($etlConfig);
        }
    }

    /**
     * Check and re-account for the spreadsheet not seen for the longest time
     * @return bool True if still accessible or no spreadsheets ever seen, false otherwise
     */
    public function verifyOldestSpreadsheet(): bool
    {
        $oldestSeen = $this->databaseAgent->getOldestSeen();
        if (is_null($oldestSeen)) {
            echo 'No spreadsheets ever seen' . PHP_EOL;
            return true;
        }
        try {
            $spreadsheet = $this->googleSheetsAgent->getSpreadsheet($oldestSeen);
        } catch (\Exception $e) {
            // Is this a "File not found" error?
            if (strpos($e->getMessage(), 'File not found') !== false) {
                echo 'Oldest spreadsheet not accessible: ' . $oldestSeen . PHP_EOL;
                return false;
            }
        }
        if (is_null($spreadsheet)) {
            echo "Oldest spreadsheet $oldestSeen is no longer accessible" . PHP_EOL;
            return false;
        }
        $this->databaseAgent->setSpreadsheetSeen(
            $oldestSeen,
            $spreadsheet->modifiedTime,
            $spreadsheet->name
        );
        echo "Oldest spreadsheet $oldestSeen is still accessible" . PHP_EOL;
        return true;
    }

    /**
     * Extract data from Google Sheets and update accounting
     */
    private function loadSheet(EtlConfig $etlConfig): void
    {
        echo '  Extracting ' . $etlConfig->googleSpreadsheetId . ' ' . $etlConfig->sheetName . PHP_EOL;
        $sheetRows = $this->googleSheetsAgent->getSheetRows(
            $etlConfig->googleSpreadsheetId,
            $etlConfig->sheetName,
        );
        echo '    Transforming columns';
        try {
            $selectors = $sheetRows->getColumnSelectorsFromHeaderRow(
                $etlConfig->columnMapping,
                $etlConfig->headerRow,
            );
        } catch (\Exception $exception) {
            throw new \Exception(
                "Load failed for\n" .
                'Spreadsheet https://docs.google.com/spreadsheets/d/' . $etlConfig->googleSpreadsheetId . "\n" .
                'Sheet ' . $etlConfig->sheetName . "\n" .
                'Missing column ' . $exception->getMessage()
            );
        }
        echo '     Loading';
        $columnNames = array_keys($etlConfig->columnMapping);
        $this->databaseAgent->createTable($etlConfig->targetTable, $columnNames);
        try {
            $dataRows = $sheetRows->getRows($selectors, $etlConfig->skipRows);
            $this->databaseAgent->loadSheet(
                $etlConfig->googleSpreadsheetId,
                $etlConfig->sheetName,
                $etlConfig->targetTable,
                $columnNames,
                $dataRows,
                $sheetRows->hash,
            );
        } catch (\Exception $exception) {
            throw new \Exception(
                "Load failed for\n" .
                'Spreadsheet https://docs.google.com/spreadsheets/d/' . $etlConfig->googleSpreadsheetId . "\n" .
                $exception->getMessage()
            );
        }
    }
}
