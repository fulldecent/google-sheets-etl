<?php
namespace fulldecent\GoogleSheetsEtl;

/**
 * Turn-key applications of the Google Sheets ETL operations
 */
class Tasks
{
    function setConfiguration(array $newConfiguration)
    {
        foreach ($newConfiguration as $newSpreadsheetConfiguration) {
            $spreadsheetId = $newSpreadsheetConfiguration->spreadsheetId;
            $sheetTableNames = [];
            foreach ($newSpreadsheetConfiguration->sheets as $newSheetName => $newSheetTableName) {
                $sheetTableNames[$newSheetName] = $newSheetTableName;
            }
            $this->configuration[$spreadsheetId] = $sheetTableNames;
        }
    }
  
    /**
     * Load sheets that have been modified since the most recent modifications in
     * our database.
     */
    public function synchronizeSomeSpreadsheets()
    {
        $this->setupDatabaseMySql();
        $latestModifiedTime = $this->getLatestMotidifedTime();
        echo '- Prior ETL is syncronized up to: ' . $latestModifiedTime . PHP_EOL;
        echo '- Accessing Sheets as: ' . $this->getAccountName() . PHP_EOL;
        echo PHP_EOL . 'LOADING NEW SHEETS' . PHP_EOL;
        $newerFiles = $this->listSomeSpreadsheets($latestModifiedTime) ?? [];
        foreach ($newerFiles as $spreadsheetId => $modifiedTime) {
            echo '  Processing speadsheetId ' . $spreadsheetId . ' modified ' . $modifiedTime . PHP_EOL;
            $this->loadSpreadsheet($spreadsheetId, $modifiedTime);

/*
        if (isset($this->configuration[$spreadsheetId][$sheetName])) {
            $unqualifiedTableName = $this->configuration[$spreadsheetId][$sheetName];
        }
*/

        }
echo "TODO: delete from database sheetsheet ids that are not in the config file\n";
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
    $metaTableName = self::META_TABLE_NAME;
    $sheetsToLoad = $this->getSheetNames($spreadsheetId);

    // Account for all new sheets in case we are interrupted while loading
    foreach ($sheetsToLoad as $sheetName) {
      echo '    Accounting for sheet: ' . $sheetName . PHP_EOL;
      $accountingSql = <<<SQL
INSERT IGNORE INTO {$this->schemaString}`{$this->tablePrefix}$metaTableName` (spreadsheet_id, sheet_name, latest_modified_time, latest_authorized_time)
VALUES (?, ?, ?, ?)
SQL;
      $statement = $this->database->prepare($accountingSql);
      $statement->execute([$spreadsheetId, $sheetName, $modifiedTime, $this->loadTime]);
    }

    // Actually load the sheets
    foreach ($sheetsToLoad as $sheetName) {
      echo '    Processing sheet: ' . $sheetName . PHP_EOL;
      $this->loadSheet($spreadsheetId, $sheetName, $modifiedTime);
    }

echo 'TODO: Check accounting and delete accounting+drop tables for sheets that were not loaded, check with latest_authorized_time' . PHP_EOL;
  }
  
  /**
   * Load one sheet to database
   *
   * @implNote: This could reduce the transaction locking time by using a
   *            temporary table to stage incoming data.
   * @implNote: This will break if the column is named __rowid
   * 
   * @param string $spreadsheetId
   * @param string $sheetName
   * @param string $modifiedTime
   * @return void
   */
  function loadSheet(string $spreadsheetId, string $sheetName, string $modifiedTime)
  {
    $metaTableName = self::META_TABLE_NAME;
    $tableName = $this->getTableName($spreadsheetId, $sheetName);
    $rows = $this->getSheetRowsFromGoogle($spreadsheetId, $sheetName);
    $this->database->beginTransaction();

    // Drop table
    $dropTableSql = "DROP TABLE IF EXISTS {$this->schemaString}`{$this->tablePrefix}$tableName`";
    $this->database->exec($dropTableSql);

    // Create table
    $createTableSql = "CREATE TABLE {$this->schemaString}`{$this->tablePrefix}$tableName` (__rowid INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    $this->database->exec($createTableSql);

echo "TODO: add columns even if only one row\n";
echo "TODO: one big CREATE TABLE\n";
    
    // Populate table
    if (count($rows) >= 2) {
      // Normalize column names, may have duplicates
      $firstRow = array_shift($rows);
      $sheetColumns = array_values(array_unique(array_map([$this, 'getColumnName'], $firstRow)));
      echo '      INFO: Found columns: ' . json_encode($sheetColumns) . PHP_EOL;
      if (count($sheetColumns) >= 1) {
        // Load each columns to database
        foreach (array_unique($sheetColumns) as $column) {
          $sql = "ALTER TABLE {$this->schemaString}`{$this->tablePrefix}$tableName` ADD COLUMN `$column` VARCHAR(100);";
          $this->database->exec($sql);
        }
        $this->insertRowsToTable($tableName, $sheetColumns, $rows);
      }
    }

    // Update accounting
    $accountingSql = <<<SQL
REPLACE INTO {$this->schemaString}`{$this->tablePrefix}$metaTableName` (spreadsheet_id, sheet_name, table_name, latest_modified_time, latest_authorized_time)
 VALUES (?, ?, ?, ?, ?)
SQL;
    $statement = $this->database->prepare($accountingSql);
    $statement->execute([$spreadsheetId, $sheetName, $tableName, $modifiedTime, $this->loadTime]);

    // Done
    $this->database->commit();
  }
  

}