<?php

declare(strict_types=1);

namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for putting CSV files into a PDO database
 */
class DatabaseAgentMysql extends DatabaseAgent
{
    /**
     * Beware max_allowed_packet errors
     * @var int
     */
    public $sqlInsertChunkSize = 500; // Beware max_allowed_packet errors

    public const SPREADSHEETS_TABLE = '__meta_spreadsheets';
    public const SHEETS_TABLE = '__meta_sheets';

    // Accounting //////////////////////////////////////////////////////////////

    /**
     * The accounting must be set up before any other methods are called
     *
     * @apiSpec Calling this method twice shall not cause any data loss or any
     *          error.
     */
    public function setUpAccounting()
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedSheetsTable = $this->quotedFullyQualifiedTableName(self::SHEETS_TABLE);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedSpreadsheetsTable (
	`_rowid_` INT NOT NULL AUTO_INCREMENT,
	`spreadsheet_id` VARCHAR(44) NOT NULL,
	`last_modified` VARCHAR(99) NOT NULL,
	`last_authorization_checked` VARCHAR(20) NOT NULL,
	`last_loaded` VARCHAR(99),
	UNIQUE KEY `row_id` (`_rowid_`),
	UNIQUE KEY `spreadsheet_id` (`spreadsheet_id`)
) ENGINE=InnoDB;        
SQL;
        $this->database->exec($sql);

        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedSheetsTable (
	`_rowid_` INT NOT NULL AUTO_INCREMENT,
	`spreadsheet_rowid` INT NOT NULL,
	`sheet_name` VARCHAR(99) NOT NULL,
	`last_loaded` VARCHAR(99),
	`table_name` VARCHAR(99),
	UNIQUE KEY `row_id` (`_rowid_`),
	UNIQUE KEY `sheet_name` (`spreadsheet_rowid`, `sheet_name`)
) ENGINE InnoDB;
SQL;
        $this->database->exec($sql);
    }

    /**
     * Account that a spreadsheet is authorized
     */
    public function accountSpreadsheetAuthorized(string $spreadsheetId, string $lastModified)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
INSERT IGNORE INTO $quotedSpreadsheetsTable
(spreadsheet_id, last_modified, last_authorization_checked, last_loaded)
VALUES
(:spreadsheet_id, :last_modified, :last_authorization_checked, null)
SQL;
        $this->database->prepare($sql)->execute(['spreadsheet_id'=>$spreadsheetId, 'last_modified'=>$lastModified, 'last_authorization_checked'=>$this->loadTime]);
        $sql = <<<SQL
UPDATE $quotedSpreadsheetsTable
   SET last_modified = :last_modified
     , last_authorization_checked = :last_authorization_checked
 WHERE spreadsheet_id = :spreadsheet_id
SQL;
        $this->database->prepare($sql)->execute(['spreadsheet_id'=>$spreadsheetId, 'last_modified'=>$lastModified, 'last_authorization_checked'=>$this->loadTime]);
    }
    
    /**
     * Account that a spreadsheet is fully loaded (after all sheets loaded)
     */
    public function accountSpreadsheetLoaded(string $spreadsheetId, string $lastModified)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<SQL
INSERT IGNORE INTO $quotedSpreadsheetsTable
(spreadsheet_id, last_modified, last_authorization_checked, last_loaded)
VALUES
(:spreadsheet_id, :last_modified, :last_authorization_checked, :last_modified)
SQL;
        $this->database->prepare($sql)->execute(['spreadsheet_id'=>$spreadsheetId, 'last_modified'=>$lastModified, 'last_authorization_checked'=>$this->loadTime]);
        $sql = <<<SQL
UPDATE $quotedSpreadsheetsTable
   SET last_modified = :last_modified
     , last_authorization_checked = :last_authorization_checked
     , last_loaded = :last_modified
 WHERE spreadsheet_id = :spreadsheet_id
SQL;
        $this->database->prepare($sql)->execute(['spreadsheet_id'=>$spreadsheetId, 'last_modified'=>$lastModified, 'last_authorization_checked'=>$this->loadTime]);
    }

    // Getters /////////////////////////////////////////////////////////////////

    /**
     * Get table name for a sheet
     *
     * @param $spreadsheetId string the spreadsheet ID to query
     * @param $sheetName string the sheet name to query
     * @return the table name for the sheet, or null if sheet is not loaded
     */
    public function getTableNameForSheet(string $spreadsheetId, string $sheetName): ?string
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedSheetsTable = $this->quotedFullyQualifiedTableName(self::SHEETS_TABLE);

        $sql = <<<SQL
SELECT table_name
  FROM $quotedSheetsTable sheets
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets._rowid_ = sheets.spreadsheet_rowid
 WHERE sheets.sheet_name = :sheet_name
   AND spreadsheets.spreadsheet_id = :spreadsheet_id
SQL;
        $query = $this->database->prepare($sql);
        $query->execute(['spreadsheet_id'=>$spreadsheetId, 'sheet_name'=>$sheetName]);
        $tableName = $query->fetchColumn();
        return $tableName === false ? null : $tableName;
    }

    /**
     * For all spreadsheets which are fully loaded, get the one with the
     * greatest lexical order of (modified_time, spreadsheet_id), or null if no
     * spreadsheets are loaded
     *
     * @see https://tools.ietf.org/html/rfc3339
     *
     * @return array like ['2015-01-01 03:04:05', '349u948k945kd43-k35529_298k938']
     */
    public function getGreatestModifiedAndIdLoaded(): ?array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        var_dump($quotedSpreadsheetsTable);
        $sql = <<<A
SELECT last_modified, spreadsheet_id
  FROM $quotedSpreadsheetsTable
 WHERE last_loaded = last_modified
 ORDER BY last_modified DESC, spreadsheet_id DESC
 LIMIT 1      
A;
        $row = $this->database->query($sql)->fetch(\PDO::FETCH_NUM);
        return $row === false ? null : $row;
    }

    /**
     * For all spreadsheets with authorization confirmed on or after the given
     * date, get the greatest spreadsheet ID
     *
     * @param string int a Unix timestamp
     * @return ?string spreadsheet ID or null
     */
    public function getGreatestIdWithAuthorizationCheckedSince(int $since): ?string
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<AAAA
SELECT spreadsheet_id
  FROM $quotedSpreadsheetsTable
 WHERE last_authorization_checked >= ?
 ORDER BY spreadsheet_id DESC
 LIMIT 1
AAAA;
        $query = $this->database->prepare($sql);
        $query->execute([$since]);
        $result = $query->fetchColumn();
        return $result === false ? null : $result;
    }

    /**
     * Get all spreadsheets which did not have authorization confirmed on or
     * after the given time
     *
     * @param string int a Unix timestamp
     * @param int limit a maximum quantity of results to return
     * @return array spreadsheet IDs in order starting with the lowest and
     *               including up to LIMIT number of rows
     */
    public function getIdsWithAuthorizationNotCheckedSince(string $since, int $limit): array
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $sql = <<<AAAA
SELECT spreadsheet_id
  FROM $quotedSpreadsheetsTable
 WHERE last_authorization_checked < ?
 ORDER BY spreadsheet_id
 LIMIT $limit
AAAA;
        $query = $this->database->prepare($sql);
        $query->execute([$since]);
        return $query->fetchColumn();
    }

    // Data store //////////////////////////////////////////////////////////////

    /**
     * Removes sheet and accounting, if exists, and load and account for sheet
     *
     * @implNote: This could reduce the transaction locking time by using a
     *            temporary table to stage incoming data.
     * @apiSpec This operation shall be atomic, no partial effect may occur on
     *          the database if program is prematurely exited.
     */
    public function loadAndAccountSheet(string $spreadsheetId, string $sheetName, string $tableName, string $modifiedTime, array $columns, array $rows)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedSheetsTable = $this->quotedFullyQualifiedTableName(self::SHEETS_TABLE);
        $this->database->beginTransaction();

        // Create table
//        echo '    Dropping table ' . $tableName;
//        $quotedTableName = $this->quotedFullyQualifiedTableName($tableName);
//        $dropTableSql = "DROP TABLE IF EXISTS $quotedTableName";
//        $this->database->exec($dropTableSql);

        echo '    Creating table';
        $quotedTableName = $this->quotedFullyQualifiedTableName($tableName);
        $quotedColumnArray = $this->normalizedQuotedColumnNames($columns);
        $createTableSql = "CREATE TABLE IF NOT EXISTS $quotedTableName (`_row` int(11) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY)";
        $this->database->exec($createTableSql);

        echo '    Adding columns';
        foreach ($this->normalizedQuotedColumnNames($columns) as $normalizedQuotedColumnName) {
            $addColumnSql = "ALTER TABLE $quotedTableName ADD COLUMN ($normalizedQuotedColumnName VARCHAR(100))";
            try {
                $this->database->exec($addColumnSql);
            } catch (\PDOException $e) {
                // Ignore if column already exists
            }
        }

        echo '    Deleting rows';
        $quotedTableName = $this->quotedFullyQualifiedTableName($tableName);
        $deleteSql = "DELETE FROM $quotedTableName";
        $this->database->exec($deleteSql);

        echo '    Inserting rows';
        $quotedColumns = implode(',', $quotedColumnArray);
        if (count($rows) > 0) {
            $sqlPrefix = "INSERT INTO $quotedTableName ($quotedColumns) VALUES";
            $sqlOneValueList = implode(',', array_fill(0, count($columns), '?'));
            // Load each row for the usable columns
            foreach (array_chunk($rows, $this->sqlInsertChunkSize, true) as $rowChunk) {
                $parameters = array_merge(...$rowChunk);
                $sqlValueLists = '(' . implode('),(', array_fill(0, count($rowChunk), $sqlOneValueList)) . ')';
                $statement = $this->database->prepare($sqlPrefix . $sqlValueLists);
                $parameters = array_map(function($v){
                    return empty($v) ? null : substr($v, 0, 100);
                }, $parameters);
                $statement->execute($parameters);
                echo '        loaded ' . ($this->arrayKeyLast($rowChunk) + 1) . ' rows' . PHP_EOL;
            }
        }

        // Update accounting
        $accountingSql = <<<AAAA
REPLACE INTO $quotedSheetsTable (
    spreadsheet_rowid,
    sheet_name,
    last_loaded,
    table_name
)
VALUES ((SELECT _rowid_ FROM $quotedSpreadsheetsTable WHERE spreadsheet_id = ?), ?, ?, ?)
AAAA;
        $statement = $this->database->prepare($accountingSql);
        $statement->execute([$spreadsheetId, $sheetName, $modifiedTime, $tableName]);

        // All done
        $this->database->commit();
    }

    /**
     * Removes sheets and accounting for any sheets that do not have the latest
     * known data loaded
     */
    public function removeOutdatedSheets(string $spreadsheetId)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedSheetsTable = $this->quotedFullyQualifiedTableName(self::SHEETS_TABLE);
        $this->database->beginTransaction();

        // Find old sheets
        $findSheetsSql = <<<AAAA
SELECT table_name
  FROM $quotedSheetsTable sheets
  JOIN $quotedSpreadsheetsTable spreadsheets
    ON spreadsheets._rowid_ = sheets.spreadsheet_rowid
 WHERE spreadsheet_id = ?
   AND sheets.last_loaded != spreadsheets.last_modified
AAAA;
        $query = $this->database->prepare($findSheetsSql);
        $query->execute([$spreadsheetId]);

        // Delete sheet tables
        foreach ($query->fetchAll(\PDO::FETCH_COLUMN) as $unqualifiedTableName) {
            $tableName = $this->quotedFullyQualifiedTableName($unqualifiedTableName);
            $dropTableSql = <<<SQL
DROP TABLE $tableName
SQL;
            $this->database->query($dropTableSql);
        }

        // Delete old sheets accounting
        $deleteSheets = <<<AAAA
DELETE 
  FROM $quotedSheetsTable
 WHERE _rowid_ IN (
       SELECT sheets._rowid_
         FROM (SELECT * FROM $quotedSheetsTable) sheets
         JOIN $quotedSpreadsheetsTable spreadsheets
           ON spreadsheets._rowid_ = sheets.spreadsheet_rowid
        WHERE spreadsheet_id = ?
          AND sheets.last_loaded != spreadsheets.last_modified
       )
AAAA;
        $query = $this->database->prepare($deleteSheets);
        $query->execute([$spreadsheetId]);

        // All done
        $this->database->commit();
    }

    /**
     * Removes sheets and accounting for given spreadsheet
     */
    public function removeSpreadsheet(string $spreadsheetId)
    {
        $quotedSpreadsheetsTable = $this->quotedFullyQualifiedTableName(self::SPREADSHEETS_TABLE);
        $quotedSheetsTable = $this->quotedFullyQualifiedTableName(self::SHEETS_TABLE);
        $this->database->beginTransaction();

        // Find sheets
        $findSheetsSql = <<<AAAA
SELECT table_name
  FROM $quotedSheetsTable
 WHERE spreadsheet_rowid IN (
       SELECT _rowid_
         FROM $quotedSpreadsheetsTable
        WHERE spreadsheet_id = ?
       ) 
AAAA;
        $query = $this->database->prepare($findSheetsSql);
        $query->execute([$spreadsheetId]);

        // Delete sheet tables
        foreach ($query->fetchAll(\PDO::FETCH_COLUMN) as $unqualifiedTableName) {
            $tableName = $this->quotedFullyQualifiedTableName($unqualifiedTableName);
            $dropTableSql = <<<SQL
DROP TABLE $tableName
SQL;
            $this->database->query($dropTableSql);
        }

        // Delete sheets accounting
        $deleteSheets = <<<AAAA
DELETE 
  FROM $quotedSheetsTable
 WHERE spreadsheet_rowid IN (
       SELECT _rowid_
         FROM (SELECT * FROM $quotedSpreadsheetsTable)
        WHERE spreadsheet_id = ?
       )
AAAA;
        $query = $this->database->prepare($deleteSheets);
        $this->database->execute([$spreadsheetId]);

        // Delete spreadsheet accounting
        $deleteSpreadsheet = <<<AAAA
DELETE 
  FROM $quotedSpreadsheetsTable
 WHERE spreadsheet_id = ?
AAAA;
        $query = $this->database->prepare($deleteSpreadsheet);
        $this->database->execute([$spreadsheetId]);

        // All done
        $this->database->commit();
    }

    // PRIVATE /////////////////////////////////////////////////////////////////

    /**
     * Use SCHEMA and PREFIX to generate table name.
     *
     * @implNote WARNING, this can make names that are too long for
     *           the database.
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param string $unqualifiedName
     * @return string
     */
    private function quotedFullyQualifiedTableName(string $unqualifiedName): string
    {
        $qualifiedTableName = ($this->tablePrefix ?? '') . $unqualifiedName;
        return isset($this->schema)
            ? $this->schema . ".`$qualifiedTableName`"
            : "`$qualifiedTableName`";
    }

    /**
     * Turns the columns into unique names of the format which MySQL and SQLite
     * allow as ASCII quoted identifiers
     *
     * @implNote: This will break if a column is named _rowid_
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     * identifiers.
     *
     * @param array $columns
     * @return array The new column names
     */
    private function normalizedQuotedColumnNames(array $columns): array
    {
        $retval = [];
        foreach ($columns as $index => $column) {
            $column = iconv('UTF-8', 'ASCII//TRANSLIT', $column);
            $column = strtolower($column);
            $column = preg_replace('/[^a-z0-9_ ]/', '', $column);
            $column = trim($column);
            if (!preg_match('/^[a-z_]/', $column)) {
                $column = '_' . $column;
            }
            if (preg_match('/^col_[0-9]+$/', $column) || empty($column) || in_array($column, $retval)) {
                $column = 'col_' . ($index + 1);
            }
            array_push($retval, '`' . $column . '`');
        }
        return $retval;
    }

    private function arrayKeyLast(array $array)
    {
        end($array);
        return key($array);
    }
}
