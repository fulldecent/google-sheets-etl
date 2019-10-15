<?php
namespace fulldecent\GoogleSheetsEtl;

/**
 * A data store and accounting for putting CSV files into a PDO database
 */
class DatabaseAgentSqlite extends DatabaseAgent
{
// TODO CODE BELOW HERE IS OLD


    private /* \PDO */ $database;
    private /* ?string */ $schema;
    private /* ?string */ $tablePrefix;
    private /* ?string */ $loadTime;
    const META_TABLE_NAME = '__meta_table_index';
    public $sqlInsertChunkSize = 500; // Beware max_allowed_packet errors
    
    function __construct(\PDO $newDatabase)
    {
        $this->database = $newDatabase;
        $this->loadTime = date('Y-m-d H:i:s');
    }
    
    function setupDatabaseMySql()
    {
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
-- Make the index, must be InnoDB because need transactions
CREATE TABLE IF NOT EXISTS $quotedFullyQualifiedMetaTableName (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'InnoDB requires an incrementing column for performance',
  `spreadsheet_id` varchar(44) NOT NULL COMMENT 'files.id per Google Drive API',
  `sheet_name` varchar(255) NOT NULL COMMENT 'spreadsheet.sheets.properties.title per Google Sheets API',
  `table_name` varchar(255) COMMENT 'The database table here',
  `latest_modified_time` varchar(50) NOT NULL COMMENT 'files.modifiedTime per Google Drive API',
  `latest_loaded_time` varchar(50) COMMENT 'files.modifiedTime per Google Drive API',
  `latest_authorized_time` varchar(50) NOT NULL COMMENT 'Local system date when visibility of file last confirmed',
  PRIMARY KEY (`id`),
UNIQUE KEY `spreadsheet_id` (`spreadsheet_id`,`sheet_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
SQL;
echo $sql;
        $this->database->exec($sql);
    }
        
    function setupDatabaseSqlite()
    {
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS $quotedFullyQualifiedMetaTableName (
    spreadsheet_id TEXT,
    sheet_name TEXT,
    table_name TEXT,
    last_modified TEXT, -- Use rfc3339
    last_loaded TEXT,
    last_authorization_checked TEXT,
    CONSTRAINT spreadsheet_id UNIQUE (spreadsheet_id,sheet_name)
)
SQL;
        $this->database->exec($sql);
    }
        
    /**
     * The latest modified time in the database
     *
     * @see https://tools.ietf.org/html/rfc3339
     * 
     * @return ?string The time, or a default value before Google Drive existed
     */
    function getLatestMotidifedTime(): ?string {
echo 'TODO: refactor this so that it get the last spreadsheet that was fully loaded. As current the way this is used it can skip other sheets on reload if one sheet causes a crash' . PHP_EOL;
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
SELECT MAX(last_modified)
  FROM $quotedFullyQualifiedMetaTableName
 WHERE last_modified = last_loaded
SQL;
        return $this->database->query($sql)->fetchColumn();
    }

    /**
     * @param string $date YYYY-MM-DD format
     */
    function getGreatestIdWithAuthorizationCheckedSince(string $date)
    {
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $sql = <<<SQL
SELECT MAX(spreadsheet_id)
  FROM $quotedFullyQualifiedMetaTableName
 WHERE last_authorization_checked >= ?
SQL;
        $statement = $this->database->prepare($sql);
        $statement->execute([$date]);
        return $statement->fetchColumn();
    }

    function createTableSqlite(string $quotedFullyQualifiedTableName, array $columns)
    {
        // Drop table
        $dropTableSql = "DROP TABLE IF EXISTS $quotedFullyQualifiedTableName";
        $this->database->exec($dropTableSql);

        $columns = $this->normalizedColumnNames($columns);
        $quotedColumns = '`' . implode('`,`', $columns) . '`';
        
        // Create table
        $createTableSql = "CREATE TABLE $quotedFullyQualifiedTableName ($quotedColumns)";
        $this->database->exec($createTableSql);
    }

    // Cannot use CSV import with PDO
    function insertRowsSqlite(string $quotedFullyQualifiedTableName, $rows)
    {
        if (!count($rows)) {
            return;
        }

        $sqlPrefix = "INSERT INTO $quotedFullyQualifiedTableName VALUES";
        $sqlOneValueList = implode(',', array_fill(0, count($rows[0]), '?'));
    
        // Load each row for the usable columns
        foreach(array_chunk($rows, $this->sqlInsertChunkSize, true) as $rowChunk) {
            $parameters = array_merge(...$rowChunk);
            $sqlValueLists = '(' . implode('),(', array_fill(0, count($rowChunk), $sqlOneValueList)) . ')';
            $statement = $this->database->prepare($sqlPrefix . $sqlValueLists);
            $statement->execute($parameters);
            echo "        loaded " . ($this->array_key_last($rowChunk) + 1) . " rows" . PHP_EOL;
        }
    }

    /**
     * Load one sheet to database
     *
     * @implNote: This will break if the column is named __rowid
     * 
     * @param string $spreadsheetId
     * @param string $sheetName
     * @param string $modifiedTime
     * @return void
     */
    function loadSheet(string $spreadsheetId, string $sheetName, string $modifiedTime, array $columns, array $rows)
    {
        $quotedFullyQualifiedMetaTableName = $this->getQuotedFullyQualifiedMetaTableName();
        $unqualifiedTableName = $this->getUnqualifiedTableName($spreadsheetId, $sheetName);
        $fullyQualifiedTableName = $this->getQuotedFullyQualifiedTableName($spreadsheetId, $sheetName);
        $this->database->beginTransaction();

        $this->createTableSqlite($fullyQualifiedTableName, $columns);
        $this->insertRowsSqlite($fullyQualifiedTableName, $rows);
        
        // Update accounting
        $accountingSql = <<<SQL
REPLACE INTO $quotedFullyQualifiedMetaTableName (
    spreadsheet_id, sheet_name, table_name, last_modified, last_loaded, last_authorization_checked
)
VALUES (?, ?, ?, ?, ?, ?)
SQL;
        $statement = $this->database->prepare($accountingSql);
        $statement->execute([$spreadsheetId, $sheetName, $unqualifiedTableName, $modifiedTime, $this->loadTime, $this->loadTime]);
        
        // Done
        $this->database->commit();
    }

    /**
     * Turns the columns into unique names of the format which MySQL and SQLite
     * allow as ASCII quoted identifiers
     * 
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     * identifiers.
     *
     * @param array $columns
     * @return array The new column names
     */
    private function normalizedColumnNames(array $columns): array
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
            $retval[] = $column;
        }
        return $retval;
    }
        
    /**
     * Undocumented function
     *
     * @param string $tableName
     * @param array $columnNames Must be unique
     * @param array $rows The values must be indexed same as columnNames
     */
    private function insertRowsToTable(string $fullyQualifiedTableName, array $normalizedColumnNames, array $normalizedRows)
    {
        $tempFile = tmpfile();
        foreach ($normalizedRows as $row) {
            fputcsv($tempFile, $row);
        }
        // put in columns...    
        $path = stream_get_meta_data($tempFile)['uri']; // eg: /tmp/phpFx0513a
        $sql = <<<SQL
LOAD DATA INFILE '$path'
INTO TABLE $fullyQualifiedTableName
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '' 
LINES TERMINATED BY '\n'
SQL;
        $this->database->query($sql);
        fclose($tempFile); // this removes the file
    }
        
    private function getUnqualifiedTableName(string $spreadsheetId, string $sheetName): string
    {
        assert(preg_match('|^[0-9a-z_/-]{44}$|i', $spreadsheetId));
        $unqualifiedTableName = "$spreadsheetId-$sheetName";
        if (isset($this->configuration[$spreadsheetId][$sheetName])) {
            $unqualifiedTableName = $this->configuration[$spreadsheetId][$sheetName];
        }
        $unqualifiedTableName = substr($unqualifiedTableName, 0, 64-strlen($this->tablePrefix ?? ''));
        $unqualifiedTableName = trim($unqualifiedTableName);
        return $unqualifiedTableName;
    }

    /**
     * Use configured table name or come up with a safe name for the database
     * table.
     *
     * @see https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
     *
     * @param string $spreadsheetId
     * @param string $sheetName
     * @return string
     */
    private function getQuotedFullyQualifiedTableName(string $spreadsheetId, string $sheetName): string
    {
        $unqualifiedTableName = $this->getUnqualifiedTableName($spreadsheetId, $sheetName);
        $qualifiedTableName = ($this->tablePrefix ?? '') . $unqualifiedTableName;
        return ($this->schema ?? '') . "`$qualifiedTableName`";
    }

    private function getQuotedFullyQualifiedMetaTableName(): string
    {
        return ($this->schema ?? '') . '`' . ($this->tablePrefix ?? '') . self::META_TABLE_NAME . '`';
    }
}